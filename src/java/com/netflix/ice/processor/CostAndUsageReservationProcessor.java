/*
 *
 *  Copyright 2013 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.ice.processor;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.PurchaseOption;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.common.TagGroupRI;
import com.netflix.ice.processor.pricelist.InstancePrices;
import com.netflix.ice.processor.pricelist.PriceListService;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Operation.ReservationOperation;
import com.netflix.ice.tag.ReservationArn;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.Tag;

public class CostAndUsageReservationProcessor extends ReservationProcessor {
    // Unused rates and amortization for RIs were added to CUR on 2018-01
	final static long jan1_2018 = new DateTime("2018-01", DateTimeZone.UTC).getMillis();
	
	public CostAndUsageReservationProcessor(
			Set<Account> reservationOwners, ProductService productService,
			PriceListService priceListService) throws IOException {
		super(reservationOwners, productService,
				priceListService);
	}
	
	private void add(Map<TagGroup, Double> map, TagGroup tg, double value) {
		Double amount = map.get(tg);
		if (amount == null)
			amount = 0.0;
		amount += value;
		map.put(tg, amount);
	}
	
	@Override
	protected void processReservations(
			ReservationService reservationService,
			CostAndUsageData data,
			Long startMilli) {
		
//		DateTime start = DateTime.now();
		
		ReadWriteData usageData = data.getUsage(product);
		ReadWriteData costData = data.getCost(product);

		// Scan the first hour and look for reservation usage with no ARN and log errors
	    for (TagGroup tagGroup: usageData.getData(0).keySet()) {
	    	if (tagGroup.operation instanceof ReservationOperation) {
	    		ReservationOperation ro = (ReservationOperation) tagGroup.operation;
	    		if (ro.getPurchaseOption() != null) {
	    			if (!(tagGroup instanceof TagGroupRI))
	    				logger.error("   --- Reserved Instance usage without reservation ID: " + tagGroup + ", " + usageData.getData(0).get(tagGroup));
//	    			else if (tagGroup.product == productService.getProductByName(Product.rdsInstance))
//	    				logger.error("   --- RDS instance tagGroup: " + tagGroup);
	    		}
	    	}
	    }
	    
		for (int i = 0; i < usageData.getNum(); i++) {
			// For each hour of usage...
		    Map<TagGroup, Double> usageMap = usageData.getData(i);
		    Map<TagGroup, Double> costMap = costData.getData(i);

			processHour(i, reservationService, usageMap, costMap, startMilli);
		}
		
//		logger.info("process time in seconds: " + Seconds.secondsBetween(start, DateTime.now()).getSeconds());
	}
	
	private void processHour(
			int hour,
			ReservationService reservationService,
			Map<TagGroup, Double> usageMap,
			Map<TagGroup, Double> costMap,
			long startMilli) {
		// Process reservations for the hour using the ReservationsService loaded from the ReservationCapacityPoller (Before Jan 1, 2018)

		Set<ReservationArn> reservationArns = reservationService.getReservations(startMilli + hour * AwsUtils.hourMillis, product);
		    
	    List<TagGroupRI> riTagGroups = Lists.newArrayList();
	    for (TagGroup tagGroup: usageMap.keySet()) {
	    	if (tagGroup instanceof TagGroupRI) {
	    		riTagGroups.add((TagGroupRI) tagGroup);
	    	}
	    }
	    
	    for (ReservationArn reservationArn: reservationArns) {		    	
		    // Get the reservation info for the utilization and tagGroup in the current hour
		    ReservationService.ReservationInfo reservation = reservationService.getReservation(reservationArn);
		    double reservedUnused = reservation.capacity;
		    TagGroup rtg = reservation.tagGroup;
						    
		    PurchaseOption purchaseOption = ((ReservationOperation) rtg.operation).getPurchaseOption();
		    
		    double savingsRate = 0.0;
		    if (startMilli < jan1_2018) {
		        InstancePrices instancePrices = prices.get(rtg.product);
			    double onDemandRate = instancePrices.getOnDemandRate(rtg.region, rtg.usageType);			    
		        savingsRate = onDemandRate - reservation.reservationHourlyCost - reservation.upfrontAmortized;
		    }
		    
            if (ReservationArn.debugReservationArn != null && hour == 0 && reservationArn == ReservationArn.debugReservationArn) {
            	logger.info("RI hour 0: capacity/initial reservedUnused=" + reservedUnused + ", reservationArn=" + reservationArn);
            }
            
		    for (TagGroupRI tg: riTagGroups) {
		    	if (tg.arn != reservationArn)
		    		continue;
		    	
			    // grab the RI tag group value and add it to the remove list
			    Double used = usageMap.remove(tg);

			    Double cost = null;
			    Double amort = null;
			    Double savings = null;
			    if (startMilli >= jan1_2018) {
				    /*
				     *  Cost, Amortization, and Savings will be in the map as of Jan. 1, 2018
				     */
				    cost = costMap.remove(tg);
				    if ((cost == null || cost == 0) && purchaseOption != PurchaseOption.AllUpfront)
				    	logger.warn("No cost in map for tagGroup: " + tg);			    
				    
				    amort = null;
				    if (purchaseOption != PurchaseOption.NoUpfront) {
					    // See if we have amortization in the map already
					    TagGroupRI atg = TagGroupRI.get(tg.account, tg.region, tg.zone, tg.product, Operation.getUpfrontAmortized(purchaseOption), tg.usageType, tg.resourceGroup, tg.arn);
					    amort = costMap.remove(atg);
					    if (hour == 0 && amort == null)
					    	logger.warn("No amortization in map for tagGroup: " + atg);
				    }
				    
				    TagGroupRI stg = TagGroupRI.get(tg.account, tg.region, tg.zone, tg.product, Operation.getSavings(purchaseOption), tg.usageType, tg.resourceGroup, tg.arn);
				    savings = costMap.remove(stg);
				    if (hour == 0 && savings == null)
				    	logger.warn("No savings in map for tagGroup: " + stg);
			    }
			    
			    if (used != null && used > 0.0) {
			    	double adjustedUsed = convertFamilyUnits(used, tg.usageType, rtg.usageType);
				    // If CUR has recurring cost (starting 2018-01), then it's already in the map. Otherwise we have to compute it from the reservation
			    	double adjustedCost = (cost != null && cost > 0) ? cost : adjustedUsed * reservation.reservationHourlyCost;
			    	double adjustedAmortization = (amort != null && amort > 0) ? amort : adjustedUsed * reservation.upfrontAmortized;
			    	double adjustedSavings = (savings != null && savings > 0) ? savings : adjustedUsed * savingsRate;
				    reservedUnused -= adjustedUsed;
	                if (ReservationArn.debugReservationArn != null && hour == 0 && reservationArn == ReservationArn.debugReservationArn) {
	                	logger.info("RI hour 0: cost=" + adjustedCost + ", used=" + used + ", adjustedUsage=" + adjustedUsed + ", reservedUnused=" + reservedUnused + ", tg=" + tg);
	                }
				    if (rtg.account == tg.account) {
					    // Used by owner account, mark as used
					    TagGroup usedTagGroup = null;
					    usedTagGroup = TagGroup.getTagGroup(tg.account, tg.region, tg.zone, tg.product, Operation.getReservedInstances(purchaseOption), tg.usageType, tg.resourceGroup);
					    add(usageMap, usedTagGroup, used);						    
					    add(costMap, usedTagGroup, adjustedCost);						    
				    }
				    else {
				    	// Borrowed by other account, mark as borrowed/lent
					    TagGroup borrowedTagGroup = TagGroup.getTagGroup(tg.account, tg.region, tg.zone, tg.product, Operation.getBorrowedInstances(purchaseOption), tg.usageType, tg.resourceGroup);
					    TagGroup lentTagGroup = TagGroup.getTagGroup(rtg.account, rtg.region, rtg.zone, rtg.product, Operation.getLentInstances(purchaseOption), rtg.usageType, tg.resourceGroup);
					    add(usageMap, borrowedTagGroup, used);
					    add(costMap, borrowedTagGroup, adjustedCost);
					    add(usageMap, lentTagGroup, adjustedUsed);
					    add(costMap, lentTagGroup, adjustedCost);
				    }
				    // assign amortization
				    if (adjustedAmortization > 0.0) {
				        TagGroup upfrontTagGroup = TagGroup.getTagGroup(tg.account, tg.region, tg.zone, tg.product, Operation.getUpfrontAmortized(purchaseOption), tg.usageType, tg.resourceGroup);
					    add(costMap, upfrontTagGroup, adjustedAmortization);
				    }
				    // assign savings
			        TagGroup savingsTagGroup = TagGroup.getTagGroup(tg.account, tg.region, tg.zone, tg.product, Operation.getSavings(purchaseOption), tg.usageType, tg.resourceGroup);
				    add(costMap, savingsTagGroup, adjustedSavings);
			    }
		    }

            if (ReservationArn.debugReservationArn != null && hour == 0 && reservationArn == ReservationArn.debugReservationArn) {
            	logger.info("RI hour 0: total unused=" + reservedUnused + ", reservationArn=" + reservationArn);
            }
		    // Unused
		    boolean haveUnused = Math.abs(reservedUnused) > 0.0001;
		    if (haveUnused) {			    	
			    ResourceGroup riResourceGroup = product == null ? null : rtg.resourceGroup;
			    TagGroup unusedTagGroup = TagGroup.getTagGroup(rtg.account, rtg.region, rtg.zone, rtg.product, Operation.getUnusedInstances(purchaseOption), rtg.usageType, riResourceGroup);
			    add(usageMap, unusedTagGroup, reservedUnused);
			    double unusedHourlyCost = reservedUnused * reservation.reservationHourlyCost;
			    add(costMap, unusedTagGroup, unusedHourlyCost);

			    if (reservedUnused < 0.0) {
			    	logger.error("Too much usage assigned to RI: " + hour + ", unused=" + reservedUnused + ", tag: " + unusedTagGroup);
			    }
			    double unusedFixedCost = reservedUnused * reservation.upfrontAmortized;
			    if (reservation.upfrontAmortized > 0.0) {
				    // assign unused amortization to owner
			        TagGroup upfrontTagGroup = TagGroup.getTagGroup(rtg.account, rtg.region, rtg.zone, rtg.product, Operation.getUpfrontUnusedAmortized(purchaseOption), rtg.usageType, riResourceGroup);
				    add(costMap, upfrontTagGroup, unusedFixedCost);
			    }
				    
			    // subtract amortization and hourly rate from savings for owner
		        TagGroup savingsTagGroup = TagGroup.getTagGroup(rtg.account, rtg.region, rtg.zone, rtg.product, Operation.getSavings(purchaseOption), rtg.usageType, riResourceGroup);
			    add(costMap, savingsTagGroup, -unusedFixedCost - unusedHourlyCost);
		    }
	    }
	    
	    // Scan the usage and cost maps to clean up any leftover entries with TagGroupRI
	    cleanup(hour, usageMap, "usage");
	    cleanup(hour, costMap, "cost");
	}
	
	private void cleanup(int hour, Map<TagGroup, Double> data, String which) {
	    List<TagGroupRI> riTagGroups = Lists.newArrayList();
	    for (TagGroup tagGroup: data.keySet()) {
	    	if (tagGroup instanceof TagGroupRI) {
	    		riTagGroups.add((TagGroupRI) tagGroup);
	    	}
	    }
	    Map<Tag, Integer> leftovers = Maps.newHashMap();
	    for (TagGroupRI tg: riTagGroups) {
	    	Integer i = leftovers.get(tg.operation);
	    	i = 1 + ((i == null) ? 0 : i);
	    	leftovers.put(tg.operation, i);
	    	
	    	Double v = data.remove(tg);
	    	TagGroup newTg = TagGroup.getTagGroup(tg.account, tg.region, tg.zone, tg.product, tg.operation, tg.usageType, tg.resourceGroup);
	    	add(data, newTg, v);
	    }
	    for (Tag t: leftovers.keySet())
	    	logger.info("Found " + leftovers.get(t) + " unconverted " + which + " RI TagGroups on hour " + hour + " for operation " + t);
	}
}
