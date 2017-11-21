package com.netflix.ice.processor;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.common.TagGroupRI;
import com.netflix.ice.processor.ReservationService.ReservationUtilization;
import com.netflix.ice.processor.pricelist.InstancePrices;
import com.netflix.ice.processor.pricelist.PriceListService;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Operation.ReservationOperation;

public class CostAndUsageReservationProcessor extends ReservationProcessor {

	public CostAndUsageReservationProcessor(
			Map<Account, List<Account>> payerAccounts,
			Set<Account> reservationOwners, ProductService productService,
			PriceListService priceListService, boolean familyBreakout) throws IOException {
		super(payerAccounts, reservationOwners, productService,
				priceListService, familyBreakout);
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
			ReadWriteData usageData,
			ReadWriteData costData,
			Long startMilli) {
		
		// Scan the first hour and look for reservation usage with no ARN
	    for (TagGroup tagGroup: usageData.getData(0).keySet()) {
	    	if (tagGroup.operation instanceof ReservationOperation) {
	    		ReservationOperation ro = (ReservationOperation) tagGroup.operation;
	    		if (ro.getUtilization() != null) {
	    			if (!(tagGroup instanceof TagGroupRI))
	    				logger.error("   --- Reserved Instance usage without reservation ID: " + tagGroup + ", " + usageData.getData(0).get(tagGroup));
//	    			else if (tagGroup.product == productService.getProductByName(Product.rdsInstance))
//	    				logger.error("   --- RDS instance tagGroup: " + tagGroup);
	    		}
	    	}
	    }
		
		for (int i = 0; i < usageData.getNum(); i++) {
			// For each hour of usage...
			Set<String> reservationIds = reservationService.getReservations(startMilli + i * AwsUtils.hourMillis);
		
		    Map<TagGroup, Double> usageMap = usageData.getData(i);
		    Map<TagGroup, Double> costMap = costData.getData(i);
		    Set<TagGroup> toBeRemoved = Sets.newHashSet();
		    Map<TagGroup, Double> toBeAdded = Maps.newHashMap();
		    
		    for (String reservationId: reservationIds) {		    	
			    // Get the reservation info for the utilization and tagGroup in the current hour
			    ReservationService.ReservationInfo reservation = reservationService.getReservation(reservationId);
			    double reservedUnused = reservation.capacity;
			    TagGroup rtg = reservation.tagGroup;
			    
			    ReservationUtilization utilization = ((ReservationOperation) rtg.operation).getUtilization();
		        InstancePrices instancePrices = prices.get(reservation.tagGroup.product);
			    double onDemandRate = instancePrices.getOnDemandRate(reservation.tagGroup.region, reservation.tagGroup.usageType);
		        double savingsRate = onDemandRate - reservation.reservationHourlyCost - reservation.upfrontAmortized;
			    
			    for (TagGroup tagGroup: usageMap.keySet()) {
			    	if (!(tagGroup instanceof TagGroupRI)) {
			    		continue;
			    	}
			    	TagGroupRI tg = (TagGroupRI) tagGroup;
			    	if (!tg.reservationId.equals(reservationId))
			    		continue;
			    	
				    // grab the RI tag group value and add it to the remove list
				    Double used = usageMap.get(tg);
				    toBeRemoved.add(tg);
				    // remove the corresponding cost entry
				    costMap.remove(tg);
				    
				    if (used != null && used > 0.0) {
				    	double adjustedUsed = convertFamilyUnits(used, tg.usageType, rtg.usageType);
					    reservedUnused -= adjustedUsed;
					    if (reservation.tagGroup.account == tg.account) {
						    // Used by owner account, mark as used
						    TagGroup usedTagGroup = null;
						    if (used == adjustedUsed || !familyBreakout)
						    	usedTagGroup = TagGroup.getTagGroup(tg.account, tg.region, tg.zone, tg.product, Operation.getReservedInstances(utilization), tg.usageType, tg.resourceGroup);
						    else
						    	usedTagGroup = TagGroup.getTagGroup(tg.account, tg.region, tg.zone, tg.product, Operation.getFamilyReservedInstances(utilization), tg.usageType, tg.resourceGroup);
						    add(toBeAdded, usedTagGroup, used);
						    add(costMap, usedTagGroup, adjustedUsed * reservation.reservationHourlyCost);						    
					    }
					    else {
					    	// Borrowed by other account, mark as borrowed/lent
						    TagGroup borrowedTagGroup = TagGroup.getTagGroup(tg.account, tg.region, tg.zone, tg.product, Operation.getBorrowedInstances(utilization), tg.usageType, tg.resourceGroup);
						    TagGroup lentTagGroup = TagGroup.getTagGroup(rtg.account, rtg.region, rtg.zone, rtg.product, Operation.getLentInstances(utilization), rtg.usageType, tg.resourceGroup);
						    add(toBeAdded, borrowedTagGroup, used);
						    add(costMap, borrowedTagGroup, adjustedUsed * reservation.reservationHourlyCost);
						    add(toBeAdded, lentTagGroup, adjustedUsed);
						    add(costMap, lentTagGroup, adjustedUsed * reservation.reservationHourlyCost);
					    }
					    // assign amortization
					    if (reservation.upfrontAmortized > 0.0) {
					        TagGroup upfrontTagGroup = TagGroup.getTagGroup(tg.account, tg.region, tg.zone, tg.product, Operation.getUpfrontAmortized(utilization), tg.usageType, tg.resourceGroup);
						    add(costMap, upfrontTagGroup, adjustedUsed * reservation.upfrontAmortized);
					    }
					    // assign savings
				        TagGroup savingsTagGroup = TagGroup.getTagGroup(tg.account, tg.region, tg.zone, tg.product, Operation.getSavings(utilization), tg.usageType, tg.resourceGroup);
					    add(costMap, savingsTagGroup, adjustedUsed * savingsRate);
				    }
			    }

			    // Unused
			    TagGroup unusedTagGroup = TagGroup.getTagGroup(rtg.account, rtg.region, rtg.zone, rtg.product, Operation.getUnusedInstances(utilization), rtg.usageType, rtg.resourceGroup);
			    if (reservedUnused != 0.0) {
				    add(toBeAdded, unusedTagGroup, reservedUnused);
				    double unusedHourlyCost = reservedUnused * reservation.reservationHourlyCost;
				    add(costMap, unusedTagGroup, unusedHourlyCost);
				    if (reservedUnused < 0.0 && Math.abs(reservedUnused) > 0.00001) {
				    	logger.error("Too much usage assigned to RI: " + i + ", unused=" + reservedUnused + ", tag: " + unusedTagGroup);
				    }
				    // assign unused amortization to owner
				    double unusedFixedCost = reservedUnused * reservation.upfrontAmortized;
			        TagGroup upfrontTagGroup = TagGroup.getTagGroup(rtg.account, rtg.region, rtg.zone, rtg.product, Operation.getUpfrontAmortized(utilization), rtg.usageType, rtg.resourceGroup);
				    add(costMap, upfrontTagGroup, unusedFixedCost);
				    
				    // subtract amortization and hourly rate from savings for owner
			        TagGroup savingsTagGroup = TagGroup.getTagGroup(rtg.account, rtg.region, rtg.zone, rtg.product, Operation.getSavings(utilization), rtg.usageType, rtg.resourceGroup);
				    add(costMap, savingsTagGroup, -unusedFixedCost - unusedHourlyCost);
			    }
		    }
		    // Remove the entries we replaced
		    for (TagGroup tg: toBeRemoved) {
		    	usageMap.remove(tg);
		    }
		    // Add the new ones
		    usageMap.putAll(toBeAdded);
		}
	}
}
