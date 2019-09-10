package com.netflix.ice.processor;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

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
import com.netflix.ice.tag.ResourceGroup;

public class CostAndUsageReservationProcessor extends ReservationProcessor {
    // Unused rates and amortization for RIs were added to CUR on 2018-01
	final static long jan1_2018 = new DateTime("2018-01", DateTimeZone.UTC).getMillis();

	public CostAndUsageReservationProcessor(
			Set<Account> reservationOwners, ProductService productService,
			PriceListService priceListService, boolean familyBreakout) throws IOException {
		super(reservationOwners, productService,
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
			CostAndUsageData data,
			Long startMilli) {
		

		ReadWriteData usageData = data.getUsage(product);
		ReadWriteData costData = data.getCost(product);

		// Scan the first hour and look for reservation usage with no ARN and log errors
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
		    Map<TagGroup, Double> usageMap = usageData.getData(i);
		    Map<TagGroup, Double> costMap = costData.getData(i);

		    if (startMilli >= jan1_2018)
				processHourRiFee(i, reservationService, usageMap, costMap, startMilli);
			else
				processHour(i, reservationService, usageMap, costMap, startMilli);
		}
	}

	private void processHourRiFee(
			int hour,
			ReservationService reservationService,
			Map<TagGroup, Double> usageMap,
			Map<TagGroup, Double> costMap,
			long startMilli)
	{
		// Process reservations for the hour using a ReservationService loaded from RIFee lineItems (Starting Jan 1, 2018)
		Set<String> reservationIds = reservationService.getReservations(startMilli + hour * AwsUtils.hourMillis, product);
		
	    Set<TagGroup> toBeRemoved = Sets.newHashSet();
	    Map<TagGroup, Double> toBeAdded = Maps.newHashMap();
	    
	    for (String reservationId: reservationIds) {		    	
		    // Get the reservation info for the utilization and tagGroup in the current hour
		    ReservationService.ReservationInfo reservation = reservationService.getReservation(reservationId);
		    double reservedUnused = reservation.capacity;
		    TagGroup rtg = reservation.tagGroup;
		    
		    ReservationUtilization utilization = ((ReservationOperation) rtg.operation).getUtilization();

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
			    Double cost = costMap.remove(tg);
			    if (cost == null && utilization != ReservationUtilization.ALL)
			    	logger.error("No cost in map for tagGroup: " + tg);			    
			    
			    Double amort = null;
			    if (utilization != ReservationUtilization.NO) {
				    // See if we have amortization in the map already
				    TagGroupRI atg = TagGroupRI.getTagGroup(tg.account, tg.region, tg.zone, tg.product, Operation.getUpfrontAmortized(utilization), tg.usageType, tg.resourceGroup, tg.reservationId);
				    amort = costMap.remove(atg);
				    if (hour == 0 && amort == null)
				    	logger.error("No amortization in map for tagGroup: " + atg);
			    }
			    
			    // See if we have savings in the map already
			    TagGroupRI stg = TagGroupRI.getTagGroup(tg.account, tg.region, tg.zone, tg.product, Operation.getSavings(utilization), tg.usageType, tg.resourceGroup, tg.reservationId);
			    Double savings = costMap.remove(stg);
			    if (hour == 0 && savings == null)
			    	logger.error("No savings in map for tagGroup: " + stg);
			    
			    if (used != null && used > 0.0) {
			    	double adjustedUsed = convertFamilyUnits(used, tg.usageType, rtg.usageType);
//	                if (hour == 0 && tg.product.isRdsInstance() && tg.reservationId.equals("foobar")) {
//	                	logger.info("RDS RI: used=" + used + ", adjustedUsage=" + adjustedUsed + ", tg=" + tagGroup);
//	                }
				    // If CUR has recurring cost (starting 2018-01), then it's already in the map. Otherwise we have to compute it from the reservation
			    	double adjustedCost = (cost != null && cost > 0) ? cost : adjustedUsed * reservation.reservationHourlyCost;
			    	double adjustedAmortization = (amort != null && amort > 0) ? amort : adjustedUsed * reservation.upfrontAmortized;
			    	double adjustedSavings = (savings != null && savings > 0) ? savings : 0;
				    reservedUnused -= adjustedUsed;
				    if (rtg.account == tg.account) {
					    // Used by owner account, mark as used
					    TagGroup usedTagGroup = null;
					    if (used == adjustedUsed || !familyBreakout)
					    	usedTagGroup = TagGroup.getTagGroup(tg.account, tg.region, tg.zone, tg.product, Operation.getReservedInstances(utilization), tg.usageType, tg.resourceGroup);
					    else
					    	usedTagGroup = TagGroup.getTagGroup(tg.account, tg.region, tg.zone, tg.product, Operation.getFamilyReservedInstances(utilization), tg.usageType, tg.resourceGroup);
					    add(toBeAdded, usedTagGroup, used);						    
					    add(costMap, usedTagGroup, adjustedCost);						    
				    }
				    else {
				    	// Borrowed by other account, mark as borrowed/lent
					    TagGroup borrowedTagGroup = TagGroup.getTagGroup(tg.account, tg.region, tg.zone, tg.product, Operation.getBorrowedInstances(utilization), tg.usageType, tg.resourceGroup);
					    TagGroup lentTagGroup = TagGroup.getTagGroup(rtg.account, rtg.region, rtg.zone, rtg.product, Operation.getLentInstances(utilization), rtg.usageType, tg.resourceGroup);
					    add(toBeAdded, borrowedTagGroup, used);
					    add(costMap, borrowedTagGroup, adjustedCost);
					    add(toBeAdded, lentTagGroup, adjustedUsed);
					    add(costMap, lentTagGroup, adjustedCost);
				    }
				    // assign amortization
				    if (adjustedAmortization > 0.0) {
				        TagGroup upfrontTagGroup = TagGroup.getTagGroup(tg.account, tg.region, tg.zone, tg.product, Operation.getUpfrontAmortized(utilization), tg.usageType, tg.resourceGroup);
					    add(costMap, upfrontTagGroup, adjustedAmortization);
				    }
				    // assign savings
			        TagGroup savingsTagGroup = TagGroup.getTagGroup(tg.account, tg.region, tg.zone, tg.product, Operation.getSavings(utilization), tg.usageType, tg.resourceGroup);
				    add(costMap, savingsTagGroup, adjustedSavings);
			    }
		    }

		    // Unused
		    boolean haveUnused = Math.abs(reservedUnused) > 0.0001;
		    if (haveUnused) {		    			
			    ResourceGroup riResourceGroup = product == null ? null : rtg.resourceGroup;
			    TagGroup unusedTagGroup = TagGroup.getTagGroup(rtg.account, rtg.region, rtg.zone, rtg.product, Operation.getUnusedInstances(utilization), rtg.usageType, riResourceGroup);
			    add(toBeAdded, unusedTagGroup, reservedUnused);
			    double unusedHourlyCost = reservedUnused * reservation.reservationHourlyCost;
			    add(costMap, unusedTagGroup, unusedHourlyCost);

			    if (reservedUnused < 0.0) {
			    	logger.error("Too much usage assigned to RI: " + hour + ", unused=" + reservedUnused + ", tag: " + unusedTagGroup + "," + reservationId);
			    }
			    double unusedFixedCost = reservedUnused * reservation.upfrontAmortized;
			    if (reservation.upfrontAmortized > 0.0) {
				    // assign unused amortization to owner
			        TagGroup upfrontTagGroup = TagGroup.getTagGroup(rtg.account, rtg.region, rtg.zone, rtg.product, Operation.getUpfrontAmortized(utilization), rtg.usageType, riResourceGroup);
				    add(costMap, upfrontTagGroup, unusedFixedCost);
			    }
				    
			    // subtract amortization and hourly rate from savings for owner
		        TagGroup savingsTagGroup = TagGroup.getTagGroup(rtg.account, rtg.region, rtg.zone, rtg.product, Operation.getSavings(utilization), rtg.usageType, riResourceGroup);
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
	
	private void processHour(
			int hour,
			ReservationService reservationService,
			Map<TagGroup, Double> usageMap,
			Map<TagGroup, Double> costMap,
			long startMilli) {
		// Process reservations for the hour using the ReservationsService loaded from the ReservationCapacityPoller (Before Jan 1, 2018)

		Set<String> reservationIds = reservationService.getReservations(startMilli + hour * AwsUtils.hourMillis, product);
	
	    Set<TagGroup> toBeRemoved = Sets.newHashSet();
	    Map<TagGroup, Double> toBeAdded = Maps.newHashMap();
	    
	    for (String reservationId: reservationIds) {		    	
		    // Get the reservation info for the utilization and tagGroup in the current hour
		    ReservationService.ReservationInfo reservation = reservationService.getReservation(reservationId);
		    double reservedUnused = reservation.capacity;
		    TagGroup rtg = reservation.tagGroup;
						    
		    ReservationUtilization utilization = ((ReservationOperation) rtg.operation).getUtilization();
	        InstancePrices instancePrices = prices.get(rtg.product);
		    double onDemandRate = instancePrices.getOnDemandRate(rtg.region, rtg.usageType);			    
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
			    Double cost = costMap.remove(tg);
			    			    
			    // See if we have savings in the map already
			    TagGroupRI stg = TagGroupRI.getTagGroup(tg.account, tg.region, tg.zone, tg.product, Operation.getSavings(utilization), tg.usageType, tg.resourceGroup, tg.reservationId);
			    Double savings = costMap.remove(stg);
			    
			    if (used != null && used > 0.0) {
			    	double adjustedUsed = convertFamilyUnits(used, tg.usageType, rtg.usageType);
				    // If CUR has recurring cost (starting 2018-01), then it's already in the map. Otherwise we have to compute it from the reservation
			    	double adjustedCost = (cost != null && cost > 0) ? cost : adjustedUsed * reservation.reservationHourlyCost;
			    	double adjustedAmortization = adjustedUsed * reservation.upfrontAmortized;
			    	double adjustedSavings = (savings != null && savings > 0) ? savings : adjustedUsed * savingsRate;
				    reservedUnused -= adjustedUsed;
				    if (rtg.account == tg.account) {
					    // Used by owner account, mark as used
					    TagGroup usedTagGroup = null;
					    if (used == adjustedUsed || !familyBreakout)
					    	usedTagGroup = TagGroup.getTagGroup(tg.account, tg.region, tg.zone, tg.product, Operation.getReservedInstances(utilization), tg.usageType, tg.resourceGroup);
					    else
					    	usedTagGroup = TagGroup.getTagGroup(tg.account, tg.region, tg.zone, tg.product, Operation.getFamilyReservedInstances(utilization), tg.usageType, tg.resourceGroup);
					    add(toBeAdded, usedTagGroup, used);						    
					    add(costMap, usedTagGroup, adjustedCost);						    
				    }
				    else {
				    	// Borrowed by other account, mark as borrowed/lent
					    TagGroup borrowedTagGroup = TagGroup.getTagGroup(tg.account, tg.region, tg.zone, tg.product, Operation.getBorrowedInstances(utilization), tg.usageType, tg.resourceGroup);
					    TagGroup lentTagGroup = TagGroup.getTagGroup(rtg.account, rtg.region, rtg.zone, rtg.product, Operation.getLentInstances(utilization), rtg.usageType, tg.resourceGroup);
					    add(toBeAdded, borrowedTagGroup, used);
					    add(costMap, borrowedTagGroup, adjustedCost);
					    add(toBeAdded, lentTagGroup, adjustedUsed);
					    add(costMap, lentTagGroup, adjustedCost);
				    }
				    // assign amortization
				    if (adjustedAmortization > 0.0) {
				        TagGroup upfrontTagGroup = TagGroup.getTagGroup(tg.account, tg.region, tg.zone, tg.product, Operation.getUpfrontAmortized(utilization), tg.usageType, tg.resourceGroup);
					    add(costMap, upfrontTagGroup, adjustedAmortization);
				    }
				    // assign savings
			        TagGroup savingsTagGroup = TagGroup.getTagGroup(tg.account, tg.region, tg.zone, tg.product, Operation.getSavings(utilization), tg.usageType, tg.resourceGroup);
				    add(costMap, savingsTagGroup, adjustedSavings);
			    }
		    }

		    // Unused
		    boolean haveUnused = Math.abs(reservedUnused) > 0.0001;
		    if (haveUnused) {			    	
			    ResourceGroup riResourceGroup = product == null ? null : rtg.resourceGroup;
			    TagGroup unusedTagGroup = TagGroup.getTagGroup(rtg.account, rtg.region, rtg.zone, rtg.product, Operation.getUnusedInstances(utilization), rtg.usageType, riResourceGroup);
			    add(toBeAdded, unusedTagGroup, reservedUnused);
			    double unusedHourlyCost = reservedUnused * reservation.reservationHourlyCost;
			    add(costMap, unusedTagGroup, unusedHourlyCost);

			    if (reservedUnused < 0.0) {
			    	logger.error("Too much usage assigned to RI: " + hour + ", unused=" + reservedUnused + ", tag: " + unusedTagGroup);
			    }
			    double unusedFixedCost = reservedUnused * reservation.upfrontAmortized;
			    if (reservation.upfrontAmortized > 0.0) {
				    // assign unused amortization to owner
			        TagGroup upfrontTagGroup = TagGroup.getTagGroup(rtg.account, rtg.region, rtg.zone, rtg.product, Operation.getUpfrontAmortized(utilization), rtg.usageType, riResourceGroup);
				    add(costMap, upfrontTagGroup, unusedFixedCost);
			    }
				    
			    // subtract amortization and hourly rate from savings for owner
		        TagGroup savingsTagGroup = TagGroup.getTagGroup(rtg.account, rtg.region, rtg.zone, rtg.product, Operation.getSavings(utilization), rtg.usageType, riResourceGroup);
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
