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
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.ReservationService.ReservationUtilization;
import com.netflix.ice.processor.pricelist.InstancePrices;
import com.netflix.ice.processor.pricelist.PriceListService;
import com.netflix.ice.reader.InstanceMetrics;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.UsageType;

public abstract class ReservationProcessor {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    // hour of data to print debug statements. Set to -1 to turn off all debug logging.
    protected int debugHour = -1;
    protected String debugFamily = "db";
    // debugAccounts - will print all accounts if null
    protected String[] debugAccounts = null; // { "AccountName" };
    // debugUtilization - will print all utilization if null
    protected ReservationUtilization debugUtilization = null; // ReservationUtilization.PARTIAL;
    // debugRegion - will print all regions if null
    protected Region[] debugRegions = { Region.AP_SOUTHEAST_2 };
    
    protected ProductService productService;
    protected PriceListService priceListService;
    
    // The following are initialized on each call to process()
    protected InstanceMetrics instanceMetrics = null;
    protected Map<Product, InstancePrices> prices;
    protected Product product;
    protected boolean familyBreakout;

    public ReservationProcessor(Set<Account> reservationOwners,
    		ProductService productService, PriceListService priceListService, boolean familyBreakout) throws IOException {
    	this.productService = productService;
    	this.priceListService = priceListService;
    	this.familyBreakout = familyBreakout;
    	
    }
    
    public void setDebugHour(int i) {
    	debugHour = i;
    }
    
    public int getDebugHour() {
    	return debugHour;
    }
    
    public void setDebugFamily(String family) {
    	debugFamily = family;
    }
    
    public String getDebugFamily() {
    	return debugFamily;
    }
    
    public void setDebugAccounts(String[] accounts) {
    	debugAccounts = accounts;
    }
    
    public String[] getDebugAccounts() {
    	return debugAccounts;
    }
    
    public void setDebugRegions(Region[] regions) {
    	debugRegions = regions;
    }
    
    public Region[] getDebugRegions() {
    	return debugRegions;
    }
    
	protected boolean debugReservations(int i, TagGroup tg, ReservationUtilization utilization) {
		// Must have match on debugHour
		if (i != debugHour)
			return false;
		
		// Must have match on debugUtilization if it isn't null
		if (debugUtilization != null && utilization != debugUtilization)
			return false;
		
		// Must have match on region if debugRegions isn't null
		if (debugRegions != null) {
			boolean match = false;
			for (Region r: debugRegions) {
				if (r == tg.region) {
					match = true;
					break;
				}
			}
			if (!match)
				return false;
		}
		
		// Must have match on account if debugAccounts isn't null
		if (debugAccounts != null) {
			boolean match = false;
			for (String a: debugAccounts) {
				if (a.equals(tg.account.name)) {
					match = true;
					break;
				}
			}
			if (!match)
				return false;
		}
		
		if (debugFamily.isEmpty()) {
			return true;
		}
		String family = tg.usageType.name.split("\\.")[0];
		if (family.equals(debugFamily)) {
			return true;
		}
		return false;
	}
   
	protected double convertFamilyUnits(double units, UsageType from, UsageType to) {
		double fromNorm = instanceMetrics.getNormalizationFactor(from);
		if (from.isMultiAZ())
			fromNorm *= 2.0;
		double toNorm = instanceMetrics.getNormalizationFactor(to);
		if (to.isMultiAZ())
			toNorm *= 2.0;
		return units *  fromNorm / toNorm;
	}

	
	/*
	 * process() will run through all the usage data looking for reservation usage and
	 * associate it with the appropriate reservations found in the reservation owner
	 * accounts. It handles both AZ and Regional scoped reservations including borrowing
	 * across accounts linked through consolidated billing and sharing of instance reservations
	 * among instance types in the same family.
	 * 
	 * The order of processing is as follows:
	 *  1. AZ-scoped reservations used within the owner account.
	 *  2. AZ-scoped reservations borrowed by other accounts within the consolidated group.
	 *  3. Region-scoped reservations used within the owner account.
	 *  4. Region-scoped reservations used within the owner account but from a different instance type within the same family.
	 *  5. Region-scoped reservations borrowed by other accounts
	 *  6. Region-scoped reservations borrowed by other accounts but from a different instance type within the same family.
	 * 
	 * When called, all usage data is flagged as bonusReserved. The job of this method is to
	 * walk through all the bonus tags and convert them to the proper reservation usage type
	 * based on the association of the actual reservations. Since the detailed billing reports
	 * don't tell us which reservation was actually used for each line item, we mimic the AWS rules
	 * as best we can. The actual usage in AWS may be slightly different. Only the AWS Cost and Usage
	 * reports fully provide the reservation ARN that is associated with each usage, but ICE doesn't
	 * use those reports. (Added support recently for processing based on cost and usage reports, but
	 * haven't done the work to grab and use the reservation ARNs.)
	 * 
	 * Amortization of up front costs is assigned to the reservations instance type in the account that purchased
	 * the reservation regardless of how it was used - either by a different family type or borrowed by another account.
	 * 
	 * Savings values, like amortization are assigned to the reservation instance type in the account that purchased
	 * the reservation. Savings is computed as the difference between the on-demand cost and the sum of the upfront amortization
	 * and hourly rate. Any unused instance costs are subtracted from the savings.
	 * 
	 * We initially calculate the max savings possible:
	 * 
	 * 	max_savings = reservation.capacity * (onDemand_rate - hourly_rate - hourly_amortization)
	 * 
	 * We then subtract off the onDemand cost of unused reservations to get actual savings
	 * 
	 * actual_savings = max_savings - (onDemand_rate * unusedCount)
	 */
	public void process(ReservationService reservationService,
			CostAndUsageData data,
			Product product,
			DateTime start,
			Map<Product, InstancePrices> prices) throws Exception {
		
		this.prices = prices;
		ReadWriteData usageData = data.getUsage(product);
		ReadWriteData costData = data.getCost(product);

		if (usageData == null || costData == null)
			return;
		
		this.product = product;

    	logger.info("---------- Process " + (product == null ? "Non-resource" : product));
    	
		if (debugHour >= 0)
			printUsage("before", usageData, costData);
				
    	instanceMetrics = priceListService.getInstanceMetrics();
		
		long startMilli = start.getMillis();
		
		processReservations(reservationService, data, startMilli);
				
		if (debugHour >= 0)
			printUsage("after", usageData, costData);		
	}
	
	abstract protected void processReservations(
			ReservationService reservationService,
			CostAndUsageData data,
			Long startMilli);	
		
	protected void printUsage(String when, ReadWriteData usageData, ReadWriteData costData) {
		logger.info("---------- usage and cost for hour " + debugHour + " " + when + " processing ----------------");
	    Map<TagGroup, Double> usageMap = usageData.getData(debugHour);
		
		SortedSet<TagGroup> usageTags = Sets.newTreeSet();
		usageTags.addAll(usageData.getTagGroups());
		for (TagGroup tagGroup: usageTags) {
			if (tagGroup.operation == Operation.ondemandInstances || tagGroup.operation == Operation.spotInstances)
				continue;
			if ((tagGroup.product.hasReservations())
					&& usageMap.get(tagGroup) != null 
					&& debugReservations(debugHour, tagGroup, ((Operation.ReservationOperation) tagGroup.operation).getUtilization())) {
				logger.info("usage " + usageMap.get(tagGroup) + " for tagGroup: " + tagGroup);
			}
		}

		Map<TagGroup, Double> costMap = costData.getData(debugHour);

		SortedSet<TagGroup> costTags = Sets.newTreeSet();
		costTags.addAll(costData.getTagGroups());
		for (TagGroup tagGroup: costTags) {
			if (tagGroup.operation == Operation.ondemandInstances || tagGroup.operation == Operation.spotInstances)
				continue;
			if ((tagGroup.product.hasReservations())
					&& costMap.get(tagGroup) != null 
					&& debugReservations(debugHour, tagGroup, ((Operation.ReservationOperation) tagGroup.operation).getUtilization())) {
				logger.info("cost " + costMap.get(tagGroup) + " for tagGroup: " + tagGroup);
			}
		}
		logger.info("---------- end of usage and cost report ----------------");
	}
}

