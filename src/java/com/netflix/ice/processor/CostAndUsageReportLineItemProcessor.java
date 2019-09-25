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

import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.netflix.ice.basic.BasicLineItemProcessor;
import com.netflix.ice.basic.BasicReservationService.Reservation;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.LineItem;
import com.netflix.ice.common.LineItem.LineItemType;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.common.TagGroupRI;
import com.netflix.ice.processor.ReservationService.ReservationUtilization;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Operation.ReservationOperation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.ReservationArn;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.Zone;

/*
* All reservation usage starts out tagged as BonusReservedInstances and is later reassigned proper tags
* based on it's usage by the ReservationProcessor.
*/
public class CostAndUsageReportLineItemProcessor extends BasicLineItemProcessor {
	public final static long jan1_2018 = new DateTime("2018-01", DateTimeZone.UTC).getMillis();
	
	public CostAndUsageReportLineItemProcessor(AccountService accountService,
			ProductService productService,
			ReservationService reservationService,
			ResourceService resourceService) {
		super(accountService, productService, reservationService,
				resourceService);
	}
   
	@Override
    protected boolean ignore(long startMilli, LineItem lineItem) {
    	// Cost and Usage report-specific checks
    	LineItemType lit = lineItem.getLineItemType();
    	if (lit == LineItemType.Tax ||
    		lit == LineItemType.EdpDiscount ||
    		lit == LineItemType.RiVolumeDiscount)
    		return true;
    	
    	return super.ignore(startMilli, lineItem);
    }

	@Override
    protected Region getRegion(LineItem lineItem) {
        return lineItem.getProductRegion();
    }

	@Override
    protected TagGroup getTagGroup(LineItem lineItem, Account account, Region region, Zone zone, Product product, Operation operation, UsageType usageType, ResourceGroup rg) {
        ReservationArn reservationArn = ReservationArn.get(lineItem.getReservationArn());
        if (operation instanceof Operation.ReservationOperation && !reservationArn.name.isEmpty()) {
        	return TagGroupRI.get(account, region, zone, product, operation, usageType, rg, reservationArn);
        }
        return super.getTagGroup(lineItem, account, region, zone, product, operation, usageType, rg);
    }


	@Override
    protected void addReservation(LineItem lineItem,
    		CostAndUsageData costAndUsageData,
    		TagGroupRI tg,
    		long startMilli) {
    	
        // If processing an RIFee from a CUR, create a reservation for the reservation processor.
        if (lineItem.getLineItemType() != LineItemType.RIFee || startMilli < jan1_2018)
        	return;
        
        // TODO: Handle reservations for DynamoDB
        if (tg.product.isDynamoDB()) {
        	return;
        }
        
        if (!(tg.operation instanceof ReservationOperation)) {
        	logger.error("operation is not a reservation operation, tag: " + tg + "\n" + lineItem);
        	return;
        }
        
        int count = Integer.parseInt(lineItem.getReservationNumberOfReservations());
        long start = new DateTime(lineItem.getReservationStartTime(), DateTimeZone.UTC).getMillis();
        long end = new DateTime(lineItem.getReservationEndTime(), DateTimeZone.UTC).getMillis();
        ReservationUtilization utilization = ((ReservationOperation) tg.operation).getUtilization();
        
        
        Double usageQuantity = Double.parseDouble(lineItem.getUsageQuantity());
        double hourlyFixedPrice = Double.parseDouble(lineItem.getAmortizedUpfrontFeeForBillingPeriod()) / usageQuantity;
        double usagePrice = Double.parseDouble(lineItem.getCost()) / usageQuantity;
        
        double hourlyUnusedFixedPrice = lineItem.getUnusedAmortizedUpfrontRate();
        double unusedUsagePrice = lineItem.getUnusedRecurringRate();
        
        if (hourlyUnusedFixedPrice > 0.0 && Math.abs(hourlyUnusedFixedPrice - hourlyFixedPrice) > 0.0001)
        	logger.info(" used and unused fixed prices are different, used: " + hourlyFixedPrice + ", unused: " + hourlyUnusedFixedPrice + ", tg: " + tg);
        if (unusedUsagePrice > 0.0 && Math.abs(unusedUsagePrice - usagePrice) > 0.0001)
        	logger.info(" used and unused usage prices are different, used: " + usagePrice + ", unused: " + unusedUsagePrice + ", tg: " + tg);
		
        Reservation r = new Reservation(tg, count, start, end, utilization, hourlyFixedPrice, usagePrice);
        costAndUsageData.addReservation(r);
        
        if (ReservationArn.debugReservationArn != null && tg.reservationArn == ReservationArn.debugReservationArn) {
        	logger.info("RI: count=" + r.count + ", tg=" + tg);
        }
    }
	
	private boolean applyMonthlyUsage(LineItemType lineItemType, boolean monthly, Product product) {
        // For CAU reports, EC2, Redshift, and RDS have cost as a monthly charge, but usage appears hourly.
        // 	so unlike EC2, we have to process the monthly line item to capture the cost,
        // 	but we don't want to add the monthly line items to the usage.
        // The reservation processor handles determination on what's unused.
		return lineItemType != LineItemType.Credit && !monthly || !(product.isRedshift() || product.isRdsInstance() || product.isEc2Instance() || product.isElasticsearch() || product.isElastiCache());
	}
	
	private void addHourData(
			LineItemType lineItemType, boolean monthly, TagGroup tagGroup,
			boolean isReservationUsage, ReservationArn reservationArn,
			double usage, double cost, double edpDiscount,
			Map<TagGroup, Double> usages, Map<TagGroup, Double> costs,
			String amort, String publicOnDemandCost, boolean isFirstHour, long startMilli) {
		
		boolean debug = isReservationUsage && ReservationArn.debugReservationArn != null && isFirstHour && reservationArn == ReservationArn.debugReservationArn;
		
        if (applyMonthlyUsage(lineItemType, monthly, tagGroup.product)) {
        	addValue(usages, tagGroup, usage);                	
            if (debug) {
            	logger.info(lineItemType + " usage=" + usage + ", tg=" + tagGroup);
            }
        }
        
        // Additional entries for reservations
        if (isReservationUsage && !tagGroup.product.isDynamoDB()) {
        	if (lineItemType != LineItemType.Credit)
        		addAmortizationAndSavings(tagGroup, reservationArn, costs, tagGroup.product, cost, edpDiscount, amort, publicOnDemandCost, debug, lineItemType, startMilli);
        	// Reservation costs are handled through the ReservationService (see addReservation() above) except
        	// after Jan 1, 2019 when they added net costs to DiscountedUsage records.
        	if (cost != 0 && (lineItemType == LineItemType.Credit || lineItemType == LineItemType.DiscountedUsage)) {
                addValue(costs, tagGroup, cost);           		
                if (debug) {
                	logger.info(lineItemType + " cost=" + cost + ", tg=" + tagGroup);
                }
        	}
        }
        else {
            addValue(costs, tagGroup, cost);
        }
	}

	@Override
    protected void addData(LineItem lineItem, TagGroup tagGroup, TagGroup resourceTagGroup, CostAndUsageData costAndUsageData, 
    		double usageValue, double costValue, boolean monthly, int[] indexes, double edpDiscount, long startMilli) {
		
        final ReadWriteData usageData = costAndUsageData.getUsage(null);
        final ReadWriteData costData = costAndUsageData.getCost(null);
        ReadWriteData usageDataOfProduct = costAndUsageData.getUsage(tagGroup.product);
        ReadWriteData costDataOfProduct = costAndUsageData.getCost(tagGroup.product);
        boolean reservationUsage = lineItem.isReserved();
        final Product product = tagGroup.product;
        ReservationArn reservationArn = ReservationArn.get(lineItem.getReservationArn());
        LineItemType lineItemType = lineItem.getLineItemType();
    	String amort = lineItem.getAmortizedUpfrontCostForUsage();
    	String publicOnDemandCost = lineItem.getPublicOnDemandCost();

        if (resourceService != null) {
            if (usageDataOfProduct == null) {
                usageDataOfProduct = new ReadWriteData();
                costDataOfProduct = new ReadWriteData();
                costAndUsageData.putUsage(tagGroup.product, usageDataOfProduct);
                costAndUsageData.putCost(tagGroup.product, costDataOfProduct);
            }
        }
        
        if (lineItemType == LineItemType.Credit && ReservationArn.debugReservationArn != null && reservationArn == ReservationArn.debugReservationArn)
        	logger.info("Credit: " + lineItem);
        
        for (int i : indexes) {
            Map<TagGroup, Double> usages = usageData.getData(i);
            Map<TagGroup, Double> costs = costData.getData(i);
            addHourData(lineItemType, monthly, tagGroup, reservationUsage, reservationArn, usageValue, costValue, edpDiscount, usages, costs, amort, publicOnDemandCost, i == 0, startMilli);

            if (resourceService != null) {
                Map<TagGroup, Double> usagesOfResource = usageDataOfProduct.getData(i);
                Map<TagGroup, Double> costsOfResource = costDataOfProduct.getData(i);
                
	            if (resourceTagGroup != null) {
	                addHourData(lineItemType, monthly, resourceTagGroup, reservationUsage, reservationArn, usageValue, costValue, edpDiscount, usagesOfResource, costsOfResource, amort, publicOnDemandCost, i == 0, startMilli);
	                
	                // Collect statistics on tag coverage
	            	boolean[] userTagCoverage = resourceService.getUserTagCoverage(lineItem);
	            	costAndUsageData.addTagCoverage(null, i, tagGroup, userTagCoverage);
	            	costAndUsageData.addTagCoverage(product, i, resourceTagGroup, userTagCoverage);
	            }
	            else {
	            	// Save the non-resource-based costs using the product name - same as if it wasn't tagged.	
	                TagGroup tg = TagGroup.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, product, tagGroup.operation, tagGroup.usageType, ResourceGroup.getResourceGroup(product.name, true));
	            	addValue(usagesOfResource, tg, usageValue);
	                addValue(costsOfResource, tg, costValue);           	
	            }
            }
        }
    }
	
	private void addAmortizationAndSavings(TagGroup tagGroup, ReservationArn reservationArn, Map<TagGroup, Double> costs, Product product,
			double costValue, double edpDiscount, String amort, String publicOnDemandCost, boolean debug, LineItemType lineItemType, long startMilli) {
        // If we have an amortization cost from a DiscountedUsage line item, save it as amortization
    	double amortCost = 0.0;
    	if (amort.isEmpty()) {
    		if (startMilli >= jan1_2018)
    			logger.warn(lineItemType + " No amortization in line item for tg=" + tagGroup);
    		return;
    	}
		amortCost = Double.parseDouble(amort);
		if (amortCost > 0.0) {
    		ReservationOperation amortOp = ReservationOperation.getUpfrontAmortized(((ReservationOperation) tagGroup.operation).getUtilization());
    		TagGroupRI tg = TagGroupRI.get(tagGroup.account, tagGroup.region, tagGroup.zone, product, amortOp, tagGroup.usageType, tagGroup.resourceGroup, reservationArn);
    		addValue(costs, tg, amortCost);
            if (debug) {
            	logger.info(lineItemType + " amort=" + amortCost + ", tg=" + tg);
            }
		}

    	// Compute and store savings if Public OnDemand Cost and Amortization is available
    	// Don't include the EDP discount in the savings - we track that separately
    	if (publicOnDemandCost.isEmpty()) {
    		if (startMilli >= jan1_2018)
    			logger.warn(lineItemType + " No public onDemand cost in line item for tg=" + tagGroup);
    		return;
    	}
		ReservationOperation savingsOp = ReservationOperation.getSavings(((ReservationOperation) tagGroup.operation).getUtilization());
		TagGroupRI tg = TagGroupRI.get(tagGroup.account,  tagGroup.region, tagGroup.zone, product, savingsOp, tagGroup.usageType, tagGroup.resourceGroup, reservationArn);
		double publicCost = Double.parseDouble(publicOnDemandCost);
		double edpCost = publicCost * (1 - edpDiscount);
		double savings = edpCost - costValue - amortCost;
		addValue(costs, tg, savings);
        if (debug) {
        	logger.info(lineItemType + " savings=" + savings + ", tg=" + tg);
        }
	}		
	
	@Override
    protected Result getResult(LineItem lineItem, long startMilli, TagGroup tg, boolean processDelayed, boolean reservationUsage, double costValue) {
        Result result = Result.hourly;
        
        switch (lineItem.getLineItemType()) {
        case RIFee:
            // Monthly recurring fees for EC2, RDS, and Redshift reserved instances
        	// Prior to Jan 1, 2018 we have to get cost from the RIFee record, so process as Monthly cost.
        	// As of Jan 1, 2018, we use the recurring fee and amortization values from DiscountedUsage line items.
        	if (startMilli >= jan1_2018) {
	            // We use the RIFee line items to extract the reservation info
		        return processDelayed ? Result.ignore : Result.delay;
        	}
        	else {
        		return processDelayed ? Result.monthly : Result.delay;
        	}
	        
        case DiscountedUsage:
        	return Result.hourly;
        case Credit:
        	return processDelayed ? Result.monthly : Result.delay;
        default:
        	break;
        		
        }
        
        if (tg.product.isDataTransfer()) {
            result = processDataTranfer(processDelayed, tg.usageType);
        }
        else if (tg.product.isCloudHsm()) {
            result = processCloudhsm(processDelayed, tg.usageType);
        }
        else if (tg.product.isEbs()) {
            result = processEbs(tg.usageType);
        }
        else if (tg.product.isRds()) {
            if (tg.usageType.name.startsWith("RDS:ChargedBackupUsage"))
                result = Result.daily;
        }
        
        if (tg.usageType.name.startsWith("TimedStorage-ByteHrs"))
            result = Result.daily;

        return result;
    }

}
