package com.netflix.ice.processor;

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

import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.netflix.ice.basic.BasicLineItemProcessor;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.LineItem;
import com.netflix.ice.common.LineItem.LineItemType;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.common.TagGroupRI;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Operation.ReservationOperation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.Zone;

/*
* All reservation usage starts out tagged as BonusReservedInstances and is later reassigned proper tags
* based on it's usage by the ReservationProcessor.
*/
public class CostAndUsageReportLineItemProcessor extends BasicLineItemProcessor {
	final static long jan1_2018 = new DateTime("2018-01", DateTimeZone.UTC).getMillis();

	public CostAndUsageReportLineItemProcessor(AccountService accountService,
			ProductService productService,
			ReservationService reservationService,
			ResourceService resourceService) {
		super(accountService, productService, reservationService,
				resourceService);
	}
   
	@Override
    protected boolean ignore(LineItem lineItem) {
    	// Cost and Usage report-specific checks
    	LineItemType lit = lineItem.getLineItemType();
    	if (lit == LineItemType.Tax ||
    		lit == LineItemType.EdpDiscount ||
    		lit == LineItemType.RiVolumeDiscount)
    		return true;
    	
    	return super.ignore(lineItem);
    }

	@Override
    protected Region getRegion(LineItem lineItem) {
        return lineItem.getProductRegion();
    }

	@Override
    protected TagGroup getTagGroup(LineItem lineItem, Account account, Region region, Zone zone, Product product, Operation operation, UsageType usageType, ResourceGroup rg) {
        String reservationId = lineItem.getReservationId();
        if (operation instanceof Operation.ReservationOperation && !reservationId.isEmpty()) {
        	return TagGroupRI.getTagGroup(account, region, zone, product, operation, usageType, rg, reservationId);
        }
        return super.getTagGroup(lineItem, account, region, zone, product, operation, usageType, rg);
    }


	@Override
    protected void addUnusedReservationRate(LineItem lineItem,
    		CostAndUsageData costAndUsageData,
    		TagGroupRI tg) {
    	
        // If processing an RIFee from a CUR, grab the unused rates for the reservation processor.
        if (lineItem.getLineItemType() != LineItemType.RIFee)
        	return;
        
        TagGroupRI tagGroup = TagGroupRI.getTagGroup(tg.account, tg.region, tg.zone, tg.product, tg.operation, tg.usageType, null, lineItem.getReservationId());
        ReadWriteData costData = costAndUsageData.getCost(null);
        int endIndex = costData.getNum();
        int[] indexes;
        if (endIndex > 1) {
            indexes = new int[endIndex];
            for (int i = 0; i < indexes.length; i++)
                indexes[i] = i;
        }
        else {
            indexes = new int[]{0};
        }
        
        Double recurringRate = lineItem.getUnusedRecurringRate();
        TagGroupRI recurringTagGroup = null;
        TagGroupRI amortTagGroup = null;
        if (recurringRate != null) {
    		ReservationOperation unusedOp = ReservationOperation.getUnusedInstances(((ReservationOperation) tagGroup.operation).getUtilization());
    		recurringTagGroup = TagGroupRI.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, unusedOp, tagGroup.usageType, null, tagGroup.reservationId);
        }
        Double amortRate = lineItem.getUnusedAmortizedUpfrontRate();
        if (amortRate != null) {
    		ReservationOperation amortOp = ReservationOperation.getUpfrontAmortized(((ReservationOperation) tagGroup.operation).getUtilization());
    		amortTagGroup = TagGroupRI.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, amortOp, tagGroup.usageType, null, tagGroup.reservationId);
        }
        
        for (int i : indexes) {
            Map<TagGroup, Double> costs = costData.getData(i);
            if (recurringRate != null)
            	addValue(costs, recurringTagGroup, recurringRate);
            if (amortRate != null)
            	addValue(costs, amortTagGroup, amortRate);
        }    	
    }

	@Override
    protected void addData(LineItem lineItem, TagGroup tagGroup, TagGroup resourceTagGroup, CostAndUsageData costAndUsageData, 
    		double usageValue, double costValue, boolean monthly, int[] indexes, double edpDiscount) {
		
        final ReadWriteData usageData = costAndUsageData.getUsage(null);
        final ReadWriteData costData = costAndUsageData.getCost(null);
        ReadWriteData usageDataOfProduct = costAndUsageData.getUsage(tagGroup.product);
        ReadWriteData costDataOfProduct = costAndUsageData.getCost(tagGroup.product);
        boolean reservationUsage = lineItem.isReserved();
        final Product product = tagGroup.product;
        String reservationId = lineItem.getReservationId();

        if (resourceService != null) {
            if (usageDataOfProduct == null) {
                usageDataOfProduct = new ReadWriteData();
                costDataOfProduct = new ReadWriteData();
                costAndUsageData.putUsage(tagGroup.product, usageDataOfProduct);
                costAndUsageData.putCost(tagGroup.product, costDataOfProduct);
            }
        }
        
        for (int i : indexes) {
            Map<TagGroup, Double> usages = usageData.getData(i);
            Map<TagGroup, Double> costs = costData.getData(i);
            //
            // For CAU reports, EC2, Redshift, and RDS have cost as a monthly charge, but usage appears hourly.
            // 	so unlike EC2, we have to process the monthly line item to capture the cost,
            // 	but we don't want to add the monthly line items to the usage.
            // The reservation processor handles determination on what's unused.
            if (!monthly || !(product.isRedshift() || product.isRdsInstance() || product.isEc2Instance())) {
            	addValue(usages, tagGroup, usageValue);                	
            }

            addValue(costs, tagGroup, costValue);
            
            // Additional entries for reservations
            if (reservationUsage) {
                // If we have an amortization cost from a DiscountedUsage line item, save it as amortization
            	String amort = lineItem.getAmortizedUpfrontCostForUsage();
            	double amortCost = 0.0;
            	if (!amort.isEmpty()) {
            		amortCost = Double.parseDouble(amort);
            		if (amortCost > 0.0) {
                		ReservationOperation amortOp = ReservationOperation.getUpfrontAmortized(((ReservationOperation) tagGroup.operation).getUtilization());
                		TagGroupRI tg = TagGroupRI.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, product, amortOp, tagGroup.usageType, null, reservationId);
                		addValue(costs, tg, amortCost);
            		}
            	}
            	// Compute and store savings if Public OnDemand Cost is available
            	// Don't include the EDP discount in the savings - we track that separately
            	String publicOnDemandCost = lineItem.getPublicOnDemandCost();
            	if (!publicOnDemandCost.isEmpty()) {
            		ReservationOperation savingsOp = ReservationOperation.getSavings(((ReservationOperation) tagGroup.operation).getUtilization());
            		TagGroupRI tg = TagGroupRI.getTagGroup(tagGroup.account,  tagGroup.region, tagGroup.zone, product, savingsOp, tagGroup.usageType, null, reservationId);
            		double publicCost = Double.parseDouble(publicOnDemandCost);
            		double edpCost = publicCost * (1 - edpDiscount);
            		addValue(costs, tg, edpCost - costValue - amortCost);
            	}
            }

            if (resourceService != null) {
	            if (resourceTagGroup != null) {
	                Map<TagGroup, Double> usagesOfResource = usageDataOfProduct.getData(i);
	                Map<TagGroup, Double> costsOfResource = costDataOfProduct.getData(i);
	
	                if (!((product.isRedshift() || product.isRds()) && monthly)) {
	                	addValue(usagesOfResource, resourceTagGroup, usageValue);
	                }
	                
	                addValue(costsOfResource, resourceTagGroup, costValue);
	                
	                // If we have an amortization cost from a DiscountedUsage line item, save it as amortization
	                if (reservationUsage) {
	                	String amort = lineItem.getAmortizedUpfrontCostForUsage();
	                	if (!amort.isEmpty()) {
	                		double amortCost = Double.parseDouble(amort);
	                		if (amortCost > 0.0) {
		                		ReservationOperation amortOp = ReservationOperation.getUpfrontAmortized(((ReservationOperation) resourceTagGroup.operation).getUtilization());
		                		TagGroupRI tg = TagGroupRI.getTagGroup(tagGroup.account, resourceTagGroup.region, tagGroup.zone, product, amortOp, tagGroup.usageType, resourceTagGroup.resourceGroup, reservationId);
		                		addValue(costsOfResource, tg, amortCost);
	                		}
	                	}
	                }
	                
	                // Collect statistics on tag coverage
	            	boolean[] userTagCoverage = resourceService.getUserTagCoverage(lineItem);
	            	costAndUsageData.addTagCoverage(null, i, tagGroup, userTagCoverage);
	            	costAndUsageData.addTagCoverage(product, i, resourceTagGroup, userTagCoverage);
	            }
	            else {
	            	// Save the non-resource-based costs using the product name - same as if it wasn't tagged.
	                Map<TagGroup, Double> usagesOfResource = usageDataOfProduct.getData(i);
	                Map<TagGroup, Double> costsOfResource = costDataOfProduct.getData(i);
	
	                TagGroup tg = TagGroup.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, product, tagGroup.operation, tagGroup.usageType, ResourceGroup.getResourceGroup(product.name, true));
	            	addValue(usagesOfResource, tg, usageValue);
	                addValue(costsOfResource, tg, costValue);           	
	            }
            }
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
	            // We only use the RIFee line items to extract unused rates
		        return processDelayed ? Result.ignore : Result.delay;
        	}
        	else {
        		return processDelayed ? Result.monthly : Result.delay;
        	}
	        
        case DiscountedUsage:
        	return Result.hourly;
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
