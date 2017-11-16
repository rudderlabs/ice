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
package com.netflix.ice.basic;

import com.google.common.collect.Maps;
import com.netflix.ice.common.*;
import com.netflix.ice.common.LineItem.LineItemType;
import com.netflix.ice.processor.*;
import com.netflix.ice.processor.ReservationService.ReservationUtilization;
import com.netflix.ice.processor.pricelist.InstancePrices;
import com.netflix.ice.tag.*;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/*
 * All reservation usage starts out tagged as BonusReservedInstances and is later reassigned proper tags
 * based on it's usage by the ReservationProcessor.
 */
public class BasicLineItemProcessor implements LineItemProcessor {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    
    private AccountService accountService;
    private ProductService productService;
    private ReservationService reservationService;

    private ResourceService resourceService;
    
    public BasicLineItemProcessor(
    		AccountService accountService, 
    		ProductService productService, 
    		ReservationService reservationService, 
    		ResourceService resourceService) {
    	this.accountService = accountService;
    	this.productService = productService;
    	this.reservationService = reservationService;
    	this.resourceService = resourceService;
    }
    
    public Result process(
    		long startMilli, 
    		boolean processDelayed,
    		boolean isCostAndUsageReport,
    		LineItem lineItem,
    		CostAndUsageData costAndUsageData,
    		InstancePrices ec2Prices,
    		Map<String, Double> ondemandRate, 
    		Instances instances) {
    	
    	// Cost and Usage report-specific checks
    	if (lineItem.getLineItemType() == LineItemType.Tax)
    		return Result.ignore;
    	
        if (StringUtils.isEmpty(lineItem.getAccountId()) ||
            StringUtils.isEmpty(lineItem.getProduct()) ||
            StringUtils.isEmpty(lineItem.getCost()))
            return Result.ignore;

        Account account = accountService.getAccountById(lineItem.getAccountId());
        if (account == null)
            return Result.ignore;

        Product product = productService.getProductByAwsName(lineItem.getProduct());
        
        if (product.isSupport()) {
        	// Don't try and deal with support. Line items have lots of craziness
        	return Result.ignore;
        }
        
    	if (StringUtils.isEmpty(lineItem.getUsageType()) ||
            StringUtils.isEmpty(lineItem.getOperation()) ||
            StringUtils.isEmpty(lineItem.getUsageQuantity())) {
    		return Result.ignore;
    	}
    	double usageValue = Double.parseDouble(lineItem.getUsageQuantity());
        double costValue = Double.parseDouble(lineItem.getCost());

        long millisStart = lineItem.getStartMillis();
        long millisEnd = lineItem.getEndMillis();

        boolean reservationUsage = lineItem.isReserved();
        final String description = lineItem.getDescription();
        		
        ReformedMetaData reformedMetaData = reform(reservationService.getDefaultReservationUtilization(millisStart), 
        		product, reservationUsage, lineItem.getOperation(), lineItem.getUsageType(), description, costValue,
        		lineItem.getPurchaseOption(), lineItem.getPricingUnit());
        product = reformedMetaData.product;
        Operation operation = reformedMetaData.operation;
        final UsageType usageType = reformedMetaData.usageType;
        Zone zone = Zone.getZone(lineItem.getZone(), reformedMetaData.region);

        int startIndex = (int)((millisStart - startMilli)/ AwsUtils.hourMillis);
        int endIndex = (int)((millisEnd + 1000 - startMilli)/ AwsUtils.hourMillis);

        Result result = Result.hourly;
        if (product.isEc2Instance()) {
            result = processEc2Instance(processDelayed, reservationUsage, operation, zone);
            if (instances != null && lineItem.hasResources())
            	instances.add(lineItem.getResource(), usageType.toString(), lineItem.getResourceTags(), account, reformedMetaData.region, zone);
        }
        else if (product.isRedshift()) {
            result = processRedshift(processDelayed, reservationUsage, operation, costValue);
            //logger.info("Process Redshift " + operation + " " + costValue + " returned: " + result);
        }
        else if (product.isDataTransfer()) {
            result = processDataTranfer(processDelayed, usageType);
        }
        else if (product.isCloudHsm()) {
            result = processCloudhsm(processDelayed, usageType);
        }
        else if (product.isEbs()) {
            result = processEbs(usageType);
        }
        else if (product.isRds() || product.isRdsInstance()) {
            result = processRds(usageType, processDelayed, reservationUsage, operation, costValue);
//            if (startIndex == 0 && reservationUsage) {
//            	logger.info(" ----- RDS usage=" + usageType + ", delayed=" + processDelayed + ", operation=" + operation + ", cost=" + costValue + ", result=" + result);
//            }
        }

        if (result == Result.ignore || result == Result.delay)
            return result;

        if (usageType.name.startsWith("TimedStorage-ByteHrs"))
            result = Result.daily;

        boolean monthlyCost = StringUtils.isEmpty(description) ? false : description.toLowerCase().contains("-month");

        ReadWriteData usageData = costAndUsageData.getUsage(null);
        ReadWriteData costData = costAndUsageData.getCost(null);
        ReadWriteData usageDataOfProduct = costAndUsageData.getUsage(product);
        ReadWriteData costDataOfProduct = costAndUsageData.getCost(product);

        if (result == Result.daily) {
            millisStart = new DateTime(millisStart, DateTimeZone.UTC).withTimeAtStartOfDay().getMillis();
            startIndex = (int)((millisStart - startMilli)/ AwsUtils.hourMillis);
            endIndex = startIndex + 24;
        }
        else if (result == Result.monthly) {
            startIndex = 0;
            endIndex = usageData.getNum();
            int numHoursInMonth = new DateTime(startMilli, DateTimeZone.UTC).dayOfMonth().getMaximumValue() * 24;
            usageValue = usageValue * endIndex / numHoursInMonth;
            costValue = costValue * endIndex / numHoursInMonth;
        }

        if (monthlyCost) {
            int numHoursInMonth = new DateTime(startMilli, DateTimeZone.UTC).dayOfMonth().getMaximumValue() * 24;
            usageValue = usageValue * numHoursInMonth;
        }

        TagGroup tagGroup = null;
        TagGroup resourceTagGroup = null;
        String reservationId = lineItem.getReservationId();
        if (operation instanceof Operation.ReservationOperation && !reservationId.isEmpty()) {
        	tagGroup = TagGroupRI.getTagGroup(account, reformedMetaData.region, zone, product, operation, usageType, null, reservationId);
        }
        else {
        	tagGroup = TagGroup.getTagGroup(account, reformedMetaData.region, zone, product, operation, usageType, null);
        }

        int[] indexes;
        if (endIndex - startIndex > 1) {
            usageValue = usageValue / (endIndex - startIndex);
            costValue = costValue / (endIndex - startIndex);
            indexes = new int[endIndex - startIndex];
            for (int i = 0; i < indexes.length; i++)
                indexes[i] = startIndex + i;
        }
        else {
            indexes = new int[]{startIndex};
        }

        if (costValue > 0 && !reservationUsage && product.isEc2Instance() && tagGroup.operation == Operation.ondemandInstances) {
            String key = operation + "|" + tagGroup.region + "|" + usageType;
            ondemandRate.put(key, costValue/usageValue);
        }

        if (lineItem.hasResources() && !lineItem.getResource().isEmpty() && resourceService != null) {
            String resourceGroupStr = resourceService.getResource(account, reformedMetaData.region, product, lineItem, millisStart);
            if (!StringUtils.isEmpty(resourceGroupStr)) {
                ResourceGroup resourceGroup = ResourceGroup.getResourceGroup(resourceGroupStr);
                if (tagGroup instanceof TagGroupRI) {
                	resourceTagGroup = TagGroupRI.getTagGroup(account, reformedMetaData.region, zone, product, operation, usageType, resourceGroup, reservationId);
                }
                else {
                	resourceTagGroup = TagGroup.getTagGroup(account, reformedMetaData.region, zone, product, operation, usageType, resourceGroup);
                }

                if (usageDataOfProduct == null) {
                    usageDataOfProduct = new ReadWriteData();
                    costDataOfProduct = new ReadWriteData();
                    costAndUsageData.putUsage(product, usageDataOfProduct);
                    costAndUsageData.putCost(product, costDataOfProduct);
                }
            }
        }

        for (int i : indexes) {
            if (!product.isMonitor()) {
                Map<TagGroup, Double> usages = usageData.getData(i);
                Map<TagGroup, Double> costs = costData.getData(i);
                //
                // For DBR reports, Redshift and RDS have cost as a monthly charge, but usage appears hourly.
                //		EC2 has cost reported in each usage lineitem.
                // For CAU reports, EC2, Redshift, and RDS have cost as a monthly charge, but usage appears hourly.
                // 	so unlike EC2, we have to process the monthly line item to capture the cost,
                // 	but we don't want to add the monthly line items to the usage.
                // The reservation processor handles determination on what's unused.
                if (result != Result.monthly || !(product.isRedshift() || product.isRdsInstance() || (product.isEc2Instance() && isCostAndUsageReport))) {
                	addValue(usages, tagGroup, usageValue,  true);                	
                }

                addValue(costs, tagGroup, costValue, true);
            }

            if (resourceTagGroup != null) {
                Map<TagGroup, Double> usagesOfResource = usageDataOfProduct.getData(i);
                Map<TagGroup, Double> costsOfResource = costDataOfProduct.getData(i);

                if (!((product.isRedshift() || product.isRds()) && result == Result.monthly)) {
                	addValue(usagesOfResource, resourceTagGroup, usageValue, !product.isMonitor());
                }
                
                addValue(costsOfResource, resourceTagGroup, costValue, !product.isMonitor());
            }
        }

        return result;
    }

    private void addValue(Map<TagGroup, Double> map, TagGroup tagGroup, double value, boolean add) {
        Double oldV = map.get(tagGroup);
        if (oldV != null) {
            value = add ? value + oldV : value;
        }

        map.put(tagGroup, value);
    }

    private Result processEc2Instance(boolean processDelayed, boolean reservationUsage, Operation operation, Zone zone) {
        if (!processDelayed && zone == null && operation.isBonus() && reservationUsage)
            return Result.ignore; // Ignore monthly cost on no upfront and partial upfront reservations
        else
            return Result.hourly;
    }

    private Result processRedshift(boolean processDelayed, boolean reservationUsage, Operation operation, double costValue) {
        if (!processDelayed && costValue != 0 && operation.isBonus() && reservationUsage)
            return Result.delay;
        else if (!processDelayed && costValue == 0 && operation.isBonus() && reservationUsage)
            return Result.hourly;
        else if (processDelayed && costValue != 0 && operation.isBonus() && reservationUsage)
            return Result.monthly;
        else
            return Result.hourly;
    }

    private Result processDataTranfer(boolean processDelayed, UsageType usageType) {
        if (!processDelayed && usageType.name.contains("PrevMon-DataXfer-"))
            return Result.delay;
        else if (processDelayed && usageType.name.contains("PrevMon-DataXfer-"))
            return Result.monthly;
        else
            return Result.hourly;
    }

    private Result processCloudhsm(boolean processDelayed, UsageType usageType) {
        if (!processDelayed && usageType.name.contains("CloudHSMUpfront"))
            return Result.delay;
        else if (processDelayed && usageType.name.contains("CloudHSMUpfront"))
            return Result.monthly;
        else
            return Result.hourly;
    }

    private Result processEbs(UsageType usageType) {
        if (usageType.name.startsWith("EBS:SnapshotUsage"))
            return Result.daily;
        else
            return Result.hourly;
    }

    private Result processRds(UsageType usageType, boolean processDelayed, boolean reservationUsage, Operation operation, double costValue) {
        if (usageType.name.startsWith("RDS:ChargedBackupUsage"))
            return Result.daily;
        else if (!processDelayed && costValue != 0 && operation.isBonus() && reservationUsage)
            return Result.delay; // Must be a monthly charge for all the hourly usage
        else if (!processDelayed && costValue == 0 && operation.isBonus() && reservationUsage)
            return Result.hourly; // Must be the hourly usage
        else if (processDelayed && costValue != 0 && operation.isBonus() && reservationUsage)
            return Result.monthly; // This is the post processing of the monthly charge for all the hourly usage
        else
            return Result.hourly;
    }

    protected ReformedMetaData reform(
    		ReservationUtilization defaultReservationUtilization,
    		Product product, 
    		boolean reservationUsage, 
    		String operationStr, 
    		String usageTypeStr, 
    		String description, 
    		double cost,
    		String purchaseOption,
    		String pricingUnit) {

        Operation operation = null;
        UsageType usageType = null;
        InstanceOs os = null;
        InstanceDb db = null;

        // first try to retrieve region info
        int index = usageTypeStr.indexOf("-");
        String regionShortName = index > 0 ? usageTypeStr.substring(0, index) : "";
        Region region = regionShortName.isEmpty() ? null : Region.getRegionByShortName(regionShortName);
        if (region != null) {
            usageTypeStr = usageTypeStr.substring(index+1);
        }
        else {
            region = Region.US_EAST_1;
        }

        if (operationStr.equals("EBS Snapshot Copy")) {
            product = productService.getProductByName(Product.ebs);
        }

        if (usageTypeStr.startsWith("ElasticIP:")) {
            product = productService.getProductByName(Product.eip);
        }
        else if (usageTypeStr.startsWith("EBS:"))
            product = productService.getProductByName(Product.ebs);
        else if (usageTypeStr.startsWith("EBSOptimized:"))
            product = productService.getProductByName(Product.ebs);
        else if (usageTypeStr.startsWith("CW:"))
            product = productService.getProductByName(Product.cloudWatch);
        else if ((usageTypeStr.startsWith("BoxUsage") || usageTypeStr.startsWith("SpotUsage")) && operationStr.startsWith("RunInstances")) {
        	// Line item for hourly "All Upfront", "Spot", or "On-Demand" EC2 instance usage
        	boolean spot = usageTypeStr.startsWith("SpotUsage");
            index = usageTypeStr.indexOf(":");
            usageTypeStr = index < 0 ? "m1.small" : usageTypeStr.substring(index+1);

            if (reservationUsage && product.isEc2()) {
            	if (purchaseOption != null) {
            		operation = Operation.getBonusReservedInstances(ReservationUtilization.get(purchaseOption));
            	}
            	else if (cost == 0)
                    operation = Operation.bonusReservedInstancesFixed;
            	else
                    operation = Operation.getBonusReservedInstances(defaultReservationUtilization);
            }
            else if (spot)
            	operation = Operation.spotInstances;
            else
                operation = Operation.ondemandInstances;
            os = getInstanceOs(operationStr);
        }
        else if (usageTypeStr.startsWith("Node") && operationStr.startsWith("RunComputeNode")) {
        	// Line item for hourly Redshift instance usage both On-Demand and Reserved.
            usageTypeStr = currentRedshiftUsageType(usageTypeStr.split(":")[1]);
            
            if (reservationUsage) {
            	if (purchaseOption != null) {
            		operation = Operation.getBonusReservedInstances(ReservationUtilization.get(purchaseOption));
            	}
            	else {
		        	// Fixed, Heavy, and HeavyPartial apply cost monthly,
		        	// so can't tell them apart without looking at the description. Examples:
		        	// No Upfront:  "Redshift, dw2.8xlarge instance-hours used this month"
		        	// All Upfront: "USD 0.0 per Redshift, dw2.8xlarge instance-hour (or partial hour)"
		            if (cost == 0 && description.contains(" 0.0 per"))
		            	operation = Operation.bonusReservedInstancesFixed;
		            else
		                operation = Operation.getBonusReservedInstances(defaultReservationUtilization);
            	}
            }
            else {
	            operation = Operation.ondemandInstances;
            }
            os = getInstanceOs(operationStr);
        }
        else if ((usageTypeStr.startsWith("InstanceUsage") || usageTypeStr.startsWith("Multi-AZUsage")) && operationStr.startsWith("CreateDBInstance")) {
        	// Line item for hourly RDS instance usage - both On-Demand and Reserved
            usageTypeStr = usageTypeStr.split(":")[1] + (usageTypeStr.startsWith("Multi") ? ".multiaz" : "");
            
            if (reservationUsage) {
            	if (purchaseOption != null) {
            		operation = Operation.getBonusReservedInstances(ReservationUtilization.get(purchaseOption));
            	}
            	else {
		        	// Fixed, Heavy, and HeavyPartial apply cost monthly,
		        	// so can't tell them apart without looking at the description. Examples:
		            // --- Need examples, for now assuming it's the same as for Redshift ---
		        	// No Upfront:  "Redshift, dw2.8xlarge instance-hours used this month"
		        	// All Upfront: "USD 0.0 per Redshift, dw2.8xlarge instance-hour (or partial hour)"
		            if (cost == 0 && description.contains(" 0.0 per"))
		            	operation = Operation.bonusReservedInstancesFixed;
		            else if (reservationUsage)
		                operation = Operation.getBonusReservedInstances(defaultReservationUtilization);
            	}
            }
            else {
            	operation = Operation.ondemandInstances;
            }
            db = getInstanceDb(operationStr);
        }
        else if (usageTypeStr.startsWith("HeavyUsage")) {
        	// If DBR report: Line item for hourly "No Upfront" or "Partial Upfront" EC2 or monthly "No Upfront" or "Partial Upfront" for Redshift and RDS
        	// If Cost and Usage report: monthly "No Upfront" or "Partial Upfront" for EC2, RDS, and Redshift
            index = usageTypeStr.indexOf(":");
            if (index < 0) {
                usageTypeStr = "m1.small";
            }
            else {
                usageTypeStr = usageTypeStr.substring(index+1);
                if (product.isRedshift()) {
                	usageTypeStr = currentRedshiftUsageType(usageTypeStr);
                }
            }
            if (product.isRds()){
                db = getInstanceDb(operationStr);
            }
            if (StringUtils.isNotEmpty(purchaseOption)) {
	            operation = getOperation(operationStr, reservationUsage, ReservationUtilization.get(purchaseOption));
            }
            else {
	            // No way to tell what purchase option this is for without referencing reservation.
	            // TODO look up reservation from ARN if this is Cost and Usage.
	            operation = getOperation(operationStr, reservationUsage, defaultReservationUtilization);
            }
            os = getInstanceOs(operationStr);
        }
        
        // Re-map all Data Transfer costs except API Gateway and CouldFront to Data Transfer (same as the AWS Billing Page Breakout)
        if (!product.isCloudFront() && !product.isApiGateway()) {
        	if (usageTypeStr.equals("DataTransfer-Regional-Bytes") || usageTypeStr.endsWith("-In-Bytes") || usageTypeStr.endsWith("-Out-Bytes"))
        		product = productService.getProductByName(Product.dataTransfer);
        }

        // Usage type string is empty for Support recurring fees.
        if (usageTypeStr.equals("Unknown") || usageTypeStr.equals("Not Applicable") || usageTypeStr.isEmpty()) {
            usageTypeStr = product.name;
        }

        if (operation == null) {
            operation = Operation.getOperation(operationStr);
        }

        if (product.isEc2() && operation instanceof Operation.ReservationOperation) {
            product = productService.getProductByName(Product.ec2Instance);
            if (os != InstanceOs.linux && os != InstanceOs.spot) {
                usageTypeStr = usageTypeStr + "." + os;
                operation = operation.isBonus() ? operation : Operation.ondemandInstances;
            }
        }

        if (product.isRds() && operation instanceof Operation.ReservationOperation) {
            product = productService.getProductByName(Product.rdsInstance);
            usageTypeStr = usageTypeStr + "." + db;
            operation = operation.isBonus() ? operation : Operation.ondemandInstances;
        }

        if (usageType == null) {
        	String unit = (operation instanceof Operation.ReservationOperation) ? "hours" : pricingUnit; 
            usageType = UsageType.getUsageType(usageTypeStr, unit);
//            if (StringUtils.isEmpty(usageType.unit)) {
//            	logger.info("No units for " + usageTypeStr + ", " + operation + ", " + description + ", " + product);
//            }
        }

        return new ReformedMetaData(region, product, operation, usageType);
    }

    private InstanceOs getInstanceOs(String operationStr) {
        int index = operationStr.indexOf(":");
        String osStr = index > 0 ? operationStr.substring(index) : "";
        return InstanceOs.withCode(osStr);
    }

    private InstanceDb getInstanceDb(String operationStr) {
        int index = operationStr.indexOf(":");
        String osStr = index > 0 ? operationStr.substring(index) : "";
        return InstanceDb.withCode(osStr);
    }

    private Operation getOperation(String operationStr, boolean reservationUsage, ReservationUtilization utilization) {
        if (operationStr.startsWith("RunInstances") ||
        		operationStr.startsWith("RunComputeNode") ||
        		operationStr.startsWith("CreateDBInstance")) {
            return (reservationUsage ? Operation.getBonusReservedInstances(utilization) : Operation.ondemandInstances);
        }
        return null;
    }
    
    private static final Map<String, String> redshiftUsageTypeMap = Maps.newHashMap();
    static {
    	redshiftUsageTypeMap.put("dw.hs1.xlarge", "ds1.xlarge");
    	redshiftUsageTypeMap.put("dw1.xlarge", "ds1.xlarge");
    	redshiftUsageTypeMap.put("dw.hs1.8xlarge", "ds1.8xlarge");
    	redshiftUsageTypeMap.put("dw1.8xlarge", "ds1.8xlarge");
    	redshiftUsageTypeMap.put("dw2.large", "dc1.large");
    	redshiftUsageTypeMap.put("dw2.8xlarge", "dc1.8xlarge");
    }

    private String currentRedshiftUsageType(String usageType) {
    	if (redshiftUsageTypeMap.containsKey(usageType))
    		return redshiftUsageTypeMap.get(usageType);
    	return usageType;
    }
    
    protected static class ReformedMetaData{
        public final Region region;
        public final Product product;
        public final Operation operation;
        public final UsageType usageType;
        public ReformedMetaData(Region region, Product product, Operation operation, UsageType usageType) {
            this.region = region;
            this.product = product;
            this.operation = operation;
            this.usageType = usageType;
        }
        
        public String toString() {
            return "\"" + region + "\",\"" + product + "\",\"" + operation + "\",\"" + usageType + "\"";

        }
    }
    
}
