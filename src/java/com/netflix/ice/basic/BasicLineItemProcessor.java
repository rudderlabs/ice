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
import com.netflix.ice.processor.ReservationService.ReservationInfo;
import com.netflix.ice.processor.ReservationService.ReservationUtilization;
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
    
    protected AccountService accountService;
    protected ProductService productService;
    protected ReservationService reservationService;

    protected ResourceService resourceService;
    
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
    
    protected boolean ignore(long startMilli, LineItem lineItem) {    	
        if (StringUtils.isEmpty(lineItem.getAccountId()) ||
            StringUtils.isEmpty(lineItem.getProduct()) ||
            StringUtils.isEmpty(lineItem.getCost()))
            return true;

        Account account = accountService.getAccountById(lineItem.getAccountId());
        if (account == null)
            return true;

        Product product = productService.getProductByAwsName(lineItem.getProduct());
        
        if (product.isSupport()) {
        	// Don't try and deal with support. Line items have lots of craziness
        	return true;
        }
        
    	if (StringUtils.isEmpty(lineItem.getUsageType()) ||
            StringUtils.isEmpty(lineItem.getOperation()) ||
            StringUtils.isEmpty(lineItem.getUsageQuantity())) {
    		return true;
    	}

    	if (!product.isRegistrar()) {
    		// Registrar product renewals occur before they expire, so often start in the following month.
    		// We handle the out-of-date-range problem later.
    		// All other cases are ignored here.
	        long nextMonthStartMillis = new DateTime(startMilli).plusMonths(1).getMillis();
	        if (lineItem.getStartMillis() >= nextMonthStartMillis) {
	        	logger.error("line item starts in a later month. Line item type = " + lineItem.getLineItemType() + ", product = " + lineItem.getProduct() + ", cost = " + lineItem.getCost());
	        	return true;
	        }
	        if (lineItem.getEndMillis() > nextMonthStartMillis) {
	        	logger.error("line item ends in a later month. Line item type = " + lineItem.getLineItemType() + ", product = " + lineItem.getProduct() + ", cost = " + lineItem.getCost());
	        	return true;
	        }
    	}
        
    	return false;
    }
    
    protected String getUsageTypeStr(String usageTypeStr) {
        int index = usageTypeStr.indexOf("-");
        String regionShortName = index > 0 ? usageTypeStr.substring(0, index) : "";
        Region region = regionShortName.isEmpty() ? null : Region.getRegionByShortName(regionShortName);
        return region == null ? usageTypeStr : usageTypeStr.substring(index+1);
    }
    
    protected Region getRegion(LineItem lineItem) {
    	String usageTypeStr = lineItem.getUsageType();
        int index = usageTypeStr.indexOf("-");
        String regionShortName = index > 0 ? usageTypeStr.substring(0, index) : "";
        Region region = regionShortName.isEmpty() ? null : Region.getRegionByShortName(regionShortName);
        return region == null ? Region.US_EAST_1 : region;
    }
    
    protected void addResourceInstance(LineItem lineItem, Instances instances, TagGroup tg) {
        // Add all resources to the instance catalog
        if (instances != null && lineItem.hasResources() && !tg.product.isDataTransfer() && !tg.product.isCloudWatch())
        	instances.add(lineItem.getResource(), lineItem.getStartMillis(), tg.usageType.toString(), lineItem.getResourceTags(), tg.account, tg.region, tg.zone, tg.product);
    }
    
    protected TagGroup getTagGroup(LineItem lineItem, Account account, Region region, Zone zone, Product product, Operation operation, UsageType usageType, ResourceGroup rg) {
        return TagGroup.getTagGroup(account, region, zone, product, operation, usageType, rg);
    }
    
    public Result process(
    		long startMilli, 
    		boolean processDelayed,
    		boolean isCostAndUsageReport,
    		LineItem lineItem,
    		CostAndUsageData costAndUsageData,
    		Instances instances,
    		double edpDiscount) {
    	
    	if (ignore(startMilli, lineItem))
    		return Result.ignore;
    	

        Account account = accountService.getAccountById(lineItem.getAccountId());
        Region region = getRegion(lineItem);
        Zone zone = Zone.getZone(lineItem.getZone(), region);
        
    	double usageValue = Double.parseDouble(lineItem.getUsageQuantity());
        double costValue = Double.parseDouble(lineItem.getCost());

        long millisStart = lineItem.getStartMillis();
        long millisEnd = lineItem.getEndMillis();
        
        if (productService.getProductByAwsName(lineItem.getProduct()).isRegistrar()) {
        	// Put all out-of-month registrar fees at the start of the month
	        long nextMonthStartMillis = new DateTime(startMilli).plusMonths(1).getMillis();
        	if (millisStart > nextMonthStartMillis) {
        		millisStart = startMilli;
        	}
        	// Put the whole fee in the first hour
        	millisEnd = new DateTime(millisStart).plusHours(1).getMillis();
        }
        
        boolean reservationUsage = lineItem.isReserved();
        final String description = lineItem.getDescription();
        
        ReservationUtilization utilization = reservationService.getDefaultReservationUtilization(millisStart);
        String purchaseOption = lineItem.getPurchaseOption();
        String reservationId = lineItem.getReservationId();
        if (StringUtils.isEmpty(purchaseOption) && StringUtils.isNotEmpty(reservationId)) {
        	ReservationInfo resInfo = reservationService.getReservation(reservationId);
        	if (resInfo != null)
        		utilization = ((Operation.ReservationOperation) resInfo.tagGroup.operation).getUtilization();
        }
        		
        ReformedMetaData reformedMetaData = reform(utilization, 
        		productService.getProductByAwsName(lineItem.getProduct()), reservationUsage, lineItem.getOperation(), lineItem.getUsageType(), description, costValue,
        		purchaseOption, lineItem.getPricingUnit());
        final Product product = reformedMetaData.product;
        final Operation operation = reformedMetaData.operation;
        final UsageType usageType = reformedMetaData.usageType;
        
        final TagGroup tagGroup = getTagGroup(lineItem, account, region, zone, product, operation, usageType, null);

        int startIndex = (int)((millisStart - startMilli)/ AwsUtils.hourMillis);
        int endIndex = (int)((millisEnd + 1000 - startMilli)/ AwsUtils.hourMillis);

        // Add all resources to the instance catalog
        addResourceInstance(lineItem, instances, tagGroup);

        final Result result = getResult(lineItem, startMilli, tagGroup, processDelayed, reservationUsage, costValue);
                
        // If processing an RIFee from a CUR, grab the unused rates for the reservation processor.
        if (processDelayed && lineItem.getLineItemType() == LineItemType.RIFee) {
        	TagGroupRI tgri = TagGroupRI.getTagGroup(account, region, zone, product, operation, usageType, null, reservationId);
        	addUnusedReservationRate(lineItem, costAndUsageData, tgri, startMilli);
        }

        if (result == Result.ignore || result == Result.delay)
            return result;

        boolean monthlyCost = StringUtils.isEmpty(description) ? false : description.toLowerCase().contains("-month");

        if (result == Result.daily) {
            millisStart = new DateTime(millisStart, DateTimeZone.UTC).withTimeAtStartOfDay().getMillis();
            startIndex = (int)((millisStart - startMilli)/ AwsUtils.hourMillis);
            endIndex = startIndex + 24;
        }
        else if (result == Result.monthly) {
            startIndex = 0;
            endIndex = costAndUsageData.getUsage(null).getNum();
            int numHoursInMonth = new DateTime(startMilli, DateTimeZone.UTC).dayOfMonth().getMaximumValue() * 24;
            usageValue = usageValue * endIndex / numHoursInMonth;
            costValue = costValue * endIndex / numHoursInMonth;
        }

        if (monthlyCost) {
            int numHoursInMonth = new DateTime(startMilli, DateTimeZone.UTC).dayOfMonth().getMaximumValue() * 24;
            usageValue = usageValue * numHoursInMonth;
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

        TagGroup resourceTagGroup = null;
        if (resourceService != null) {
            ResourceGroup resourceGroup = resourceService.getResourceGroup(account, region, product, lineItem, millisStart);
            resourceTagGroup = getTagGroup(lineItem, account, region, zone, product, operation, usageType, resourceGroup);
        }
        
        addData(lineItem, tagGroup, resourceTagGroup, costAndUsageData, usageValue, costValue, result == Result.monthly, indexes, edpDiscount);
        return result;
    }

    protected void addData(LineItem lineItem, TagGroup tagGroup, TagGroup resourceTagGroup,
    		CostAndUsageData costAndUsageData, double usageValue, double costValue, boolean monthly, int[] indexes, double edpDiscount) {
        final ReadWriteData usageData = costAndUsageData.getUsage(null);
        final ReadWriteData costData = costAndUsageData.getCost(null);
        ReadWriteData usageDataOfProduct = costAndUsageData.getUsage(tagGroup.product);
        ReadWriteData costDataOfProduct = costAndUsageData.getCost(tagGroup.product);
        final Product product = tagGroup.product;

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
            // For DBR reports, Redshift and RDS have cost as a monthly charge, but usage appears hourly.
            //		EC2 has cost reported in each usage lineitem.
            // The reservation processor handles determination on what's unused.
            if (!monthly || !(product.isRedshift() || product.isRdsInstance())) {
            	addValue(usages, tagGroup, usageValue);                	
            }

            addValue(costs, tagGroup, costValue);
            
            if (resourceService != null) {
	            if (resourceTagGroup != null) {
	                Map<TagGroup, Double> usagesOfResource = usageDataOfProduct.getData(i);
	                Map<TagGroup, Double> costsOfResource = costDataOfProduct.getData(i);
	
	                if (!((product.isRedshift() || product.isRds()) && monthly)) {
	                	addValue(usagesOfResource, resourceTagGroup, usageValue);
	                }
	                
	                addValue(costsOfResource, resourceTagGroup, costValue);
	                
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

    protected void addValue(Map<TagGroup, Double> map, TagGroup tagGroup, double value) {
        Double oldV = map.get(tagGroup);
        if (oldV != null) {
            value += oldV;
        }

        map.put(tagGroup, value);
    }
    
    protected void addUnusedReservationRate(LineItem lineItem,
    		CostAndUsageData costAndUsageData,
    		TagGroupRI tg, long startMilli) {    	
        return;        
    }

    protected Result getResult(LineItem lineItem, long startMilli, TagGroup tg, boolean processDelayed, boolean reservationUsage, double costValue) {
        Result result = Result.hourly;
        if (tg.product.isEc2Instance()) {
            result = processEc2Instance(processDelayed, reservationUsage, tg.operation, tg.zone);
        }
        else if (tg.product.isRedshift()) {
            result = processRedshift(processDelayed, reservationUsage, tg.operation, costValue);
            //logger.info("Process Redshift " + operation + " " + costValue + " returned: " + result);
        }
        else if (tg.product.isDataTransfer()) {
            result = processDataTranfer(processDelayed, tg.usageType);
        }
        else if (tg.product.isCloudHsm()) {
            result = processCloudhsm(processDelayed, tg.usageType);
        }
        else if (tg.product.isEbs()) {
            result = processEbs(tg.usageType);
        }
        else if (tg.product.isRds() || tg.product.isRdsInstance()) {
            result = processRds(tg.usageType, processDelayed, reservationUsage, tg.operation, costValue);
//            if (startIndex == 0 && reservationUsage) {
//            	logger.info(" ----- RDS usage=" + usageType + ", delayed=" + processDelayed + ", operation=" + operation + ", cost=" + costValue + ", result=" + result);
//            }
        }
        
        if (tg.usageType.name.startsWith("TimedStorage-ByteHrs"))
            result = Result.daily;

        return result;
    }
    
    private Result processEc2Instance(boolean processDelayed, boolean reservationUsage, Operation operation, Zone zone) {
        if (!processDelayed && zone == null && operation.isBonus() && reservationUsage)
            return Result.delay; // Delay monthly cost on no upfront and partial upfront reservations
        else if (processDelayed && zone == null && operation.isBonus() && reservationUsage)
            return Result.ignore; // Ignore monthly cost on no upfront and partial upfront reservations - We'll process the unused rates now if CUR
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

    protected Result processDataTranfer(boolean processDelayed, UsageType usageType) {
        if (!processDelayed && usageType.name.contains("PrevMon-DataXfer-"))
            return Result.delay;
        else if (processDelayed && usageType.name.contains("PrevMon-DataXfer-"))
            return Result.monthly;
        else
            return Result.hourly;
    }

    protected Result processCloudhsm(boolean processDelayed, UsageType usageType) {
        if (!processDelayed && usageType.name.contains("CloudHSMUpfront"))
            return Result.delay;
        else if (processDelayed && usageType.name.contains("CloudHSMUpfront"))
            return Result.monthly;
        else
            return Result.hourly;
    }

    protected Result processEbs(UsageType usageType) {
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

        usageTypeStr = getUsageTypeStr(usageTypeStr);

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
            int index = usageTypeStr.indexOf(":");
            usageTypeStr = index < 0 ? "m1.small" : usageTypeStr.substring(index+1);

            if (reservationUsage && product.isEc2()) {
            	if (StringUtils.isNotEmpty(purchaseOption)) {
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
            	if (StringUtils.isNotEmpty(purchaseOption)) {
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
        	boolean multiaz = usageTypeStr.startsWith("Multi");
            usageTypeStr = usageTypeStr.split(":")[1];
            
            // db.m4.10xlarge usage in eu-west-1 is in the reports as "db.m4.10xl", so need to fix it.
            // There may be others, so just look to see if the usage type string ends with "xl" and add "arge"
            if (usageTypeStr.endsWith("xl")) {
            	usageTypeStr = usageTypeStr + "arge";
            }
            
            if (multiaz) {
            	usageTypeStr += ".multiaz";
            }
            
            if (reservationUsage) {
            	if (StringUtils.isNotEmpty(purchaseOption)) {
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
            int index = usageTypeStr.indexOf(":");
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
	            // No way to tell what purchase option this is
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
            usageTypeStr = usageTypeStr + os.usageType;
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

        return new ReformedMetaData(product, operation, usageType);
    }

    protected InstanceOs getInstanceOs(String operationStr) {
        int index = operationStr.indexOf(":");
        String osStr = index > 0 ? operationStr.substring(index) : "";
        return InstanceOs.withCode(osStr);
    }

    protected InstanceDb getInstanceDb(String operationStr) {
        int index = operationStr.indexOf(":");
        String osStr = index > 0 ? operationStr.substring(index) : "";
        return InstanceDb.withCode(osStr);
    }

    protected Operation getOperation(String operationStr, boolean reservationUsage, ReservationUtilization utilization) {
        if (operationStr.startsWith("RunInstances") ||
        		operationStr.startsWith("RunComputeNode") ||
        		operationStr.startsWith("CreateDBInstance")) {
            return (reservationUsage ? Operation.getBonusReservedInstances(utilization) : Operation.ondemandInstances);
        }
        return null;
    }
    
    protected static final Map<String, String> redshiftUsageTypeMap = Maps.newHashMap();
    static {
    	redshiftUsageTypeMap.put("dw.hs1.xlarge", "ds1.xlarge");
    	redshiftUsageTypeMap.put("dw1.xlarge", "ds1.xlarge");
    	redshiftUsageTypeMap.put("dw.hs1.8xlarge", "ds1.8xlarge");
    	redshiftUsageTypeMap.put("dw1.8xlarge", "ds1.8xlarge");
    	redshiftUsageTypeMap.put("dw2.large", "dc1.large");
    	redshiftUsageTypeMap.put("dw2.8xlarge", "dc1.8xlarge");
    }

    protected String currentRedshiftUsageType(String usageType) {
    	if (redshiftUsageTypeMap.containsKey(usageType))
    		return redshiftUsageTypeMap.get(usageType);
    	return usageType;
    }
    
    protected static class ReformedMetaData{
        public final Product product;
        public final Operation operation;
        public final UsageType usageType;
        public ReformedMetaData(Product product, Operation operation, UsageType usageType) {
            this.product = product;
            this.operation = operation;
            this.usageType = usageType;
        }
        
        public String toString() {
            return "\"" + product + "\",\"" + operation + "\",\"" + usageType + "\"";

        }
    }
    
}
