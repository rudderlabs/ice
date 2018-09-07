package com.netflix.ice.processor;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.netflix.ice.common.LineItem;

public class CostAndUsageReportLineItem extends LineItem {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    private int lineItemIdIndex;
    private int billTypeIndex;
    private final int resourceTagStartIndex;
	private final String[] resourceTagsHeader;
	private int purchaseOptionIndex;
	private int lineItemTypeIndex;
	private int lineItemNormalizationFactorIndex;
	private int productNormalizationSizeFactorIndex;
	private int productUsageTypeIndex;
	private int publicOnDemandCostIndex;
	private LineItemType lineItemType;
	private int pricingUnitIndex;
	private int reservationArnIndex;
	
	private static Map<String, Double> normalizationFactors = Maps.newHashMap();
	
	{
		normalizationFactors.put("nano", 0.25);
		normalizationFactors.put("micro", 0.5);
		normalizationFactors.put("small", 1.0);
		normalizationFactors.put("medium", 2.0);
		normalizationFactors.put("large", 4.0);
		normalizationFactors.put("xlarge", 8.0);
	}
	    	
    public CostAndUsageReportLineItem(boolean useBlended, CostAndUsageReport report) {
    	lineItemIdIndex = report.getColumnIndex("identity",  "LineItemId");
    	billTypeIndex = report.getColumnIndex("bill", "BillType");
        accountIdIndex = report.getColumnIndex("lineItem", "UsageAccountId");
        productIndex = report.getColumnIndex("product", "ProductName");
        zoneIndex = report.getColumnIndex("lineItem", "AvailabilityZone");
        descriptionIndex = report.getColumnIndex("lineItem", "LineItemDescription");
        usageTypeIndex = report.getColumnIndex("lineItem", "UsageType");
        operationIndex = report.getColumnIndex("lineItem", "Operation");
        usageQuantityIndex = report.getColumnIndex("lineItem", "UsageAmount");
        startTimeIndex = report.getColumnIndex("lineItem", "UsageStartDate");
        endTimeIndex = report.getColumnIndex("lineItem", "UsageEndDate");
        rateIndex = report.getColumnIndex("lineItem", useBlended ? "BlendedRate" : "UnblendedRate");
        costIndex = report.getColumnIndex("lineItem", useBlended ? "BlendedCost" : "UnblendedCost");
        resourceIndex = report.getColumnIndex("lineItem", "ResourceId");
        reservedIndex = report.getColumnIndex("pricing", "term");
        
        resourceTagStartIndex = report.getCategoryStartIndex("resourceTags");
        resourceTagsHeader = report.getCategoryHeader("resourceTags");
        
        purchaseOptionIndex = report.getColumnIndex("pricing", "PurchaseOption");
        lineItemTypeIndex = report.getColumnIndex("lineItem", "LineItemType");
        lineItemNormalizationFactorIndex = report.getColumnIndex("lineItem", "NormalizationFactor");
        productNormalizationSizeFactorIndex = report.getColumnIndex("product", "normalizationSizeFactor"); // First appeared in 07-2017
        productUsageTypeIndex = report.getColumnIndex("product",  "usagetype"); 
        
        publicOnDemandCostIndex = report.getColumnIndex("pricing", "publicOnDemandCost");        
        pricingUnitIndex = report.getColumnIndex("pricing", "unit");
        reservationArnIndex = report.getColumnIndex("reservation", "ReservationARN");
    }
    
    public String toString() {
    	String[] values = new String[]{
    			items[lineItemIdIndex],
    			items[billTypeIndex],
    			items[accountIdIndex],
    			items[zoneIndex],
    			items[productIndex],
    			items[operationIndex],
    			items[usageTypeIndex],
    			items[descriptionIndex],
    			items[usageQuantityIndex],
    			items[startTimeIndex],
    			items[endTimeIndex],
    			items[rateIndex],
    			items[costIndex],
    			items[resourceIndex],
    			items[reservedIndex],
    			items[purchaseOptionIndex],
    			items[lineItemTypeIndex],
    			items[lineItemNormalizationFactorIndex],
    			items[productUsageTypeIndex],
    			items[publicOnDemandCostIndex],
    			items[pricingUnitIndex],
    			items[reservationArnIndex],
    	};
    	return StringUtils.join(values, ",");
    }
    
    @Override
    public void setItems(String[] items) {
    	super.setItems(items);
        lineItemType = null;
        try {
        	lineItemType = LineItemType.valueOf(items[lineItemTypeIndex]);
        } catch (Exception e) {
        }
        if (lineItemType == null)
        	logger.error("Unknown lineItemType: " + items[lineItemTypeIndex]);
    }
    
    public int size() {
    	return resourceTagStartIndex + resourceTagsHeader.length;
    }
        
    public BillType getBillType() {
    	return BillType.valueOf(items[billTypeIndex]);
    }

    @Override
    public long getStartMillis() {
        return amazonBillingDateFormatISO.parseMillis(items[startTimeIndex]);
    }

    @Override
    public long getEndMillis() {
        return amazonBillingDateFormatISO.parseMillis(items[endTimeIndex]);
    }
    
    @Override
    public String getUsageType() {
    	String purchaseOption = getPurchaseOption();
    	if ((lineItemType == LineItemType.RIFee || lineItemType == LineItemType.DiscountedUsage) && (purchaseOption.isEmpty() || !purchaseOption.equals("All Upfront")))
    		return items[productUsageTypeIndex];
    	return items[usageTypeIndex];
    }
    
    @Override
    public String[] getResourceTagsHeader() {
    	String[] header = new String[resourceTagsHeader.length];
    	for (int i = 0; i < resourceTagsHeader.length; i++)
    		header[i] = resourceTagsHeader[i].substring("resourceTags/".length());
    		
    	return header;
    }

    @Override
    public int getResourceTagsSize() {
    	if (items.length - resourceTagStartIndex <= 0)
    		return 0;
    	return items.length - resourceTagStartIndex;
    }

    @Override
    public String getResourceTag(int index) {
    	if (items.length <= resourceTagStartIndex + index)
    		return "";
    	return items[resourceTagStartIndex + index];
    }

    @Override
    public Map<String, String>  getResourceTags() {
    	Map<String, String> tags = Maps.newHashMap();
    	for (int i = 0; i < resourceTagsHeader.length && i+resourceTagStartIndex < items.length; i++) {
    		if (items[i+resourceTagStartIndex].isEmpty()) {
    			continue;
    		}
    		String tag = resourceTagsHeader[i].substring("resourceTags/".length());
    		if (tag.startsWith("user:"))
    			tag = tag.substring("user:".length());
    		tags.put(tag, items[i+resourceTagStartIndex]);
    	}
    	return tags;
    }
    
    @Override
    public boolean isReserved() {
    	if (reservedIndex > items.length) {
    		logger.error("Line item record too short. Reserved index = " + reservedIndex + ", record length = " + items.length);
    		return false;
    	}
    	return items[reservedIndex].equals("Reserved") || items[usageTypeIndex].contains("HeavyUsage");
    }

	@Override
	public String getPurchaseOption() {
		return items[purchaseOptionIndex];
	}

	@Override
	public LineItemType getLineItemType() {
		return lineItemType;
	}
	
	private double computeProductNormalizedSizeFactor(String usageType) {
		String ut = usageType;
		if (ut.contains(":"))
			ut = ut.split(":")[1];
		
		if (ut.startsWith("db."))
			ut = ut.substring("db.".length());
		
		String[] usageParts = ut.split("\\.");
		if (usageParts.length < 2)
			return 1.0;
		String size = usageParts[1];
		
		if (size.endsWith("xlarge") && size.length() > "xlarge".length())
			return Double.parseDouble(size.substring(0, size.lastIndexOf("xlarge"))) * 8;
		
		Double factor = normalizationFactors.get(size);
		return factor == null ? 1.0 : factor;
	}
	
	@Override
	public String getUsageQuantity() {
    	String purchaseOption = getPurchaseOption();
    	if (purchaseOption.isEmpty() || purchaseOption.equals("All Upfront")) {
    		return super.getUsageQuantity();
    	}

    	if (lineItemType == LineItemType.DiscountedUsage) {
			double usageAmount = Double.parseDouble(items[usageQuantityIndex]);
			double normFactor = items[lineItemNormalizationFactorIndex].isEmpty() ? computeProductNormalizedSizeFactor(items[usageTypeIndex]) : Double.parseDouble(items[lineItemNormalizationFactorIndex]);
			double productFactor = items[productNormalizationSizeFactorIndex].isEmpty() ? computeProductNormalizedSizeFactor(items[productUsageTypeIndex]) : Double.parseDouble(items[productNormalizationSizeFactorIndex]);
			Double actualUsage = usageAmount * normFactor / productFactor;			
			return actualUsage.toString();
		}
		return super.getUsageQuantity();
	}
	
	@Override
	public String getPublicOnDemandCost() {
		return items[publicOnDemandCostIndex];
	}
	
	public int getLineItemTypeIndex() {
		return lineItemTypeIndex;
	}

	public int getResourceTagStartIndex() {
		return resourceTagStartIndex;
	}

	public int getPurchaseOptionIndex() {
		return purchaseOptionIndex;
	}

	public int getLineItemNormalizationFactorIndex() {
		return lineItemNormalizationFactorIndex;
	}

	public int getProductNormalizationSizeFactorIndex() {
		return productNormalizationSizeFactorIndex;
	}

	public int getProductUsageTypeIndex() {
		return productUsageTypeIndex;
	}
	
	public int getReservationArnIndex() {
		return reservationArnIndex;
	}

	public String getPricingUnit() {
		String unit = items[pricingUnitIndex];
		if (unit.equals("Hrs")) {
			unit = "hours";
		}
		if (unit.equals("GB-Mo")) {
			unit = "GB";
		}
		else {
			// Don't bother with any other units as AWS is extremely inconsistent
			unit = super.getPricingUnit();
		}
		return unit;
	}
	
	@Override
	public String getLineItemId() {
		return items[lineItemIdIndex];
	}
	
	@Override
	public String getReservationId() {
		String arn = items[reservationArnIndex];
		// First try the form for ec2 reservations
		// Note: Prior to October 2017 RDS reservation IDs used the EC2 form and did not match the RDS RI ID.
		int i = arn.indexOf("/");
		if (i < 0) {
			// try the rds form
			i = arn.lastIndexOf(":");
		}
		return i > 0 ? arn.substring(i + 1) : arn;
	}
}

