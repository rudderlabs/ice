package com.netflix.ice.common;

import java.util.Map;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.netflix.ice.tag.Region;

public abstract class LineItem {
    public static final DateTimeFormatter amazonBillingDateFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(DateTimeZone.UTC);
    public static final DateTimeFormatter amazonBillingDateFormat2 = DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss").withZone(DateTimeZone.UTC);
    public static final DateTimeFormatter amazonBillingDateFormatISO = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ").withZone(DateTimeZone.UTC);

    private int lineNumber = 0;
    
	protected int accountIdIndex;
	protected int productIndex;
	protected int zoneIndex;
	protected int reservedIndex;
	protected int descriptionIndex;
	protected int usageTypeIndex;
	protected int operationIndex;
	protected int usageQuantityIndex;
	protected int startTimeIndex;
	protected int endTimeIndex;
	protected int rateIndex;
	protected int costIndex;
	protected int resourceIndex;
    
	protected String[] items;
    
	public String[] getItems() {
		return items;
	}
	
    public void setItems(String[] items) {
    	this.items = items;
    	lineNumber++;
    }
    
    public int getLineNumber() {
    	return lineNumber;
    }
    
    public String getAccountId() {
    	return items[accountIdIndex];
    }
    
    public String getProduct() {
    	return items[productIndex];
    }
    
    public String getZone() {
    	return items[zoneIndex];
    }
    
    public String getReserved() {
    	return items[reservedIndex];
    }
    
    public String getDescription() {
    	return items[descriptionIndex];
    }
    
    public String getUsageType() {
    	return items[usageTypeIndex];
    }
    
    public String getOperation() {
    	return items[operationIndex];
    }
    
    public String getUsageQuantity() {
    	return items[usageQuantityIndex];
    }
    
    public String getStartTime() {
    	return items[startTimeIndex];
    }
    
    public String getEndTime() {
    	return items[endTimeIndex];
    }
    
    public String getRate() {
    	return items[rateIndex];
    }
    
    public String getCost() {
    	return items[costIndex];
    }
    
    public String getResource() {
    	return items[resourceIndex];
    }
    
    public void setResource(String resourceId) {
    	items[resourceIndex] = resourceId;
    }
    
    public boolean hasResources() {
    	return items.length > resourceIndex;
    }

    public long getStartMillis() {
        try {
            return amazonBillingDateFormat.parseMillis(items[startTimeIndex]);
        }
        catch (IllegalArgumentException e) {
            return amazonBillingDateFormat2.parseMillis(items[startTimeIndex]);
        }
    }

    public long getEndMillis() {
        try {
            return amazonBillingDateFormat.parseMillis(items[endTimeIndex]);
        }
        catch (IllegalArgumentException e) {
            return amazonBillingDateFormat2.parseMillis(items[endTimeIndex]);
        }
    }
    
    abstract public String[] getResourceTagsHeader();
    
    abstract public int getResourceTagsSize();
    
    abstract public String getResourceTag(int index);
    
    abstract public Map<String, String> getResourceTags();
    
    abstract public boolean isReserved();

	public String getPurchaseOption() {
		return null;
	}

	public String getPublicOnDemandCost() {
		return "";
	}

	public int getAccountIdIndex() {
		return accountIdIndex;
	}

	public int getProductIndex() {
		return productIndex;
	}

	public int getZoneIndex() {
		return zoneIndex;
	}

	public int getReservedIndex() {
		return reservedIndex;
	}

	public int getDescriptionIndex() {
		return descriptionIndex;
	}

	public int getUsageTypeIndex() {
		return usageTypeIndex;
	}

	public int getOperationIndex() {
		return operationIndex;
	}

	public int getUsageQuantityIndex() {
		return usageQuantityIndex;
	}

	public int getStartTimeIndex() {
		return startTimeIndex;
	}

	public int getEndTimeIndex() {
		return endTimeIndex;
	}

	public int getRateIndex() {
		return rateIndex;
	}

	public int getCostIndex() {
		return costIndex;
	}

	public int getResourceIndex() {
		return resourceIndex;
	}
	
	/**
	 * BillType
	 */
	public static enum BillType {
		Anniversary,
		Purchase,
		Refund;
	}

	/**
	 * LineItemType is one of the columns from AWS Cost and Usage reports
	 */
	public static enum LineItemType {
		Credit,
		DiscountedUsage,
		EdpDiscount,
		Fee,
		Refund,
		RIFee,
		RiVolumeDiscount,
		Tax,
		Usage;
	}
	
	public LineItemType getLineItemType() {
		return null;
	}
	
	public String getPricingUnit() {
    	String usageType = getUsageType();
		String unit = "";
		if (usageType.startsWith("AW-HW-1")) {
			// AmazonWorkSpaces
		}
		else if (usageType.contains("Lambda-GB-Second") || usageType.contains("Bytes") || usageType.contains("ByteHrs") || getDescription().contains("GB")) {
            unit = "GB";
    	}
        
        // Won't indicate "hours" for instance usage, so clients must handle that themselves.
        return unit;
	}

	abstract public String getLineItemId();

	public String getReservationId() {
		return "";
	}
	
	/**
	 * reservation/AmortizedUpfrontCostForUsage() appeared 2018-01 in CUR files
	 * reservation/NetAmortizedUpfrontCostForUsage() appeared 2019-01 in CUR files
	 * @return
	 */
	public String getAmortizedUpfrontCostForUsage() {
		return "";
	}
	
	/**
	 * reservation/RecurringFeeForUsage appeared 2018-01 in CUR files
	 * reservation/NetRecurringFeeForUsage appeared 2019-01 in CUR files
	 * @return recurring fee
	 */
	public String getRecurringFeeForUsage() {
		return "";
	}
	
	public Double getUnusedAmortizedUpfrontRate() {
		return null;
	}
	
	public Double getUnusedRecurringRate() {
		return null;
	}

	public Region getProductRegion() {
		return null;
	}

}
