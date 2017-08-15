package com.netflix.ice.common;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public abstract class LineItem {
    public static final DateTimeFormatter amazonBillingDateFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(DateTimeZone.UTC);
    public static final DateTimeFormatter amazonBillingDateFormat2 = DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss").withZone(DateTimeZone.UTC);
    public static final DateTimeFormatter amazonBillingDateFormatISO = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ").withZone(DateTimeZone.UTC);

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
    
    abstract public String getResourceTagsString();
    
    abstract public boolean isReserved();

	public String getPurchaseOption() {
		return null;
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

}
