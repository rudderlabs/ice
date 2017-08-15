package com.netflix.ice.processor;

import java.util.Arrays;

import com.netflix.ice.common.LineItem;

public class DetailedBillingReportLineItem extends LineItem {	    
	protected String[] header;

	public DetailedBillingReportLineItem(boolean useBlended, boolean withTags, String[] header) {
    	this.header = header;
    	
        boolean hasBlendedCost = false;
        boolean useBlendedCost = useBlended;
        for (String column: header) {
            if (column.equalsIgnoreCase("UnBlendedCost")) {
                hasBlendedCost = true;
                break;
            }
        }
        accountIdIndex = 2;
        productIndex = 5 + (withTags ? 0 : -1);
        zoneIndex = 11 + (withTags ? 0 : -1);
        reservedIndex = 12 + (withTags ? 0 : -1);
        descriptionIndex = 13 + (withTags ? 0 : -1);
        usageTypeIndex = 9 + (withTags ? 0 : -1);
        operationIndex = 10 + (withTags ? 0 : -1);
        usageQuantityIndex = 16 + (withTags ? 0 : -1);
        startTimeIndex = 14 + (withTags ? 0 : -1);
        endTimeIndex = 15 + (withTags ? 0 : -1);
        // When blended vales are present, the rows look like this
        //    ..., UsageQuantity, BlendedRate, BlendedCost, UnBlended Rate, UnBlended Cost
        // Without Blended Rates
        //    ..., UsageQuantity, UnBlendedRate, UnBlendedCost
        // We want to always reference the UnBlended Cost unless useBlendedCost is true.
        //rateIndex = 19 + (withTags ? 0 : -1) + ((hasBlendedCost && useBlendedCost == false) ? 0 : -2);
        costIndex = 20 + (withTags ? 0 : -1) + ((hasBlendedCost && useBlendedCost == false) ? 0 : -2);
        resourceIndex = 21 + (withTags ? 0 : -1) + (hasBlendedCost ? 0 : -2);
    }

    @Override
    public long getStartMillis() {
        try {
            return amazonBillingDateFormat.parseMillis(items[startTimeIndex]);
        }
        catch (IllegalArgumentException e) {
            return amazonBillingDateFormat2.parseMillis(items[startTimeIndex]);
        }
    }

    @Override
    public long getEndMillis() {
        try {
            return amazonBillingDateFormat.parseMillis(items[endTimeIndex]);
        }
        catch (IllegalArgumentException e) {
            return amazonBillingDateFormat2.parseMillis(items[endTimeIndex]);
        }
    }
    
    @Override
    public String[] getResourceTagsHeader() {
    	return Arrays.copyOfRange(header, resourceIndex + 1, header.length);
    }

    @Override
    public int getResourceTagsSize() {
    	return header.length - resourceIndex - 1;
    }

    @Override
    public String getResourceTag(int index) {
    	return items[resourceIndex + 1 + index];
    }

    @Override
    public String getResourceTagsString() {
    	StringBuilder sb = new StringBuilder();
    	boolean first = true;
    	for (int i = resourceIndex + 1; i < items.length; i++) {
    		if (items[i].isEmpty()) {
    			continue;
    		}
    		sb.append((first ? "" : "|") + header[i].substring("user:".length()) + "=" + items[i]);
    		first = false;
    	}
    	return sb.toString();
    }
    
    @Override
    public boolean isReserved() {
    	return items[reservedIndex].equals("Y");
    }
}

