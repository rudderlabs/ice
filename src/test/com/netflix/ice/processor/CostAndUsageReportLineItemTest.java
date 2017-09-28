package com.netflix.ice.processor;

import static org.junit.Assert.*;

import org.junit.Test;

import com.netflix.ice.common.LineItem;

public class CostAndUsageReportLineItemTest {
	static String[] header = new String[]{
    	"identity/LineItemId",
    	"bill/BillType",
        "lineItem/UsageAccountId",
        "lineItem/LineItemType",
        "lineItem/UsageStartDate",
        "lineItem/UsageEndDate",
        "lineItem/UsageType",
        "lineItem/Operation",
        "lineItem/AvailabilityZone",
        "lineItem/ResourceId",
        "lineItem/UsageAmount",
        "lineItem/NormalizationFactor",
        "lineItem/UnblendedRate",
        "lineItem/UnblendedCost",
        "lineItem/LineItemDescription",
        "product/ProductName",
        "product/normalizationSizeFactor",
        "product/usagetype", 
        "pricing/PurchaseOption",
        "pricing/publicOnDemandCost",        
        "pricing/term",
        "pricing/unit",
	};

	class TestReport extends CostAndUsageReport {
		public TestReport() {
			super(null, null);
		}
		
		@Override
		public int getColumnIndex(String category, String name) {
			String column = category + "/" + name;
			for (int i = 0; i < header.length; i++) {
				if (header[i].equals(column))
					return i;
			}
			return -1;
		}
		public int getCategoryStartIndex(String category) {
			for (int i = 0; i < header.length; i++) {
				if (header[i].startsWith(category))
					return i;
			}
			return -1;
		}
		public String[] getCategoryHeader(String category) {
			return null;
		}
		
	}
	
	@Test
	public void testGetUsageQuantity() {
		LineItem li = new CostAndUsageReportLineItem(false, new TestReport());
		
		String[] item = {
				"somelineitemid",	// LineItemId
				"Anniversary",		// BillType
				"234567890123",		// UsageAccountId
				"DiscountedUsage",	// LineItemType
				"2017-09-01T00:00:00Z",	// UsageStartDate
				"2017-09-01T01:00:00Z", // UsageEndDate
				"APS2-InstanceUsage:db.t2.micro", // UsageType
				"CreateDBInstance:0014", // Operation
				"ap-southeast-2", // AvailabilityZone
				"arn:aws:rds:ap-southeast-2:123456789012:db:ss1v3i6xr3d1hss", // ResourceId
				"1.0000000000", // UsageAmount
				"", // NormalizationFactor
				"0.0000000000", // UnblendedRate
				"0.0000000000", // UnblendedCost
				"PostgreSQL, db.t2.micro reserved instance applied", // LineItemDescription
				"Amazon Relational Database Service", // ProductName
				"0.5", // normalizationSizeFactor
				"APS2-InstanceUsage:db.t2.micro", // usagetype
				"Partial Upfront", // PurchaseOption
				"0.0280000000", // publicOnDemandCost
				"Reserved", // term
				"Hrs", // unit
		};
		li.setItems(item);
		assertEquals("Usage quantity is incorrect", Double.parseDouble(li.getUsageQuantity()), 1.0, 0.001);
	}

}
