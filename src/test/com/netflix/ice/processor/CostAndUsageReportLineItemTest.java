package com.netflix.ice.processor;

import static org.junit.Assert.*;

import java.io.File;
import org.junit.Test;

import com.netflix.ice.common.LineItem;

public class CostAndUsageReportLineItemTest {
    private static final String resourcesDir = "src/test/resources";

	String[] item = {
			"somelineitemid",	// LineItemId
			"Anniversary",		// BillType
			"234567890123",		// PayerAccountId
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
			"Prod", // resourceTags/user:Environment
	};
	
	@Test
	public void testGetUsageQuantity() {
		CostAndUsageReport caur = new CostAndUsageReport(new File(resourcesDir, "LineItemTest-Manifest.json"), null);
		LineItem li = new CostAndUsageReportLineItem(false, null, caur);		
		li.setItems(item);
		assertEquals("Usage quantity is incorrect", Double.parseDouble(li.getUsageQuantity()), 1.0, 0.001);
	}
	
	@Test
	public void testResourceTags() {
		CostAndUsageReport caur = new CostAndUsageReport(new File(resourcesDir, "LineItemTest-Manifest.json"), null);
		LineItem li = new CostAndUsageReportLineItem(false, null, caur);		
		li.setItems(item);
		assertEquals("Wrong resource tags size", 1, li.getResourceTagsSize());
		String[] tagsHeader = li.getResourceTagsHeader();
		assertEquals("Wrong tags header size", 1, tagsHeader.length);
		assertEquals("Wrong tags header value", "user:Environment", tagsHeader[0]);
		assertEquals("Wrong resource tag value", "Prod", li.getResourceTag(0));
		assertEquals("Wrong tags string", "Prod", li.getResourceTags().get("Environment"));
	}

}
