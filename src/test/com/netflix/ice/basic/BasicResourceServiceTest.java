package com.netflix.ice.basic;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.LineItem;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.processor.CostAndUsageReport;
import com.netflix.ice.processor.CostAndUsageReportLineItem;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.UserTag;

public class BasicResourceServiceTest {
    private static final String resourcesDir = "src/test/resources";

	@Test
	public void testGetResource() {
		CostAndUsageReport caur = new CostAndUsageReport(new File(resourcesDir, "ResourceTest-Manifest.json"), null);
		LineItem li = new CostAndUsageReportLineItem(false, null, caur);		
		String[] item = {
				"DiscountedUsage", // LineItemType
				"foobar@example.com", // resourceTags/user:Email
				"Prod", // resourceTags/user:Environment
				"", // resourceTags/user:environment
				"serviceAPI", // resourceTags/user:Product
		};

		li.setItems(item);
		Map<String, List<String>> tagKeys = Maps.newHashMap();
		Map<String, List<String>> tagValues = Maps.newHashMap();
		ProductService ps = new BasicProductService(null);
		String[] customTags = new String[]{
				"Environment", "Product"
			};
		ResourceService rs = new BasicResourceService(ps, customTags, new String[]{}, tagKeys, tagValues, null);
		rs.initHeader(li.getResourceTagsHeader());
		
		ResourceGroup resource = rs.getResourceGroup(null, null, ps.getProductByName(Product.ec2Instance), li, 0);
		assertEquals("Resource name doesn't match", "Prod" + ResourceGroup.separator + "serviceAPI", resource.name);
	}
	
	@Test
	public void testUserTags() {
		Map<String, List<String>> tagKeys = Maps.newHashMap();
		Map<String, List<String>> tagValues = Maps.newHashMap();
		ProductService ps = new BasicProductService(null);
		String[] customTags = new String[]{
				"Environment", "Product"
			};
		ResourceService rs = new BasicResourceService(ps, customTags, new String[]{}, tagKeys, tagValues, null);
		List<String> userTags = rs.getUserTags();
		assertEquals("userTags list length is incorrect", 2, userTags.size());
	}
	
	@Test
	public void testSpecialUserTagKeyCharactersAndValues() throws IOException {
		Properties p = new Properties();
		p.load(new StringReader("prop.foo+bar=test\n"));
		for (String name: p.stringPropertyNames()) {
			assertEquals("Incorrect property name", "prop.foo+bar", name);
		}
	}
	
	@Test
	public void testGetUserTagValue() {
		Map<String, List<String>> tagKeys = Maps.newHashMap();
		Map<String, List<String>> tagValues = Maps.newHashMap();
		ProductService ps = new BasicProductService(null);
		String[] customTags = new String[]{
				"Environment"
			};
		
		tagValues.put("Prod", Lists.newArrayList("production", "prd"));
		ResourceService rs = new BasicResourceService(ps, customTags, new String[]{}, tagKeys, tagValues, null);

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
				"production", // resourceTags/user:Environment
		};
		CostAndUsageReport caur = new CostAndUsageReport(new File(resourcesDir, "LineItemTest-Manifest.json"), null);
		LineItem li = new CostAndUsageReportLineItem(false, null, caur);		
		li.setItems(item);
		
		// Check for value in alias list
		rs.initHeader(li.getResourceTagsHeader());		
		String tagValue = rs.getUserTagValue(li, rs.getUserTags().get(0));
		assertEquals("Incorrect tag value alias", "Prod", tagValue);
		
		// Check for non-matching-case version of value
		item[item.length - 1] = "prod";
		tagValue = rs.getUserTagValue(li, rs.getUserTags().get(0));
		assertEquals("Incorrect tag value alias", "Prod", tagValue);
	}
	
	@Test
	public void testDefaultAccountTags() {
		CostAndUsageReport caur = new CostAndUsageReport(new File(resourcesDir, "ResourceTest-Manifest.json"), null);
		LineItem li = new CostAndUsageReportLineItem(false, null, caur);		
		String[] item = {
				"DiscountedUsage", // LineItemType
				"foobar@example.com", // resourceTags/user:Email
				"", // resourceTags/user:Environment
				"", // resourceTags/user:environment
				"serviceAPI", // resourceTags/user:Product
		};
		li.setItems(item);
		Map<String, List<String>> tagKeys = Maps.newHashMap();
		Map<String, List<String>> tagValues = Maps.newHashMap();
		ProductService ps = new BasicProductService(null);
		String[] customTags = new String[]{
				"Environment", "Product"
			};
		Map<BasicResourceService.Key, String> defaultTags = Maps.newHashMap();
		defaultTags.put(new BasicResourceService.Key("AccountTagTest", "Environment"), "Prod");

		ResourceService rs = new BasicResourceService(ps, customTags, new String[]{}, tagKeys, tagValues, defaultTags);
		rs.initHeader(li.getResourceTagsHeader());		
		
		ResourceGroup resourceGroup = rs.getResourceGroup(new Account("12345", "AccountTagTest"), null, ps.getProductByName(Product.ec2Instance), li, 0);
		UserTag[] userTags = resourceGroup.getUserTags();
		assertEquals("default resource group not set", userTags[0].name, "Prod");
	}
	
	@Test
	public void testGetUserTagCoverage() {
		Map<String, List<String>> tagKeys = Maps.newHashMap();
		Map<String, List<String>> tagValues = Maps.newHashMap();
		ProductService ps = new BasicProductService(null);
		String[] customTags = new String[]{
				"Environment", "Department", "Email"
			};
		
		class TestLineItem extends LineItem {
			final String[] items;
			
			TestLineItem(String[] items) {
				this.items = items;
			}
			
		    @Override
		    public int getResourceTagsSize() {
		    	if (items.length <= 0)
		    		return 0;
		    	return items.length;
		    }

		    @Override
		    public String getResourceTag(int index) {
		    	if (items.length <= index)
		    		return "";
		    	return items[index];
		    }

			@Override
			public String[] getResourceTagsHeader() {
				return null;
			}

			@Override
			public Map<String, String> getResourceTags() {
				return null;
			}

			@Override
			public boolean isReserved() {
				return false;
			}

			@Override
			public String getPricingUnit() {
				return null;
			}

			@Override
			public String getLineItemId() {
				return null;
			}
		}
		ResourceService rs = new BasicResourceService(ps, customTags, new String[]{}, tagKeys, tagValues, null);
		rs.initHeader(new String[]{ "user:Email", "user:Department", "user:Environment" });
		LineItem lineItem = new TestLineItem(new String[]{ "joe@company.com", "1234", "" });
		boolean[] coverage = rs.getUserTagCoverage(lineItem);
		
		assertEquals("Environment isn't first user tag", "Environment", rs.getUserTags().get(0));
		assertFalse("Environment is set", coverage[0]);
		assertTrue("Department not set", coverage[1]);
		assertTrue("Email not set", coverage[2]);
	}
}
