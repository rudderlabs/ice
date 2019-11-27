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
import com.netflix.ice.processor.config.TagConfig;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.UserTag;

public class BasicResourceServiceTest {
    private static final String resourcesDir = "src/test/resources";

	@Test
	public void testGetResourceGroup() {
		CostAndUsageReport caur = new CostAndUsageReport(new File(resourcesDir, "ResourceTest-Manifest.json"), null);
		LineItem li = new CostAndUsageReportLineItem(false, null, caur);		
		String[] item = {
				"123456789012", // PayerAccountId
				"DiscountedUsage", // LineItemType
				"foobar@example.com", // resourceTags/user:Email
				"Prod", // resourceTags/user:Environment
				"", // resourceTags/user:environment
				"serviceAPI", // resourceTags/user:Product
		};

		li.setItems(item);
		ProductService ps = new BasicProductService();
		// include a tag not in the line item
		String[] customTags = new String[]{
				"Environment", "Product", "CostCenter"
			};
		ResourceService rs = new BasicResourceService(ps, customTags, new String[]{});
		rs.initHeader(li.getResourceTagsHeader(), "123456789012");
		Map<String, String> defaultTags = Maps.newHashMap();
		defaultTags.put("CostCenter", "1234");
		rs.putDefaultTags("123456789012", defaultTags);
		
		BasicAccountService as = new BasicAccountService();
		ResourceGroup resource = rs.getResourceGroup(as.getAccountByName("123456789012"), Region.US_EAST_1, ps.getProductByName(Product.ec2Instance), li, 0);
		assertEquals("Resource name doesn't match", "Prod" + ResourceGroup.separator + "serviceAPI" + ResourceGroup.separator + "1234", resource.name);
	}
	
	@Test
	public void testUserTags() {
		ProductService ps = new BasicProductService();
		String[] customTags = new String[]{
				"Environment", "Product"
			};
		ResourceService rs = new BasicResourceService(ps, customTags, new String[]{});
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
		ProductService ps = new BasicProductService();
		String[] customTags = new String[]{
				"Environment"
			};
		
		
		Map<String, List<String>> tagValues = Maps.newHashMap();
		tagValues.put("Prod", Lists.newArrayList("production", "prd"));
		tagValues.put("QA", Lists.newArrayList("test", "quality assurance"));
		TagConfig tc = new TagConfig("Environment", null, tagValues);
		
		ResourceService rs = new BasicResourceService(ps, customTags, new String[]{});
		List<TagConfig> configs = Lists.newArrayList();
		configs.add(tc);
		rs.setTagConfigs("234567890123", configs);

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
				"production", // resourceTags/user:Environment
		};
		CostAndUsageReport caur = new CostAndUsageReport(new File(resourcesDir, "LineItemTest-Manifest.json"), null);
		LineItem li = new CostAndUsageReportLineItem(false, null, caur);		
		li.setItems(item);
		
		// Check for value in alias list
		rs.initHeader(li.getResourceTagsHeader(), "234567890123");		
		String tagValue = rs.getUserTagValue(li, rs.getUserTags().get(0));
		assertEquals("Incorrect tag value alias", "Prod", tagValue);
		
		// Check for non-matching-case version of value
		item[item.length - 1] = "prod";
		tagValue = rs.getUserTagValue(li, rs.getUserTags().get(0));
		assertEquals("Incorrect tag value alias", "Prod", tagValue);
		
		// Check for tag with leading/trailing white space
		item[item.length - 1] = " pr od ";
		tagValue = rs.getUserTagValue(li, rs.getUserTags().get(0));
		assertEquals("Incorrect tag value alias when leading/trailing white space", "Prod", tagValue);
		
		// Check for tag with embedded white space in alias config
		item[item.length - 1] = "qualityassurance";
		tagValue = rs.getUserTagValue(li, rs.getUserTags().get(0));
		assertEquals("Incorrect tag value alias when config had a space in the value", "QA", tagValue);
	}
	
	@Test
	public void testDefaultAccountTags() {
		CostAndUsageReport caur = new CostAndUsageReport(new File(resourcesDir, "ResourceTest-Manifest.json"), null);
		LineItem li = new CostAndUsageReportLineItem(false, null, caur);		
		String[] item = {
				"123456789012", // PayerAccountId
				"DiscountedUsage", // LineItemType
				"foobar@example.com", // resourceTags/user:Email
				"", // resourceTags/user:Environment
				"", // resourceTags/user:environment
				"serviceAPI", // resourceTags/user:Product
		};
		li.setItems(item);
		ProductService ps = new BasicProductService();
		String[] customTags = new String[]{
				"Environment", "Product"
			};
		Map<String, String> defaultTags = Maps.newHashMap();
		defaultTags.put("Environment", "Prod");
		
		ResourceService rs = new BasicResourceService(ps, customTags, new String[]{});
		rs.putDefaultTags("12345", defaultTags);
		rs.initHeader(li.getResourceTagsHeader(), "12345");		
		
		ResourceGroup resourceGroup = rs.getResourceGroup(new Account("12345", "AccountTagTest", null), null, ps.getProductByName(Product.ec2Instance), li, 0);
		UserTag[] userTags = resourceGroup.getUserTags();
		assertEquals("default resource group not set", userTags[0].name, "Prod");
	}
	
	@Test
	public void testGetUserTagCoverage() {
		ProductService ps = new BasicProductService();
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
			
			@Override
			public String getPayerAccountId() {
				return "123456789012";
			}
		}
		ResourceService rs = new BasicResourceService(ps, customTags, new String[]{});
		rs.initHeader(new String[]{ "user:Email", "user:Department", "user:Environment" }, "1234");
		LineItem lineItem = new TestLineItem(new String[]{ "joe@company.com", "1234", "" });
		boolean[] coverage = rs.getUserTagCoverage(lineItem);
		
		assertEquals("Environment isn't first user tag", "Environment", rs.getUserTags().get(0));
		assertFalse("Environment is set", coverage[0]);
		assertTrue("Department not set", coverage[1]);
		assertTrue("Email not set", coverage[2]);
	}
}
