package com.netflix.ice.basic;

import static org.junit.Assert.*;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.Maps;
import com.netflix.ice.common.LineItem;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.processor.CostAndUsageReport;
import com.netflix.ice.processor.CostAndUsageReportLineItem;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.ResourceGroup;

public class BasicResourceServiceTest {
    private static final String resourcesDir = "src/test/resources";

	String[] item = {
			"DiscountedUsage", // LineItemType
			"foobar@example.com", // resourceTags/user:Email
			"Prod", // resourceTags/user:Environment
			"", // resourceTags/user:environment
			"serviceAPI", // resourceTags/user:Product
	};

	@Test
	public void testGetResource() {
		CostAndUsageReport caur = new CostAndUsageReport(new File(resourcesDir, "ResourceTest-Manifest.json"), null);
		LineItem li = new CostAndUsageReportLineItem(false, caur);		
		li.setItems(item);
		Map<String, List<String>> tagKeys = Maps.newHashMap();
		Map<String, List<String>> tagValues = Maps.newHashMap();
		ProductService ps = new BasicProductService(null);
		String[] customTags = new String[]{
				"Environment", "Product"
			};
		ResourceService rs = new BasicResourceService(ps, customTags, new String[]{}, tagKeys, tagValues);
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
		ResourceService rs = new BasicResourceService(ps, customTags, "".split(","), tagKeys, tagValues);
		List<String> userTags = rs.getUserTags();
		assertEquals("userTags list length is incorrect", 2, userTags.size());
	}
}
