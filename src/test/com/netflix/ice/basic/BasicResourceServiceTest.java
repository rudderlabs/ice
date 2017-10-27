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
		ResourceService rs = new BasicResourceService(ps, tagKeys, tagValues);
		String[] customTags = new String[]{
			"Environment", "Product"
		};
		rs.init(customTags);
		rs.initHeader(li.getResourceTagsHeader());
		
		String resource = rs.getResource(null, null, ps.getProductByName(Product.ec2Instance), li, 0);
		assertEquals("Resource name doesn't match", "Prod_serviceAPI", resource);
	}
}
