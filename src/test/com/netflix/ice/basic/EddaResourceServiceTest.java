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

import java.util.Properties;

import org.json.JSONArray;
import org.junit.Test;

import com.netflix.ice.common.LineItem;
import com.netflix.ice.processor.DetailedBillingReportLineItem;
import com.netflix.ice.tag.Product;


/**
 * Note: These tests require a running instance of Edda running at the url configured in ice.properties, so they 
 * strictly speaking not unit tests any more...
 */
public class EddaResourceServiceTest {
    static final String[] dbrHeader = {
		"InvoiceID","PayerAccountId","LinkedAccountId","RecordType","RecordId","ProductName","RateId","SubscriptionId","PricingPlanId","UsageType","Operation","AvailabilityZone","ReservedInstance","ItemDescription","UsageStartDate","UsageEndDate","UsageQuantity","BlendedRate","BlendedCost","UnBlendedRate","UnBlendedCost"
    };
    
    private LineItem makeLineItem() {
		LineItem lineItem = new DetailedBillingReportLineItem(false, true, dbrHeader);
		String[] items = new String[dbrHeader.length];
		for (int i = 0; i < items.length; i++)
			items[i] = null;
		lineItem.setItems(items);
		
		return lineItem;
    }

	@Test
	public void test() throws Exception {
		EddaResourceService service = new EddaResourceService(new Properties());

		service.init(null);

		// does nothing really...
		service.commit();

		assertNotNull(service.getProductsWithResources());
		
		LineItem lineItem = makeLineItem();
		
		assertEquals("Product-name for unsupported resource", "somename", service.getResource(null, null, new Product("somename"), lineItem, 0));
		assertEquals("Error for empty resourceId", "Error", service.getResource(null, null, new Product(Product.ec2), lineItem, 0));
		lineItem.setResource("");
		assertEquals("Error for empty resourceId", "Error", service.getResource(null, null, new Product(Product.ec2), lineItem, 0));

		lineItem.setResource("someunknowninstance");
		assertEquals("Unknown for resourceIds that we do not find", "Unknown", service.getResource(null, null, new Product(Product.ec2), lineItem, 0));

		JSONArray instances = service.readInstanceArray();

		lineItem.setResource(instances.getString(0));
		String resource = service.getResource(null, null, new Product(Product.ec2), lineItem, 0);
		assertFalse("Not Error for an actual instance", "Error".equals(resource));

		resource = service.getResource(null, null, new Product(Product.ec2), lineItem, 0);
		assertFalse("Not Error for an actual instance", "Error".equals(resource));

		for(int i = 0;i < instances.length();i++) {
			lineItem.setResource(instances.getString(i));
			resource = service.getResource(null, null, new Product(Product.ec2), lineItem, 0);
			assertFalse("Not Error for an actual instance", "Error".equals(resource));
		}
	}

	@Test
	public void testWrongURL() throws Exception {
		// use a normal setup for retrieving the instances
		EddaResourceService service = new EddaResourceService(new Properties());
		JSONArray instances = service.readInstanceArray();

		LineItem lineItem = makeLineItem();
		

		// overwrite config with an invalid hostname
		Properties prop = new Properties();
		prop.setProperty("ice.eddaresourceservice.url", "http://invalidhostname:18081/edda/api/v2/");
		service = new EddaResourceService(prop);

		// now the retrieved resources should return an error even for valid instances
		lineItem.setResource(instances.getString(0));
		String resource = service.getResource(null, null, new Product(Product.ec2), lineItem, 0);
		assertTrue("Error even for an actual instance when using wrong URL", "Error".equals(resource));

		// overwrite config with an invalid URL
		prop.setProperty("ice.eddaresourceservice.url", "sasie://invalidhostname:18081/edda/api/v2/");
		service = new EddaResourceService(prop);

		// now the retrieved resources should return an error even for valid instances
		lineItem.setResource(instances.getString(0));
		resource = service.getResource(null, null, new Product(Product.ec2), lineItem, 0);
		assertTrue("Error even for an actual instance when using wrong URL", "Error".equals(resource));
	}
}
