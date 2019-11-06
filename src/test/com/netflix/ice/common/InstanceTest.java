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
package com.netflix.ice.common;

import static org.junit.Assert.*;

import java.util.Map;

import org.junit.Test;

import com.google.common.collect.Maps;
import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.Zone;
import com.netflix.ice.tag.Zone.BadZone;

public class InstanceTest {
	private static Zone us_east_1a;
	
	static {
		try {
			us_east_1a = Region.US_EAST_1.getZone("us-east-1a");
		} catch (BadZone e) {
		}
	}

	@Test
	public void testSerializer() throws BadZone {
		Map<String, String> tags = Maps.newHashMap();
		tags.put("Environment", "prod");
		AccountService as = new BasicAccountService();
		ProductService ps = new BasicProductService();
		
		Account account = as.getAccountById("123456789012", "");
		Instance i = new Instance("i-17f85eef87efb7a53", "c4.2xlarge", account, Region.US_EAST_1, us_east_1a, ps.getProductByName(Product.ec2), tags, 0);
		String[] values = i.values();
		String[] expected = new String[]{"i-17f85eef87efb7a53","c4.2xlarge","123456789012",account.getIceName(),"us-east-1","us-east-1a","EC2","Environment=prod"};
		assertArrayEquals("serialized form wrong", expected, values);
		
		Instance got = new Instance(values, as, ps);
		
		assertEquals("IDs don't match", i.id, got.id);
		assertEquals("types don't match", i.type, got.type);
		assertEquals("accounts don't match", i.account, got.account);
		assertEquals("regions don't match", i.region, got.region);
		assertEquals("zones don't match", i.zone, got.zone);
		assertEquals("products don't match", i.product, got.product);
		assertEquals("tags size is wrong", i.tags.size(), got.tags.size());
		assertEquals("Environment tag is wrong", "prod", got.tags.get("Environment"));
	}
	
	@Test
	public void testTagWithCommas() throws BadZone {
		Map<String, String> tags = Maps.newHashMap();
		String tagValue = "= I have equal signs, and a comma =";
		tags.put("Name", tagValue);
		AccountService as = new BasicAccountService();
		ProductService ps = new BasicProductService();
		
				
		Account account = as.getAccountById("123456789012", "");
		Instance i = new Instance("i-17f85eef87efb7a53", "c4.2xlarge", account, Region.US_EAST_1, us_east_1a, ps.getProductByName(Product.ec2), tags, 0);
		String[] values = i.values();
		String[] expected = new String[]{"i-17f85eef87efb7a53","c4.2xlarge","123456789012",account.getIceName(),"us-east-1","us-east-1a","EC2","Name=" + tagValue};
		assertArrayEquals("serialized form wrong", expected, values);
		
		Instance got = new Instance(values, as, ps);
		assertEquals("IDs don't match", i.id, got.id);
		assertEquals("types don't match", i.type, got.type);
		assertEquals("accounts don't match", i.account, got.account);
		assertEquals("regions don't match", i.region, got.region);
		assertEquals("zones don't match", i.zone, got.zone);
		assertEquals("products don't match", i.product, got.product);
		assertEquals("tags size is wrong", i.tags.size(), got.tags.size());
		assertEquals("Name tag is wrong", tagValue, got.tags.get("Name"));
	}
	
	@Test
	public void testMultipleTags() throws BadZone {
		Map<String, String> tags = Maps.newHashMap();
		tags.put("Environment", "prod");
		tags.put("Email", "foo@bar.com");
		tags.put("Name", "= I have equal signs, and a comma =");
		AccountService as = new BasicAccountService();
		ProductService ps = new BasicProductService();		
				
		Instance i = new Instance("i-17f85eef87efb7a53", "c4.2xlarge", as.getAccountById("123456789012", ""), Region.US_EAST_1, us_east_1a, ps.getProductByName(Product.ec2), tags, 0);
		String[] values = i.values();
		Instance got = new Instance(values, as, ps);
		
		assertEquals("IDs don't match", i.id, got.id);
		assertEquals("types don't match", i.type, got.type);
		assertEquals("accounts don't match", i.account, got.account);
		assertEquals("regions don't match", i.region, got.region);
		assertEquals("zones don't match", i.zone, got.zone);
		assertEquals("products don't match", i.product, got.product);
		assertEquals("tags size is wrong", i.tags.size(), got.tags.size());
		assertEquals("Environment tag is wrong", "prod", got.tags.get("Environment"));
		assertEquals("Email tag is wrong", "foo@bar.com", got.tags.get("Email"));
	}
	
	@Test
	public void testTagWithSeparator() throws BadZone {
		Map<String, String> tags = Maps.newHashMap();
		String tagValue = "I have a separator: " + Instance.tagSeparator;
		tags.put("Name", tagValue);
		AccountService as = new BasicAccountService();
		ProductService ps = new BasicProductService();		
				
		Instance i = new Instance("i-17f85eef87efb7a53", "c4.2xlarge", as.getAccountById("123456789012", ""), Region.US_EAST_1, us_east_1a, ps.getProductByName(Product.ec2), tags, 0);
		String[] values = i.values();
		Instance got = new Instance(values, as, ps);
		assertEquals("Name tag is wrong", tagValue.replace(Instance.tagSeparator, Instance.tagSeparatorReplacement), got.tags.get("Name"));
	}

	@Test
	public void testNoTags() throws BadZone {
		Map<String, String> tags = Maps.newHashMap();
		AccountService as = new BasicAccountService();
		ProductService ps = new BasicProductService();
		
				
		Instance i = new Instance("i-17f85eef87efb7a53", "c4.2xlarge", as.getAccountById("123456789012", ""), Region.US_EAST_1, us_east_1a, ps.getProductByName(Product.ec2), tags, 0);
		String[] values = i.values();
		Instance got = new Instance(values, as, ps);
		assertEquals("tags size is wrong", i.tags.size(), got.tags.size());
	}
	
	@Test
	public void testNoZone() throws BadZone {
		Map<String, String> tags = Maps.newHashMap();
		tags.put("Environment", "prod");
		AccountService as = new BasicAccountService();
		ProductService ps = new BasicProductService();
		
				
		Instance i = new Instance("i-17f85eef87efb7a53", "c4.2xlarge", as.getAccountById("123456789012", ""), Region.US_EAST_1, null, ps.getProductByName(Product.ec2), tags, 0);
		String[] values = i.values();
		Instance got = new Instance(values, as, ps);
		assertEquals("tags size is wrong", i.tags.size(), got.tags.size());
	}

	@Test
	public void testNoTagsNoZone() throws BadZone {
		Map<String, String> tags = Maps.newHashMap();
		AccountService as = new BasicAccountService();
		ProductService ps = new BasicProductService();
		
				
		Instance i = new Instance("i-17f85eef87efb7a53", "c4.2xlarge", as.getAccountById("123456789012", ""), Region.US_EAST_1, null, ps.getProductByName(Product.ec2), tags, 0);
		String[] values = i.values();
		Instance got = new Instance(values, as, ps);
		assertEquals("tags size is wrong", i.tags.size(), got.tags.size());
	}
}
