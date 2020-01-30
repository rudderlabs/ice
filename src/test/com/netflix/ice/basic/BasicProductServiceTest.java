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

import org.junit.Test;

import com.netflix.ice.common.ProductService;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Product.Source;

public class BasicProductServiceTest {

	@Test
	public void testGetProduct() {
		BasicProductService ps = new BasicProductService();
		Product product1 = ps.getProduct("AWS ProductA", "AWSProductA");
		assertEquals("Wrong product name", "ProductA", product1.name);
		
		// get product again and make sure it's the same
		assertEquals("Didn't get the same product the second time", product1, ps.getProduct("AWS ProductA", "AWSProductA"));
		
		Product product2 = ps.getProduct("Amazon ProductA", "AmazonProductA");
		assertEquals("Wrong product name", "ProductA", product2.name);
		
		// get product again and make sure it's the same
		assertEquals("Didn't get the same product the second time", product2, ps.getProduct("Amazon ProductA", "AmazonProductA"));
		
		// Should automatically match AWS and Amazon prefixes
		assertEquals("Both product strings should refer to the same object", product1, product2);
		
		Product ec2 = ps.getProduct("Amazon Elastic Compute Cloud", "AmazonEC2");
		assertTrue("isEC2 returned wrong state", ec2.isEc2());
		
		Product cc1 = ps.addProduct(new Product("AWSCodeCommit", "AWSCodeCommit", Source.pricing));
		Product cc2 = ps.getProduct("AWS CodeCommit", "AWSCodeCommit");
		assertFalse("did not replace product with wrong service name from pricing", cc1 == cc2);
		// Test that we don't change the name back if the existing product was set from a CUR rather than the pricing service
		Product cc3 = ps.getProduct("AWSCodeCommit", "AWSCodeCommit");
		assertTrue("replaced product name from cur", cc2 == cc3);		
	}

	@Test
	public void testGetProductByName() {
		ProductService ps = new BasicProductService();
		Product product1 = ps.getProductByServiceName("Product1");
		assertTrue("Wrong product name, expected Product, got " + product1.name, product1.name.equals("Product1"));
		
		Product product2 = ps.getProduct("Amazon Product1", "AmazonProduct1");		
		assertEquals("Both product strings should refer to the same object", product1, product2);
		
		Product rdsInstance = ps.getProductByName(Product.rdsInstance);		
		assertTrue("getProductByName() returned wrong RDS product", rdsInstance.isRdsInstance());
	}
	
	@Test
	public void testGetProductByServiceCode() {
		BasicProductService ps = new BasicProductService();
		Product s3 = ps.addProduct(new Product("Amazon Simple Storage Service", "AmazonS3", Source.pricing));
		Product s3got = ps.getProductByServiceCode("AmazonS3");
		assertTrue("Got different products", s3 == s3got);		
	}

	@Test
	public void testGetProductWithEmptyName() {
		BasicProductService ps = new BasicProductService();
		Product rds = ps.addProduct(new Product("Amazon Relational Database Service", "AmazonRDS", Source.pricing));
		Product rdsEmptyName = ps.getProduct("", "AmazonRDS");
		assertEquals("Changed product name to empty string", rds.name, rdsEmptyName.name);
	}
}
