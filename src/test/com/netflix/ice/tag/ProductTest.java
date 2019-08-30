package com.netflix.ice.tag;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import com.netflix.ice.tag.Product.Source;

public class ProductTest {

	@Before
	public void addOverride() {
		Product.addOverride("Product", "Product (P)");
	}
	
	@Test
	public void testConstructor() {
		Product product = new Product("Amazon Elastic MapReduce", "ElasticMapReduce", Source.pricing);
		assertEquals("Wrong product name", "Elastic MapReduce", product.name);
		assertEquals("Wrong service name", "Amazon Elastic MapReduce", product.getServiceName());
		assertEquals("Wrong service code", "ElasticMapReduce", product.getServiceCode());

		product = new Product("AWS CloudTrail", "AWSCloudTrail", Source.pricing);
		assertEquals("Wrong product name", "CloudTrail",  product.name);		
		assertEquals("Wrong service name", "AWS CloudTrail", product.getServiceName());
		assertEquals("Wrong service code", "AWSCloudTrail", product.getServiceCode());
	}
	
	@Test
	public void testOverrideFromCanonicalName() {
		Product product = new Product(Product.getOverride("Product"), "Code", Source.pricing);
		assertEquals("Wrong product name", "Product (P)", product.name);
		
		String alt = Product.getOverride("Product");
		assertEquals("Wrong product name from getOverride()", "Product (P)", alt);
		
		String aws = Product.getCanonicalName("Product (P)");
		assertEquals("Wrong product name from getAwsName()", "Product", aws);
	}
	
	@Test
	public void testAlternateFromAlternateName() {
		Product product = new Product(Product.getOverride("Product (P)"), "Code", Source.pricing);
		assertEquals("Wrong product name", "Product (P)", product.name);
		
		String alt = Product.getOverride("Product");
		assertEquals("Wrong product name from getOverride()", "Product (P)", alt);
		
		String aws = Product.getCanonicalName("Product (P)");
		assertEquals("Wrong product name from getAwsName()", "Product", aws);		
	}
	
	@Test
	public void testCode() {
		Product.addOverride("Foo Bar", "Foo Bar (FB)");
		Product product = new Product("Amazon Foo Bar", "AmazonFooBar", Source.pricing);
		assertEquals("Wrong code from getCode()", "AmazonFooBar", product.getServiceCode());
	}
	
	@Test
	public void testIsEc2() {
		Product product = new Product(Product.ec2, "AmazonEC2", Source.pricing);
		assertTrue("isEc2() returned false, but should be true", product.isEc2());
	}
	
	@Test
	public void testIsSupport() {
		Product product = new Product("Support (Developer)", "AWSDeveloperSupport", Source.pricing);
		assertTrue("isSupport() returned false, but should be true", product.isSupport());
		
		product = new Product("Suppose Not", "AWSSupposeNot", Source.pricing);
		assertFalse("isSupport() returned true, but should be false", product.isSupport());
	}
	
	@Test
	public void testIsRds() {
		Product productRds = new Product(Product.rds, "AmazonRDS", Source.pricing);
		Product productRdsFull = new Product(Product.rdsFull, "AmazonRDS", Source.pricing);
		assertTrue("isRDS() returned false, but should be true", productRds.isRds());
		assertTrue("isRDS() returned false, but should be true", productRdsFull.isRds());
	}
	
	@Test
	public void testHasResourceTag() {
		Product productRds = new Product(Product.rds, "AmazonRDS", Source.pricing);
		Product productCloudWatch = new Product(Product.cloudWatch, "AmazonCloudWatch", Source.pricing);
		assertTrue("RDS should have resource tags", productRds.hasResourceTags());
		assertFalse("CloudWatch should not have resource tags", productCloudWatch.hasResourceTags());
	}
}
