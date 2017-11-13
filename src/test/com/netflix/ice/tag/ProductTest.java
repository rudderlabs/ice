package com.netflix.ice.tag;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class ProductTest {

	@Before
	public void addOverride() {
		Product.addOverride("Product", "Product (P)");
	}
	
	@Test
	public void testCanonicalName() {
		Product product = new Product("Amazon Elastic MapReduce");
		assertEquals("Wrong product name", "Elastic MapReduce", product.name);		

		product = new Product("AWS Elastic Beanstalk");
		assertEquals("Wrong product name", "Elastic Beanstalk",  product.name);		
}
	
	@Test
	public void testOverrideFromCanonicalName() {
		Product product = new Product(Product.getOverride("Product"));
		assertEquals("Wrong product name", "Product (P)", product.name);
		
		String alt = Product.getOverride("Product");
		assertEquals("Wrong product name from getOverride()", "Product (P)", alt);
		
		String aws = Product.getCanonicalName("Product (P)");
		assertEquals("Wrong product name from getAwsName()", "Product", aws);
	}
	
	@Test
	public void testAlternateFromAlternateName() {
		Product product = new Product(Product.getOverride("Product (P)"));
		assertEquals("Wrong product name", "Product (P)", product.name);
		
		String alt = Product.getOverride("Product");
		assertEquals("Wrong product name from getOverride()", "Product (P)", alt);
		
		String aws = Product.getCanonicalName("Product (P)");
		assertEquals("Wrong product name from getAwsName()", "Product", aws);		
	}
	
	@Test
	public void testShortName() {
		Product.addOverride("Foo Bar", "Foo Bar (FB)");
		Product product = new Product("Amazon Foo Bar");
		assertEquals("Wrong short name from getShortName()", "Foo_Bar", product.getFileName());
	}
	
	@Test
	public void testIsEc2() {
		Product product = new Product(Product.ec2);
		assertTrue("isEc2() returned false, but should be true", product.isEc2());
	}
	
	@Test
	public void testIsSupport() {
		Product product = new Product("Support (Developer)");
		assertTrue("isSupport() returned false, but should be true", product.isSupport());
		
		product = new Product("Suppose Not");
		assertFalse("isSupport() returned true, but should be false", product.isSupport());
	}
	
	@Test
	public void testIsRds() {
		Product productRds = new Product(Product.rds);
		Product productRdsFull = new Product(Product.rdsFull);
		assertTrue("isRDS() returned false, but should be true", productRds.isRds());
		assertTrue("isRDS() returned false, but should be true", productRdsFull.isRds());
	}
	
}
