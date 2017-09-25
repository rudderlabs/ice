package com.netflix.ice.tag;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class ProductTest {

	@Before
	public void addAlternate() {
		Product.addAlternate("Product", "Product (P)");
	}
	
	@Test
	public void testCanonicalName() {
		Product product = new Product("Amazon Elastic MapReduce");
		assertTrue("Wrong product name, expected Elastic MapReduce, got " + product.name, product.name.equals("Elastic MapReduce"));		

		product = new Product("AWS Elastic Beanstalk");
		assertTrue("Wrong product name, expected Elastic Beanstalk, got " + product.name, product.name.equals("Elastic Beanstalk"));		
}
	
	@Test
	public void testAlternateFromAwsName() {
		Product product = new Product(Product.getAlternate("Product"));
		assertTrue("Wrong product name, expected Product (P), got " + product.name, product.name.equals("Product (P)"));
		
		String alt = Product.getAlternate("Product");
		assertTrue("Wrong product name from getAlternate(), expected Product (P), got " + alt, alt.equals("Product (P)"));
		
		String aws = Product.getAwsName("Product (P)");
		assertTrue("Wrong product name from getAwsName(), expected Product, got " + aws, aws.equals("Product"));
	}
	
	@Test
	public void testAlternateFromAlternateName() {
		Product product = new Product(Product.getAlternate("Product (P)"));
		assertTrue("Wrong product name, expected Product (P), got " + product.name, product.name.equals("Product (P)"));
		
		String alt = Product.getAlternate("Product");
		assertTrue("Wrong product name from getAlternate(), expected Product (P), got " + alt, alt.equals("Product (P)"));
		
		String aws = Product.getAwsName("Product (P)");
		assertTrue("Wrong product name from getAwsName(), expected Product, got " + aws, aws.equals("Product"));		
	}
	
	@Test
	public void testShortName() {
		Product.addAlternate("Foo Bar", "Foo Bar (FB)");
		Product product = new Product("Amazon Foo Bar");
		assertTrue("Wrong short name from getShortName(), expected Foo_Bar, got " + product.getFileName(), "Foo_Bar".equals(product.getFileName()));
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
