package com.netflix.ice.basic;

import static org.junit.Assert.*;

import org.junit.Test;

import com.netflix.ice.common.ProductService;
import com.netflix.ice.tag.Product;

public class BasicProductServiceTest {

	@Test
	public void testGetProductByAwsName() {
		ProductService ps = new BasicProductService();
		Product product1 = ps.getProductByAwsName("AWS ProductA");
		assertTrue("Wrong product name, expected ProductA, got " + product1.name, product1.name.equals("ProductA"));
		
		Product product2 = ps.getProductByAwsName("Amazon ProductA");
		assertTrue("Wrong product name, expected ProductA, got " + product2.name, product2.name.equals("ProductA"));
		
		assertEquals("Both product strings should refer to the same object", product1, product2);
	}

	@Test
	public void testGetProductByName() {
		ProductService ps = new BasicProductService();
		Product product1 = ps.getProductByName("Product1");
		assertTrue("Wrong product name, expected Product, got " + product1.name, product1.name.equals("Product1"));
		
		Product product2 = ps.getProductByAwsName("Amazon Product1");		
		assertEquals("Both product strings should refer to the same object", product1, product2);
	}

	@Test
	public void testAlternateProductName() {
		ProductService ps = new BasicProductService();
		Product.addAlternate("Alternate Product", "Alternate Product (AP)");
		Product product = ps.getProductByName("Alternate Product");
		assertTrue("Wrong product name, expected Alternate Product (AP), got " + product.name, product.name.equals("Alternate Product (AP)"));
		assertTrue("Wrong product short name, expected product, got " + product.getShortName(), product.getShortName().equals("alternate_product"));
	}

}
