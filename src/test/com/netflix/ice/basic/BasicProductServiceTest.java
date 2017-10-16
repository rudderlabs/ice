package com.netflix.ice.basic;

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Test;

import com.netflix.ice.common.ProductService;
import com.netflix.ice.tag.Product;

public class BasicProductServiceTest {

	@Test
	public void testConstructor() {
		Properties alts = new Properties();
		alts.setProperty("VN", "Verbose Name");
		ProductService ps = new BasicProductService(alts);
		Product vn = ps.getProductByAwsName("Verbose Name");
		assertTrue("Alternate name not registered by constructor", vn.name.equals("VN"));
	}

	@Test
	public void testGetProductByAwsName() {
		ProductService ps = new BasicProductService(null);
		Product product1 = ps.getProductByAwsName("AWS ProductA");
		assertTrue("Wrong product name, expected ProductA, got " + product1.name, product1.name.equals("ProductA"));
		
		// get product again and make sure it's the same
		assertTrue("Didn't get the same product the second time", ps.getProductByAwsName("AWS ProductA") == product1);
		
		Product product2 = ps.getProductByAwsName("Amazon ProductA");
		assertTrue("Wrong product name, expected ProductA, got " + product2.name, product2.name.equals("ProductA"));
		
		// get product again and make sure it's the same
		assertTrue("Didn't get the same product the second time", ps.getProductByAwsName("Amazon ProductA") == product2);
		
		// Should automatically match AWS and Amazon prefixes
		assertEquals("Both product strings should not refer to the same object", product1, product2);
	}

	@Test
	public void testGetProductByName() {
		ProductService ps = new BasicProductService(null);
		Product product1 = ps.getProductByName("Product1");
		assertTrue("Wrong product name, expected Product, got " + product1.name, product1.name.equals("Product1"));
		
		Product product2 = ps.getProductByAwsName("Amazon Product1");		
		assertEquals("Both product strings should refer to the same object", product1, product2);
	}

	@Test
	public void testAlternateProductName() {
		Properties alts = new Properties();
		alts.setProperty("SGS", "Simple Gravity Service");
		
		ProductService ps = new BasicProductService(alts);
		
		Product product = ps.getProductByName("Simple Gravity Service");
		assertEquals("Wrong product name", "SGS", product.name);
		assertEquals("Wrong product file name", product.getFileName(), "Simple_Gravity_Service");
		
		Product product2 = ps.getProductByName("Simple Gravity Service");
		assertEquals("Products don't match", product, product2);
		assertTrue("Products don't match using ==", product == product2);
	}
	
}
