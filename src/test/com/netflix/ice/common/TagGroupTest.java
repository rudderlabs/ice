package com.netflix.ice.common;

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Test;

import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.UsageType;

public class TagGroupTest {

	@Test
	public void testRdsTags() {
		Properties props = new Properties();
		props.setProperty("RDS", "Relational Database Service");
		ProductService ps = new BasicProductService(props);
		
		AccountService as = new BasicAccountService(new Properties());
		
		TagGroup tg1 = new TagGroup(as.getAccountById("111111111111"), Region.US_EAST_1, null, ps.getProductByAwsName("Relational Database Service"), Operation.getOperation("CreateDBInstance"), UsageType.getUsageType("RDS:GP2-Storage", "GB"), null);
		TagGroup tg2 = new TagGroup(as.getAccountById("111111111111"), Region.US_EAST_1, null, ps.getProductByAwsName("Relational Database Service"), Operation.getOperation("CreateDBInstance"), UsageType.getUsageType("RDS:GP2-Storage", "GB"), null);
		assertEquals("TagGroups should be equivalent", tg1, tg2);

		tg2 = new TagGroup(as.getAccountById("111111111111"), Region.US_EAST_1, null, ps.getProductByName("RDS"), Operation.getOperation("CreateDBInstance"), UsageType.getUsageType("RDS:GP2-Storage", "GB"), null);
		assertEquals("TagGroups with alias should be equivalent", tg1, tg2);
		
		Product p = new Product(Product.rdsFull);
		tg2 = new TagGroup(as.getAccountById("111111111111"), Region.US_EAST_1, null, p, Operation.getOperation("CreateDBInstance"), UsageType.getUsageType("RDS:GP2-Storage", "GB"), null);
		assertEquals("Product tags should be equivalent", tg1.product, p);
		assertEquals("TagGroups with alias made with new product object should be equivalent", tg1, tg2);		
	}
	
	@Test
	public void testEquals() {
		ProductService ps = new BasicProductService(new Properties());
		AccountService as = new BasicAccountService(new Properties());
		TagGroup tg1 = TagGroup.getTagGroup(as.getAccountById("111111111111"), Region.US_EAST_1, null, ps.getProductByAwsName("Relational Database Service"), Operation.getOperation("CreateDBInstance"), UsageType.getUsageType("RDS:GP2-Storage", "GB"), null);
		TagGroup tg2 = TagGroup.getTagGroup(as.getAccountById("111111111111"), Region.US_EAST_1, null, ps.getProductByAwsName("Relational Database Service"), Operation.getOperation("CreateDBInstance"), UsageType.getUsageType("RDS:GP2-Storage", "GB"), null);
		assertTrue("Should be equal", tg1 == tg2);
		assertEquals("Should be equal", tg1, tg1);
		assertEquals("Should be equal", tg1, tg2);
	}

}
