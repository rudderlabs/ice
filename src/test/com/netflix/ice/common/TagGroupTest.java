package com.netflix.ice.common;

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.UsageType;

public class TagGroupTest {
	private static ProductService ps;
	
	@BeforeClass
	public static void init() {
		Properties props = new Properties();
		props.setProperty("RDS", "Relational Database Service");
		ps = new BasicProductService(props);
	}

	@Test
	public void testRdsTags() {		
		AccountService as = new BasicAccountService(new Properties());
		
		TagGroup tg1 = TagGroup.getTagGroup(as.getAccountById("111111111234"), Region.US_EAST_1, null, ps.getProductByAwsName("AWS Relational Database Service"), Operation.getOperation("CreateDBInstance"), UsageType.getUsageType("RDS:GP2-Storage", "GB"), null);
		TagGroup tg2 = TagGroup.getTagGroup(as.getAccountById("111111111234"), Region.US_EAST_1, null, ps.getProductByAwsName("AWS Relational Database Service"), Operation.getOperation("CreateDBInstance"), UsageType.getUsageType("RDS:GP2-Storage", "GB"), null);
		assertEquals("TagGroups should be equivalent", tg1, tg2);

		tg2 = TagGroup.getTagGroup(as.getAccountById("111111111234"), Region.US_EAST_1, null, ps.getProductByName("RDS"), Operation.getOperation("CreateDBInstance"), UsageType.getUsageType("RDS:GP2-Storage", "GB"), null);
		assertEquals("TagGroups with alias should be equivalent", tg1, tg2);
		
		Product p = ps.getProductByName(Product.rdsFull);
		tg2 = TagGroup.getTagGroup(as.getAccountById("111111111234"), Region.US_EAST_1, null, p, Operation.getOperation("CreateDBInstance"), UsageType.getUsageType("RDS:GP2-Storage", "GB"), null);
		assertEquals("Product tags should be equivalent", System.identityHashCode(tg1.product), System.identityHashCode(p));
		assertEquals("Product tags should be equivalent", tg1.product, p);
		assertEquals("TagGroups with alias made with new product object should be equivalent", tg1, tg2);		
	}
	
	@Test
	public void testEquals() {
		AccountService as = new BasicAccountService(new Properties());
		TagGroup tg1 = TagGroup.getTagGroup(as.getAccountById("111111111345"), Region.US_EAST_1, null, ps.getProductByAwsName("Amazon Relational Food Service"), Operation.getOperation("CreateDBInstance"), UsageType.getUsageType("RDS:GP2-Storage", "GB"), null);
		TagGroup tg2 = TagGroup.getTagGroup(as.getAccountById("111111111345"), Region.US_EAST_1, null, ps.getProductByAwsName("Amazon Relational Food Service"), Operation.getOperation("CreateDBInstance"), UsageType.getUsageType("RDS:GP2-Storage", "GB"), null);
		assertTrue("Should be equal", tg1 == tg2);
		assertEquals("Should be equal", tg1, tg1);
		assertEquals("Should be equal", tg1, tg2);
		
		TagGroup tga = TagGroup.getTagGroup(as.getAccountById("111111111345"), Region.US_EAST_1, null, ps.getProductByName("Data Transfer"), Operation.getOperation("PublicIP-Out"), UsageType.getUsageType("USW2-AWS-Out-Bytes", "GB"), null);
		TagGroup tgb = TagGroup.getTagGroup(as.getAccountById("111111111345"), Region.US_EAST_1, null, ps.getProductByName("Data Transfer"), Operation.getOperation("PublicIP-Out"), UsageType.getUsageType("USW1-AWS-Out-Bytes", "GB"), null);
		assertFalse("Should not be equal", tga.equals(tgb));
	}

}
