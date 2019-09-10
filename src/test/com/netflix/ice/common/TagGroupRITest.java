package com.netflix.ice.common;

import static org.junit.Assert.*;

import org.junit.Test;

import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.UsageType;

public class TagGroupRITest {

	@Test
	public void testCompareTo() {
		AccountService as = new BasicAccountService();
		ProductService ps = new BasicProductService();
		
		TagGroup tg1 = TagGroup.getTagGroup(
				as.getAccountById("111111111234"),
				Region.US_EAST_1, 
				null, 
				ps.getProduct("AWS Relational Database Service", "AmazonRDS"), 
				Operation.getOperation("CreateDBInstance"), 
				UsageType.getUsageType("RDS:GP2-Storage", "GB"), 
				null);

		TagGroup tg2 = TagGroupRI.getTagGroup(
				as.getAccountById("111111111234"), 
				Region.US_EAST_1, 
				null, 
				ps.getProduct("AWS Relational Database Service", "AmazonRDS"), 
				Operation.getOperation("CreateDBInstance"), 
				UsageType.getUsageType("RDS:GP2-Storage", "GB"), 
				null,
				"arn");

		assertTrue("TagGroupRI should be greater than TagGroup", tg1.compareTo(tg2) < 0);
	}

}
