package com.netflix.ice.processor;

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.common.Config.TagCoverage;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.UsageType;

public class CostAndUsageDataTest {

	@Test
	public void testAddTagCoverage() {
		AccountService as = new BasicAccountService(new Properties());
		ProductService ps = new BasicProductService(new Properties());
		String[] userTags = new String[]{ "Email, Environment" };
		CostAndUsageData cau = new CostAndUsageData(Lists.newArrayList(userTags), TagCoverage.withUserTags);
		
        TagGroup tagGroup = TagGroup.getTagGroup(as.getAccountById("123"), Region.US_WEST_2, null, ps.getProductByName("S3"), Operation.ondemandInstances, UsageType.getUsageType("c1.medium", "hours"), null);
		cau.addTagCoverage(null, 0, tagGroup, new boolean[]{true, false});
		
		ReadWriteTagCoverageData data = cau.getTagCoverage(null);
		
		TagCoverageMetrics tcm = data.getData(0).get(tagGroup);
		
		assertEquals("wrong metrics total", 1, tcm.total);
		assertEquals("wrong count on Email tag", 1, tcm.counts[0]);
		assertEquals("wrong count on Environment tag", 0, tcm.counts[1]);		
	}
	
}
