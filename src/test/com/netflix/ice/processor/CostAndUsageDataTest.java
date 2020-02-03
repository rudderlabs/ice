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
package com.netflix.ice.processor;

import static org.junit.Assert.*;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.common.Config.TagCoverage;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.UsageType;

public class CostAndUsageDataTest {

	@Test
	public void testAddTagCoverage() {
		AccountService as = new BasicAccountService();
		ProductService ps = new BasicProductService();
		String[] userTags = new String[]{ "Email, Environment" };
		CostAndUsageData cau = new CostAndUsageData(0, null, Lists.newArrayList(userTags), TagCoverage.withUserTags, as, ps);
		
        TagGroup tagGroup = TagGroup.getTagGroup(as.getAccountById("123", ""), Region.US_WEST_2, null, ps.getProduct(Product.Code.S3), Operation.ondemandInstances, UsageType.getUsageType("c1.medium", "hours"), null);
		cau.addTagCoverage(null, 0, tagGroup, new boolean[]{true, false});
		
		ReadWriteTagCoverageData data = cau.getTagCoverage(null);
		
		TagCoverageMetrics tcm = data.getData(0).get(tagGroup);
		
		assertEquals("wrong metrics total", 1, tcm.total);
		assertEquals("wrong count on Email tag", 1, tcm.counts[0]);
		assertEquals("wrong count on Environment tag", 0, tcm.counts[1]);		
	}
	
}
