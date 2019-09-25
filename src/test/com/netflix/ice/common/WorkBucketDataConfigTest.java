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
package com.netflix.ice.common;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.Config.TagCoverage;
import com.netflix.ice.tag.Account;

public class WorkBucketDataConfigTest {
    protected Logger logger = LoggerFactory.getLogger(getClass());

	@Test
	public void testWorkBucketDataConfig() {
		String startMonth = "2019-01";
		List<Account> accounts = Lists.newArrayList();
		accounts.add(new Account("123456789012", "Acct1"));
		accounts.add(new Account("234567890123", "Acct2"));
		
		List<String> zlist = Lists.newArrayList();
		zlist.add("us-east-1a");
		zlist.add("us-east-1b");
		zlist.add("us-east-1c");
		Map<String, List<String>> zones = Maps.newHashMap();
		zones.put("us-east-1", zlist);
		
		List<String> userTags = Lists.newArrayList();
		userTags.add("Tag1");
		userTags.add("Tag2");
		userTags.add("Tag3");
		
		WorkBucketDataConfig wbdc = new WorkBucketDataConfig(startMonth, accounts, zones, userTags, false, TagCoverage.basic);
		
		String json = wbdc.toJSON();
		WorkBucketDataConfig got = new WorkBucketDataConfig(json);
		
		assertEquals("Bad date string", startMonth, got.getStartMonth());
		assertEquals("Bad accounts", accounts, got.getAccounts());
		assertEquals("Bad zones", zones, got.getZones());
		assertEquals("Bad user tags", userTags, got.getUserTags());
		assertEquals("Bad familyRiBreakout value", false, got.getFamilyRiBreakout());
		assertEquals("Bad tagCoverage value", TagCoverage.basic, got.getTagCoverage());
	}

}
