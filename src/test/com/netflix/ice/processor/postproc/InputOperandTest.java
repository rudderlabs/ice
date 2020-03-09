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
package com.netflix.ice.processor.postproc;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.common.AggregationTagGroup;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.postproc.OperandConfig.OperandType;
import com.netflix.ice.tag.Account;

public class InputOperandTest {
	static private BasicAccountService as;
	static private BasicProductService ps;
	
	@BeforeClass
	public static void init() {
		List<Account> accts = Lists.newArrayList();
		accts.add(new Account("123456789012", "Account1", null));
		accts.add(new Account("234567890123", "Account2", null));
		as = new BasicAccountService(accts);
		ps = new BasicProductService();
	}
	
	@Test
	public void testEmptyOperand() throws Exception {
		OperandConfig oc = new OperandConfig();
		InputOperand io = new InputOperand(oc, as);
		
		TagGroup tg = TagGroup.getTagGroup("123456789012", "us-east-1", null, "IOTestProduct", "OP1", "UT1", "", null, as, ps);
		AggregationTagGroup atg = io.aggregateTagGroup(tg, as, ps);
		assertTrue("tg should match", io.matches(atg, tg));
	}

	@Test
	public void testNonEmptyOperand() throws Exception {
		OperandConfig oc = new OperandConfig();
		oc.setAccount("123456789012");
		InputOperand io = new InputOperand(oc, as);
		
		TagGroup tg = TagGroup.getTagGroup("123456789012", "us-east-1", null, "IOTestProduct", "OP1", "UT1", "", null, as, ps);
		AggregationTagGroup atg = io.aggregateTagGroup(tg, as, ps);
		assertTrue("tg should match", io.matches(atg, tg));
		
		oc.setAccount("234567890123");
		io = new InputOperand(oc, as);
		assertNull("should not have an aggregationTagGroup", io.aggregateTagGroup(tg, as, ps));
		oc.setAccount(null);
		
		
		List<String> accounts = Lists.newArrayList(new String[]{"234567890123"});
		List<String> exclude = Lists.newArrayList(new String[]{"Account"});
		oc.setAccounts(accounts);
		oc.setExclude(exclude);
		io = new InputOperand(oc, as);
		atg = io.aggregateTagGroup(tg, as, ps);
		assertTrue("tg should match", io.matches(atg, tg));		
		TagGroup tg1 = TagGroup.getTagGroup("234567890123", "us-east-1", null, "IOTestProduct", "OP1", "UT1", "", null, as, ps);
		assertFalse("tg should not match", io.matches(atg, tg1));
	}
	
	@Test
	public void testOperandCacheKey() throws Exception {
		// Set up the in operand
		OperandConfig inOperand = new OperandConfig();
		inOperand.setType(OperandType.usage);
		InputOperand in = new InputOperand(inOperand, as);		
		TagGroup tg = TagGroup.getTagGroup("123456789012", "us-east-1", null, "IOTestProduct", "OP1", "UT1", "", "tag1|tag2", as, ps);		
		AggregationTagGroup atg = in.aggregateTagGroup(tg, as, ps);
		
		/*
		 * Test case where operand depends on input operand for all its tags
		 */
		OperandConfig oc = new OperandConfig();
		oc.setType(OperandType.usage);
		InputOperand io = new InputOperand(oc, as);
		String key = io.cacheKey(atg);
		
		assertEquals("wrong operand cache key with no aggregation", "usage,123456789012,us-east-1,null,IOTestProduct,OP1,UT1,tag1|tag2", key);
		
		/*
		 * Test case where operand groups by five tag types
		 */
		List<String> groupBy = Lists.newArrayList(new String[]{"Account","Zone","Operation","UsageType","ResourceGroup"});
		oc.setGroupBy(groupBy);
		io = new InputOperand(oc, as);
		key = io.cacheKey(atg);
		
		assertEquals("wrong operand cache key with aggregation", "usage,123456789012,null,OP1,UT1,tag1|tag2", key);
		
		/*
		 * Test case where operand gets all it's tags from the config
		 */
		oc.setGroupBy(null);
		oc.setAccount("123456789012");
		oc.setRegion("us-east-1");
		oc.setZone(null);
		oc.setProduct("IOTestProduct");
		oc.setOperation("OP1");
		oc.setUsageType("UT1");
		io = new InputOperand(oc, as);
		key = io.cacheKey(atg);
		
		assertEquals("wrong operand cache key with no aggregation or atg dependencies", "usage,123456789012,us-east-1,null,IOTestProduct,OP1,UT1,tag1|tag2", key);		
	}
	
	@Test
	public void testOperandMatches() throws Exception {
		// Set up the in operand
		OperandConfig inOperand = new OperandConfig();
		inOperand.setType(OperandType.usage);
		InputOperand in = new InputOperand(inOperand, as);		
		TagGroup tg = TagGroup.getTagGroup("123456789012", "us-east-1", null, "IOTestProduct", "OP1", "UT1", "", "tag1", as, ps);		
		AggregationTagGroup atg = in.aggregateTagGroup(tg, as, ps);

		// Test case where operand depends on 'in' operand for all its tag types
		OperandConfig oc = new OperandConfig();
		oc.setType(OperandType.cost);
		InputOperand io = new InputOperand(oc, as);
		assertTrue("TagGroup should match operand that depends on 'in'", io.matches(atg, tg));
		
		
		// Test case where operand is aggregating everything
		List<String> groupBy = Lists.newArrayList(new String[]{});
		oc = new OperandConfig();
		oc.setType(OperandType.cost);
		oc.setGroupBy(groupBy);
		io = new InputOperand(oc, as);
		TagGroup tg1 = TagGroup.getTagGroup("123456789012", "us-west-2", null, "IOTestProduct", "OP2", "UT2", "", "tag2", as, ps);		
		assertTrue("TagGroup should match all aggregation operand", io.matches(atg, tg1));
		
		// Test case where aggregating all accounts, but tag group account is different
		groupBy = Lists.newArrayList(new String[]{"Region","Zone","Product","Operation","UsageType","ResourceGroup"});
		oc = new OperandConfig();
		oc.setType(OperandType.cost);
		oc.setGroupBy(groupBy);
		io = new InputOperand(oc, as);
		tg1 = TagGroup.getTagGroup("234567890123", "us-east-1", null, "IOTestProduct", "OP1", "UT1", "", "tag1", as, ps);		
		assertTrue("TagGroup should match account aggregation operand with different account", io.matches(atg, tg1));
	}
	
	@Test
	public void testOperandMatchesWithOmmittedValue() throws Exception {
		// Set up the in operand
		OperandConfig inOperand = new OperandConfig();
		inOperand.setType(OperandType.usage);
		inOperand.setProduct("(?!IOTestProduct$)^.*$");
		inOperand.setOperation("(?!OP$)^.*$");
		inOperand.setUsageType("(?!UT$)^.*$");
		List<String> groupBy = Lists.newArrayList(new String[]{"Account","Region","Zone","ResourceGroup"});
		inOperand.setGroupBy(groupBy);
		InputOperand in = new InputOperand(inOperand, as);		
		TagGroup tg = TagGroup.getTagGroup("123456789012", "us-east-1", null, "IOTestProduct1", "OP1", "UT1", "", null, as, ps);		
		AggregationTagGroup atg = in.aggregateTagGroup(tg, as, ps);

		// Test case where omitting a product based on regex with no dependency on 'in' aggregation tag group
		OperandConfig oc = new OperandConfig();
		oc.setType(OperandType.cost);
		oc.setProduct("(?!IOTestProduct$)^.*$");
		oc.setOperation("(?!OP$)^.*$");
		oc.setUsageType("(?!UT$)^.*$");
		InputOperand io = new InputOperand(oc, as);
		TagGroup tg1 = TagGroup.getTagGroup("123456789012", "us-east-1", null, "IOTestProduct", "OP1", "UT1", "", null, as, ps);		
		assertFalse("TagGroup should not match product aggregation operand with ommitted product", io.matches(atg, tg1));
		
		tg1 = TagGroup.getTagGroup("123456789012", "us-east-1", null, "IOTestProduct1", "OP", "UT1", "", null, as, ps);		
		assertFalse("TagGroup should not match product aggregation operand with ommitted operation", io.matches(atg, tg1));
		
		tg1 = TagGroup.getTagGroup("123456789012", "us-east-1", null, "IOTestProduct1", "OP1", "UT", "", null, as, ps);		
		assertFalse("TagGroup should not match product aggregation operand with ommitted usageType", io.matches(atg, tg1));		
	}

}
