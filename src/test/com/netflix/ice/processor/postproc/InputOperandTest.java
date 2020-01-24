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

}
