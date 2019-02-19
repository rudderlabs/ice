package com.netflix.ice.common;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.netflix.ice.tag.Account;

public class WorkBucketDataConfigTest {
    protected Logger logger = LoggerFactory.getLogger(getClass());

	@Test
	public void testWorkBucketDataConfig() {
		String startMonth = "2019-01";
		List<Account> accounts = Lists.newArrayList();
		accounts.add(new Account("123456789012", "Acct1"));
		accounts.add(new Account("234567890123", "Acct2"));
		
		List<String> userTags = Lists.newArrayList();
		userTags.add("Tag1");
		userTags.add("Tag2");
		userTags.add("Tag3");
		
		WorkBucketDataConfig wbdc = new WorkBucketDataConfig(startMonth, accounts, userTags, false);
		
		String json = wbdc.toJSON();
		WorkBucketDataConfig got = new WorkBucketDataConfig(json);
		
		assertEquals("Bad date string", startMonth, got.getStartMonth());
		assertEquals("Bad accounts", accounts, got.getAccounts());
		assertEquals("Bad user tags", userTags, got.getUserTags());
		assertEquals("Bad familyRiBreakout value", false, got.getFamilyRiBreakout());
	}

}
