package com.netflix.ice.processor.config;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Test;

import com.amazonaws.services.organizations.model.Tag;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class AccountConfigTest {

	@Test
	public void testContructor() {
		List<String> customTags = Lists.newArrayList("Tag1");
		
		Map<String, String> accountTags = Maps.newHashMap();
		accountTags.put("Tag1", "tag1value");
		accountTags.put("IceName", "act1");
		accountTags.put("IceRole", "ice");
		accountTags.put("IceRiProducts", "ec2+rds");
		accountTags.put("IceExternalId", "12345");
		
		List<com.amazonaws.services.organizations.model.Tag> tags = Lists.newArrayList();
		for (Entry<String, String> e: accountTags.entrySet()) {
			Tag tag = new Tag();
			tag.setKey(e.getKey());
			tag.setValue(e.getValue());
			tags.add(tag);
		}
		
		AccountConfig account = new AccountConfig("123456789012", "account1", tags, customTags);
		
		assertEquals("Wrong account id", "123456789012", account.id);
		assertEquals("Wrong account name", "act1", account.name);
		assertEquals("Wrong account awsName", "account1", account.awsName);
		assertEquals("Wrong number of account tags", 1, account.tags.size());
		assertTrue("Map doesn't have tag", account.tags.containsKey("Tag1"));
		String tag = account.tags.get("Tag1");
		assertEquals("Wrong tag value", "tag1value", tag);
		assertEquals("Wrong number of RI Products", 2, account.riProducts.size());
		assertEquals("Wrong RI Product", "ec2", account.riProducts.get(0));
		assertEquals("Wrong role", "ice", account.role);
		assertEquals("Wrong externalId", "12345", account.externalId);
		
		String s = account.toString();
		assertEquals("Wrong string form", "id: 123456789012, name: act1, awsName: account1, riProducts: [ec2, rds], role: ice, externalId: 12345, tags: {Tag1: tag1value}", s);
	}

}
