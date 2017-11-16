package com.netflix.ice.common;

import static org.junit.Assert.*;

import java.util.Map;
import java.util.Properties;

import org.junit.Test;

import com.google.common.collect.Maps;
import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.Zone;

public class InstanceTest {

	@Test
	public void testSerializer() {
		Map<String, String> tags = Maps.newHashMap();
		tags.put("Environment", "prod");
		AccountService as = new BasicAccountService(new Properties());
		
				
		Instance i = new Instance("i-17f85eef87efb7a53", "c4.2xlarge", as.getAccountById("123456789012"), Region.US_EAST_1, Zone.US_EAST_1A, tags);
		String asString = i.serialize();
		String expected = "i-17f85eef87efb7a53,c4.2xlarge,123456789012,123456789012,us-east-1,us-east-1a,Environment=prod\n";
		assertEquals("serialized form wrong", expected, asString);
		
		Instance got = Instance.deserialize(asString, as);
		
		assertEquals("IDs don't match", i.id, got.id);
		assertEquals("types don't match", i.type, got.type);
		assertEquals("accounts don't match", i.account, got.account);
		assertEquals("regions don't match", i.region, got.region);
		assertEquals("zones don't match", i.zone, got.zone);
		assertEquals("tags size is wrong", i.tags.size(), got.tags.size());
		assertEquals("Environment tag is wrong", "prod", got.tags.get("Environment"));
	}
	
	@Test
	public void testTagWithCommas() {
		Map<String, String> tags = Maps.newHashMap();
		String tagValue = "= I have equal signs, and a comma =";
		tags.put("Name", tagValue);
		AccountService as = new BasicAccountService(new Properties());
		
				
		Instance i = new Instance("i-17f85eef87efb7a53", "c4.2xlarge", as.getAccountById("123456789012"), Region.US_EAST_1, Zone.US_EAST_1A, tags);
		String asString = i.serialize();
		String expected = "i-17f85eef87efb7a53,c4.2xlarge,123456789012,123456789012,us-east-1,us-east-1a,\"Name=" + tagValue + "\"\n";
		assertEquals("serialized form wrong", expected, asString);
		
		Instance got = Instance.deserialize(asString, as);
		assertEquals("IDs don't match", i.id, got.id);
		assertEquals("types don't match", i.type, got.type);
		assertEquals("accounts don't match", i.account, got.account);
		assertEquals("regions don't match", i.region, got.region);
		assertEquals("zones don't match", i.zone, got.zone);
		assertEquals("tags size is wrong", i.tags.size(), got.tags.size());
		assertEquals("Name tag is wrong", tagValue, got.tags.get("Name"));
	}
	
	@Test
	public void testMultipleTags() {
		Map<String, String> tags = Maps.newHashMap();
		tags.put("Environment", "prod");
		tags.put("Email", "foo@bar.com");
		tags.put("Name", "= I have equal signs, and a comma =");
		AccountService as = new BasicAccountService(new Properties());
		
				
		Instance i = new Instance("i-17f85eef87efb7a53", "c4.2xlarge", as.getAccountById("123456789012"), Region.US_EAST_1, Zone.US_EAST_1A, tags);
		String asString = i.serialize();
		Instance got = Instance.deserialize(asString, as);
		
		assertEquals("IDs don't match", i.id, got.id);
		assertEquals("types don't match", i.type, got.type);
		assertEquals("accounts don't match", i.account, got.account);
		assertEquals("regions don't match", i.region, got.region);
		assertEquals("zones don't match", i.zone, got.zone);
		assertEquals("tags size is wrong", i.tags.size(), got.tags.size());
		assertEquals("Environment tag is wrong", "prod", got.tags.get("Environment"));
		assertEquals("Email tag is wrong", "foo@bar.com", got.tags.get("Email"));
	}

	@Test
	public void testNoTags() {
		Map<String, String> tags = Maps.newHashMap();
		AccountService as = new BasicAccountService(new Properties());
		
				
		Instance i = new Instance("i-17f85eef87efb7a53", "c4.2xlarge", as.getAccountById("123456789012"), Region.US_EAST_1, Zone.US_EAST_1A, tags);
		String asString = i.serialize();
		Instance got = Instance.deserialize(asString, as);
		assertEquals("tags size is wrong", i.tags.size(), got.tags.size());
	}
	
	@Test
	public void testNoZone() {
		Map<String, String> tags = Maps.newHashMap();
		tags.put("Environment", "prod");
		AccountService as = new BasicAccountService(new Properties());
		
				
		Instance i = new Instance("i-17f85eef87efb7a53", "c4.2xlarge", as.getAccountById("123456789012"), Region.US_EAST_1, null, tags);
		String asString = i.serialize();
		Instance got = Instance.deserialize(asString, as);
		assertEquals("tags size is wrong", i.tags.size(), got.tags.size());
	}

	@Test
	public void testNoTagsNoZone() {
		Map<String, String> tags = Maps.newHashMap();
		AccountService as = new BasicAccountService(new Properties());
		
				
		Instance i = new Instance("i-17f85eef87efb7a53", "c4.2xlarge", as.getAccountById("123456789012"), Region.US_EAST_1, null, tags);
		String asString = i.serialize();
		Instance got = Instance.deserialize(asString, as);
		assertEquals("tags size is wrong", i.tags.size(), got.tags.size());
	}
}
