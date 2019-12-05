package com.netflix.ice.processor.config;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.TagConfig;

public class TagConfigTest {

	@Test
	public void testConstructor() {
		String name = "Env";
		List<String> aliases = Lists.newArrayList(new String[]{"Environment"});
		Map<String, List<String>> values = Maps.newHashMap();
		List<String> prodAliases = Lists.newArrayList(new String[]{"production"});
		values.put("Prod", prodAliases);
		
		
		TagConfig tc = new TagConfig(name, aliases, values);
		assertEquals("wrong tag name", "Env", tc.name);
		assertEquals("wrong number of aliases", 1, tc.aliases.size());
		assertEquals("wrong alias", "Environment", tc.aliases.get(0));
		assertEquals("wrong number of values", 1, tc.values.size());
		assertTrue("wrong value name", tc.values.containsKey("Prod"));
		assertEquals("wrong number of value aliases", 1, tc.values.get("Prod").size());
		assertEquals("wrong value alias", "production", tc.values.get("Prod").get(0));
	}

	@Test
	public void testDeserializeFromYaml() throws JsonParseException, JsonMappingException, IOException {
		String yaml = "" +
		"name: Env\n" +
		"aliases: [Environment]\n" +
		"values:\n" +
		"  Prod: [production]\n" +
		"mapped:\n" +
		"  QA:\n" +
		"    Application: [test-web-server]\n";

		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		TagConfig tc = new TagConfig();
		tc = mapper.readValue(yaml, tc.getClass());		
		
		assertEquals("wrong tag name", "Env", tc.name);
		assertEquals("wrong number of aliases", 1, tc.aliases.size());
		assertEquals("wrong alias", "Environment", tc.aliases.get(0));
		assertEquals("wrong number of values", 1, tc.values.size());
		assertTrue("wrong value name", tc.values.containsKey("Prod"));
		assertEquals("wrong number of value aliases", 1, tc.values.get("Prod").size());
		assertEquals("wrong value alias", "production", tc.values.get("Prod").get(0));
		assertEquals("wrong number of computed values", 1, tc.mapped.size());
		assertTrue("wrong computed value name", tc.mapped.containsKey("QA"));
		assertEquals("wrong number of matches", 1, tc.mapped.get("QA").size());
		assertTrue("wrong computed value key", tc.mapped.get("QA").containsKey("Application"));
		assertEquals("wrong number of computed value key matches", 1, tc.mapped.get("QA").get("Application").size());
		assertEquals("wrong computed value mapped value", "test-web-server", tc.mapped.get("QA").get("Application").get(0));		
	}
}
