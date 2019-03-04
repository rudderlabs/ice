package com.netflix.ice.processor.kubernetes;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.basic.BasicResourceService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.processor.kubernetes.Tagger.TaggerConfig;
import com.netflix.ice.tag.UserTag;

public class TaggerTest {
	private ResourceService rs;
	private ProductService ps;
	private String[] customTags = new String[]{"Tag1", "Tag2", "Tag3"};
	
	class TestKubernetesReport extends KubernetesReport {

		public TestKubernetesReport(DateTime month, String[] userTags) {
			super(null, null, null, null, null, null, month, userTags);
		}
		
		@Override
		public String getString(String[] item, KubernetesColumn col) {
			if (col != KubernetesColumn.Namespace)
				throw new IllegalArgumentException("expected Namespace, got " + col);
			return item[1];
		}
		
		@Override
		public String getUserTag(String[] item, String col) {
			if (!col.equals("Tag3"))
				throw new IllegalArgumentException("expected Tag3, got " + col);
			return item[2];
		}
	}
	
	@Before
	public void init() {
		ps = new BasicProductService(null);
		Map<String, List<String>> tagKeys = Maps.newHashMap();
		Map<String, List<String>> tagValues = Maps.newHashMap();
		rs = new BasicResourceService(ps, customTags, new String[]{}, tagKeys, tagValues, null);
	}


	@Test
	public void testTagger() throws IOException {
		TestKubernetesReport tkr = new TestKubernetesReport(new DateTime("2019-01", DateTimeZone.UTC), customTags);
		String[] item = new String[]{ "dev-usw2a", "bar", "foobar" };
		
		String[] tagsToCopy = new String[]{ "Tag3" };
		Map<String, String> rules = Maps.newHashMap();
		rules.put("Tag2.Foo", "bar");
		rules.put("Tag1.Bar", ".*bar.*");
		Tagger t = new Tagger(tagsToCopy, rules, rs, null, null, null);
		UserTag[] userTags = new UserTag[customTags.length];
		t.tag(tkr, item, userTags);
		assertEquals("Incorrect tagged value", "Bar", userTags[0].name);
		assertEquals("Incorrect tagged value", "Foo", userTags[1].name);
		assertEquals("Tag3 not copied", "foobar", userTags[2].name);
		
		item = new String[]{ "dev-usw2a", "inAbar", "" };
		userTags = new UserTag[customTags.length];
		userTags[2] = UserTag.get("FooBar");
		t.tag(tkr, item, userTags);
		assertEquals("Incorrect tagged value", "Bar", userTags[0].name);
		assertEquals("Wrong tag changed", null, userTags[1]);
		assertEquals("Tag3 was overwritten", "FooBar", userTags[2].name);
		
		item = new String[]{ "dev-usw2a", "inAbar", "useMe" };
		userTags = new UserTag[customTags.length];
		userTags[2] = UserTag.get("overwriteMe");
		t.tag(tkr, item, userTags);
		assertEquals("Tag3 was not overwritten", "useMe", userTags[2].name);
	}
	
	@Test
	public void testTaggerConfig() throws IOException {
		String[] tagsToCopy = new String[]{ "Tag3" };
		Map<String, String> rules = Maps.newHashMap();
		rules.put("Tag2.Foo", "bar");
		rules.put("Tag1.Bar", ".*bar.*");
		
		Tagger t = new Tagger(tagsToCopy, rules, rs, null, null, null);
		
		String json = t.config.toJSON();		
		TaggerConfig testConfig = t.new TaggerConfig(json);
		assertEquals("tagsToCopy doesn't match", String.join(",", t.config.getTagsToCopy()), String.join(",", testConfig.getTagsToCopy()));
		for (int i = 0; i < t.config.getRules().size(); i++) {
			Tagger.Rule orig = t.config.getRules().get(i);
			Tagger.Rule test = testConfig.getRules().get(i);
			assertEquals("tagName doesn't match", orig.getTagName(), test.getTagName());
			assertEquals("tagIndex doesn't match", orig.getTagIndex(), test.getTagIndex());
			assertEquals("value doesn't match", orig.getValue(), test.getValue());
			assertEquals("patterns doesn't match", orig.getPatterns(), test.getPatterns());			
		}
	}
}
