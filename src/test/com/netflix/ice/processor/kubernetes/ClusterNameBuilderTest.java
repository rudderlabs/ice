package com.netflix.ice.processor.kubernetes;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import com.netflix.ice.tag.UserTag;

public class ClusterNameBuilderTest {

	@Test
	public void testNoFuncs() {
		String[] tags = new String[]{ "Tag1", "Tag2", "Tag3" };
		ClusterNameBuilder cnb = new ClusterNameBuilder("Tag3", tags);
		UserTag[] userTags = new UserTag[]{ UserTag.get("One"), UserTag.get("Two"), UserTag.get("Three")};
		String name = cnb.getClusterNames(userTags).get(0);
		assertEquals("Rule with no functions failed", "Three", name);
	}
	
	@Test
	public void testLiteral() {
		ClusterNameBuilder cnb = new ClusterNameBuilder("\"foobar\"", null);
		String name = cnb.getClusterNames(null).get(0);
		assertEquals("ToUpper failed", "foobar", name);
	}
	
	@Test
	public void testToUpper() {
		String[] tags = new String[]{ "Tag1", "Tag2", "Tag3" };
		
		ClusterNameBuilder cnb = new ClusterNameBuilder("Tag2.toUpper()", tags);
		UserTag[] userTags = new UserTag[]{ UserTag.get("One"), UserTag.get("Two"), UserTag.get("Three")};
		String name = cnb.getClusterNames(userTags).get(0);
		assertEquals("ToUpper failed", "TWO", name);
	}

	@Test
	public void testToLower() {
		String[] tags = new String[]{ "Tag1", "Tag2", "Tag3" };
		
		ClusterNameBuilder cnb = new ClusterNameBuilder("Tag2.toLower()", tags);
		UserTag[] userTags = new UserTag[]{ UserTag.get("One"), UserTag.get("Two"), UserTag.get("Three")};
		String name = cnb.getClusterNames(userTags).get(0);
		assertEquals("ToLower failed", "two", name);
	}

	@Test
	public void testRegex() {
		String[] tags = new String[]{ "Tag1", "Tag2", "Tag3" };
		
		ClusterNameBuilder cnb = new ClusterNameBuilder("Tag2.regex(\"Stripme-(.*)\")", tags);
		UserTag[] userTags = new UserTag[]{ UserTag.get("One"), UserTag.get("Stripme-Two"), UserTag.get("Three")};
		String name = cnb.getClusterNames(userTags).get(0);
		assertEquals("Regex failed", "Two", name);
	}

	@Test
	public void testRegexWithToLower() {
		String[] tags = new String[]{ "Tag1", "Tag2", "Tag3" };
		
		ClusterNameBuilder cnb = new ClusterNameBuilder("Tag2.regex(\"Stripme-(.*)\").toLower()", tags);
		UserTag[] userTags = new UserTag[]{ UserTag.get("One"), UserTag.get("Stripme-Two"), UserTag.get("Three")};
		String name = cnb.getClusterNames(userTags).get(0);
		assertEquals("Regex failed", "two", name);
	}
	
	@Test
	public void testMultipleTagRules() {
		String[] tags = new String[]{ "Tag1", "Tag2", "Tag3" };
		
		ClusterNameBuilder cnb = new ClusterNameBuilder("Tag1.toLower()+Tag2.regex(\"Stripme(-.*)\")", tags);
		UserTag[] userTags = new UserTag[]{ UserTag.get("One"), UserTag.get("Stripme-Two"), UserTag.get("Three")};
		String name = cnb.getClusterNames(userTags).get(0);
		assertEquals("Regex failed", "one-Two", name);
	}
	
	@Test
	public void testEmptyTags() {
		String[] tags = new String[]{ "Tag1", "Tag2", "Tag3" };
		
		ClusterNameBuilder cnb = new ClusterNameBuilder("Tag2.toUpper()", tags);
		UserTag[] userTags = new UserTag[]{ null, null, null};
		assertEquals("Should not return any cluster names", 0, cnb.getClusterNames(userTags).size());
	}

	@Test
	public void testMultipleFormulae() {
		String[] tags = new String[]{ "Tag1", "Tag2", "Tag3" };
		String[] formulae = new String[]{ "Tag1.toLower()+Tag2.regex(\"Stripme(-.*)\")", "Tag3.regex(\"k8s-(.*)\")" };
		ClusterNameBuilder cnb = new ClusterNameBuilder(String.join(",", formulae), tags);
		UserTag[] userTags = new UserTag[]{ UserTag.get("One"), UserTag.get("Stripme-Two"), UserTag.get("k8s-Three")};
		List<String> names = cnb.getClusterNames(userTags);
		assertEquals("Wrong number of cluster names", 2, names.size());
		assertEquals("Regex failed", "one-Two", names.get(0));
		assertEquals("Regex failed", "Three", names.get(1));
	}
}
