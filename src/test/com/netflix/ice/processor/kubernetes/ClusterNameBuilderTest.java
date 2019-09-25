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
package com.netflix.ice.processor.kubernetes;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.netflix.ice.tag.UserTag;

public class ClusterNameBuilderTest {

	@Test
	public void testNoFuncs() {
		String[] tags = new String[]{ "Tag1", "Tag2", "Tag3" };
		ClusterNameBuilder cnb = new ClusterNameBuilder(Lists.newArrayList("Tag3"), tags);
		UserTag[] userTags = new UserTag[]{ UserTag.get("One"), UserTag.get("Two"), UserTag.get("Three")};
		String name = cnb.getClusterNames(userTags).get(0);
		assertEquals("Rule with no functions failed", "Three", name);
	}
	
	@Test
	public void testLiteral() {
		ClusterNameBuilder cnb = new ClusterNameBuilder(Lists.newArrayList("\"foobar\""), null);
		String name = cnb.getClusterNames(null).get(0);
		assertEquals("ToUpper failed", "foobar", name);
	}
	
	@Test
	public void testToUpper() {
		String[] tags = new String[]{ "Tag1", "Tag2", "Tag3" };
		
		ClusterNameBuilder cnb = new ClusterNameBuilder(Lists.newArrayList("Tag2.toUpper()"), tags);
		UserTag[] userTags = new UserTag[]{ UserTag.get("One"), UserTag.get("Two"), UserTag.get("Three")};
		String name = cnb.getClusterNames(userTags).get(0);
		assertEquals("ToUpper failed", "TWO", name);
	}

	@Test
	public void testToLower() {
		String[] tags = new String[]{ "Tag1", "Tag2", "Tag3" };
		
		ClusterNameBuilder cnb = new ClusterNameBuilder(Lists.newArrayList("Tag2.toLower()"), tags);
		UserTag[] userTags = new UserTag[]{ UserTag.get("One"), UserTag.get("Two"), UserTag.get("Three")};
		String name = cnb.getClusterNames(userTags).get(0);
		assertEquals("ToLower failed", "two", name);
	}

	@Test
	public void testRegex() {
		String[] tags = new String[]{ "Tag1", "Tag2", "Tag3" };
		
		ClusterNameBuilder cnb = new ClusterNameBuilder(Lists.newArrayList("Tag2.regex(\"Stripme-(.*)\")"), tags);
		UserTag[] userTags = new UserTag[]{ UserTag.get("One"), UserTag.get("Stripme-Two"), UserTag.get("Three")};
		String name = cnb.getClusterNames(userTags).get(0);
		assertEquals("Regex failed", "Two", name);
	}

	@Test
	public void testRegexWithToLower() {
		String[] tags = new String[]{ "Tag1", "Tag2", "Tag3" };
		
		ClusterNameBuilder cnb = new ClusterNameBuilder(Lists.newArrayList("Tag2.regex(\"Stripme-(.*)\").toLower()"), tags);
		UserTag[] userTags = new UserTag[]{ UserTag.get("One"), UserTag.get("Stripme-Two"), UserTag.get("Three")};
		String name = cnb.getClusterNames(userTags).get(0);
		assertEquals("Regex failed", "two", name);
	}
	
	@Test
	public void testMultipleTagRules() {
		String[] tags = new String[]{ "Tag1", "Tag2", "Tag3" };
		
		ClusterNameBuilder cnb = new ClusterNameBuilder(Lists.newArrayList("Tag1.toLower()+Tag2.regex(\"Stripme(-.*)\")"), tags);
		UserTag[] userTags = new UserTag[]{ UserTag.get("One"), UserTag.get("Stripme-Two"), UserTag.get("Three")};
		String name = cnb.getClusterNames(userTags).get(0);
		assertEquals("Regex failed", "one-Two", name);
	}
	
	@Test
	public void testEmptyTags() {
		String[] tags = new String[]{ "Tag1", "Tag2", "Tag3" };
		
		ClusterNameBuilder cnb = new ClusterNameBuilder(Lists.newArrayList("Tag2.toUpper()"), tags);
		UserTag[] userTags = new UserTag[]{ null, null, null};
		assertEquals("Should not return any cluster names", 0, cnb.getClusterNames(userTags).size());
	}

	@Test
	public void testMultipleFormulae() {
		String[] tags = new String[]{ "Tag1", "Tag2", "Tag3" };
		String[] formulae = new String[]{ "Tag1.toLower()+Tag2.regex(\"Stripme(-.*)\")", "Tag3.regex(\"k8s-(.*)\")" };
		ClusterNameBuilder cnb = new ClusterNameBuilder(Lists.newArrayList(formulae), tags);
		UserTag[] userTags = new UserTag[]{ UserTag.get("One"), UserTag.get("Stripme-Two"), UserTag.get("k8s-Three")};
		List<String> names = cnb.getClusterNames(userTags);
		assertEquals("Wrong number of cluster names", 2, names.size());
		assertEquals("Regex failed", "one-Two", names.get(0));
		assertEquals("Regex failed", "Three", names.get(1));
	}
}
