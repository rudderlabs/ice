package com.netflix.ice.tag;

import static org.junit.Assert.*;

import org.junit.Test;

public class ResourceGroupTest {

	@Test
	public void testGetResourceGroup() {
		ResourceGroup rg = ResourceGroup.getResourceGroup("foo", false);
		assertEquals("single name incorrect", "foo", rg.name);		
	
		rg = ResourceGroup.getResourceGroup("|foo", false);
		assertEquals("group with only second tag name incorrect", "|foo", rg.name);		
	}

	@Test
	public void testGetResourceGroupArray() {
		ResourceGroup rg = ResourceGroup.getResourceGroup(new String[]{"foo"});
		assertEquals("single name incorrect", "foo", rg.name);		
	
		rg = ResourceGroup.getResourceGroup(new String[]{"", "foo"});
		assertEquals("group with empty first tag incorrect", "|foo", rg.name);
		
		rg = ResourceGroup.getResourceGroup(new String[]{null, "foo"});
		assertEquals("group with null first tag incorrect", "|foo", rg.name);		
	}
	
	@Test
	public void testTagValueWithSeparator() {
		String[] tags = new String[]{"foo", "has|separator", "bar"};
		ResourceGroup rg = new ResourceGroup(tags);
		String[] got = new String[tags.length];
		String[] expect = new String[tags.length];
		UserTag[] ut = rg.getUserTags();
		for (int i = 0; i < ut.length; i++) {
			expect[i] = tags[i].replace(ResourceGroup.separator, ResourceGroup.separatorReplacement);
			got[i] = ut[i].name;
		}
			
		assertArrayEquals("incorrect user tags", expect, got);
	}
}
