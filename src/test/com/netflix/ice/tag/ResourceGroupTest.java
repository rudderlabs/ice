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
		assertEquals("group with only second tag name incorrect", "|foo", rg.name);		
	}
}
