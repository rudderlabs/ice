package com.netflix.ice.tag;

import static org.junit.Assert.*;

import org.junit.Test;

public class TagTest {

    public static final Tag testTag = new Tag("test") {
		private static final long serialVersionUID = 1L;
    };
    
    public static final Tag aaaTag = new Tag("aaa") {
		private static final long serialVersionUID = 1L;
    };
    
	@Test
	public void testCompareTo() {
		Tag t = Tag.aggregated;
		assertEquals("aggregated not equal to itself", 0, t.compareTo(t));
		assertTrue("aggregated not less than test tag", t.compareTo(testTag) < 0);
		assertTrue("test tag not greater than aggregated", testTag.compareTo(t) > 0);
		
		assertEquals("aaa not equal to itself", 0, aaaTag.compareTo(aaaTag));
		assertTrue("aggregated not less than test tag", t.compareTo(aaaTag) < 0);
		assertTrue("test tag not greater than aggregated", aaaTag.compareTo(t) > 0);
	}

}
