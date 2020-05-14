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
    
    public static final Tag emptyTag = new Tag("") {
    	private static final long serialVersionUID = 1L;
    };
    
    public static final Tag nullTag = new Tag(null) {
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
		
		assertTrue("null tag not greater than aggregated", t.compareTo(nullTag) < 0);
		assertTrue("null tag not equal to empty tag", nullTag.compareTo(emptyTag) == 0);
		assertTrue("null tag not set to empty string", nullTag.name.isEmpty());		
	}

}
