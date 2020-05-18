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

import com.netflix.ice.tag.Zone.BadZone;

public class RegionTest {

	@Test
	public void testGetZone() throws BadZone {
		Zone zone = Region.US_WEST_2.getZone("us-west-2a");
		assertEquals("Wrong region", "us-west-2", zone.region.name);
		
		zone = Region.US_WEST_2.getZone("us-west-2");
		assertNull("Returned non-null zone", zone);
		
		zone = Region.EU_WEST_1.getZone("eu-west-1a");
		assertEquals("Wrong zone", "eu-west-1a", zone.name);
		assertEquals("Wrong region", "eu-west-1", zone.region.name);		
	}
	
	@Test(expected = BadZone.class)
	public void testBadZoneException() throws BadZone {
		Region.US_WEST_2.getZone("eu-west-1");		
	}
	
	@Test
	public void testRegionCompareTo() {
		assertTrue("aggregated should be before GLOBAL", Tag.aggregated.compareTo(Region.GLOBAL) < 0);
		assertTrue("aggregated should be before GLOBAL", Region.GLOBAL.compareTo(Tag.aggregated) > 0);
		assertTrue("GLOBAL should be equal to GLOBAL", Region.GLOBAL.compareTo(Region.GLOBAL) == 0);
		assertTrue("GLOBAL should be before ap-northeast-1", Region.GLOBAL.compareTo(Region.AP_NORTHEAST_1) < 0);
		assertTrue("GLOBAL should be before ap-northeast-1", Region.AP_NORTHEAST_1.compareTo(Region.GLOBAL) > 0);
		assertTrue("ap-northeast-1 should be equal to ap-northeast-1", Region.AP_NORTHEAST_1.compareTo(Region.AP_NORTHEAST_1) == 0);
	}
}
