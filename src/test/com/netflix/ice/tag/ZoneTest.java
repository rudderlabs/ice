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

public class ZoneTest {

	@Test
	public void testGetZoneByName() {
		Zone zone = Zone.getZone("us-west-2a");
		assertEquals("Wrong region", "us-west-2", zone.region.name);
		
		zone = Zone.getZone("us-west-2");
		assertEquals("Returned non-null zone", null, zone);
	}
	
	@Test
	public void testGetZoneByNameAndRegion() {
		Zone zone = Zone.getZone("eu-west-1", Region.US_WEST_2);
		assertEquals("Returned non-null zone", null, zone);
		
		zone = Zone.getZone("eu-west-1", Region.EU_WEST_1);
		assertEquals("Returned non-null zone", null, zone);
		
		zone = Zone.getZone("eu-west-1a", Region.EU_WEST_1);
		assertEquals("Wrong zone", "eu-west-1a", zone.name);
		assertEquals("Wrong region", "eu-west-1", zone.region.name);
		
	}

}
