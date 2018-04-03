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
