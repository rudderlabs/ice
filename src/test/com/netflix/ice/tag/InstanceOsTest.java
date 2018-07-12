package com.netflix.ice.tag;

import static org.junit.Assert.*;

import org.junit.Test;

public class InstanceOsTest {

	@Test
	public void testLinux() {
		InstanceOs ios = InstanceOs.withCode("");
		assertEquals("Incorrect InstanceOs for Linux instance", "", ios.usageType);
		assertFalse("Linux instance reported as spot", ios.isSpot);
	}
	
	@Test
	public void testLinuxSpot() {
		InstanceOs ios = InstanceOs.withCode(":SV001");
		assertEquals("Incorrect InstanceOs for Linux instance", "", ios.usageType);
		assertTrue("Linux instance reported as spot", ios.isSpot);
	}
	
	@Test
	public void testWindows() {
		InstanceOs ios = InstanceOs.withCode(":0002");
		assertEquals("Incorrect InstanceOs for Windows instance", ".windows", ios.usageType);
		assertFalse("Windows instance reported as spot", ios.isSpot);
	}
	
	@Test
	public void testWindowsSpot() {
		InstanceOs ios = InstanceOs.withCode(":0002:SV001");
		assertEquals("Incorrect InstanceOs for Windows Spot instance", ".windows", ios.usageType);
		assertTrue("Windows instance reported as spot", ios.isSpot);
	}

}
