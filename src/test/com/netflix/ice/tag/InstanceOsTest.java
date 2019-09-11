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
