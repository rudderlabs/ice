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

public class FamilyTagTest {

	@Test
	public void testGetFamilyName() {
		class test {
			public String instance;
			public String family;
			public String error;
			
			public test(String instance, String family, String error) {
				this.instance = instance;
				this.family = family;
				this.error = error;
			}
		}
		test[] tests = new test[]{
				new test("c3.xlarge", "c3", "Wrong linux family name"),
				new test("c3.xlarge.windows", "c3.xlarge.windows", "Wrong windows family name"),
				new test("db.m1.large.mysql", "db.m1.mysql", "Wrong family name for DB instance"),
				new test("db.m1.large.multiaz.mysql", "db.m1.mysql", "Wrong family name for DB instance"),
				new test("db.m1.large.sqlstd", "db.m1.large.sqlstd", "Wrong family name for DB instance"),
		};
		
		for (test t: tests) {
			String got = FamilyTag.getFamilyName(t.instance);
			assertTrue(t.error + ", expected " + t.family + ", got " + got, got.equals(t.family));
		}
	}
}
