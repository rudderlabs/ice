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

public class ConsolidatedOperationTest {

	@Test
	public void testGetConsolidatedName() {
		class Test {
			public final String in;
			public final String out;
			public final Integer seq;
			
			Test(String in, String out, Integer seq) {
				this.in = in;
				this.out = out;
				this.seq = seq;
			}
		}
		Test[] tests = new Test[]{
				new Test("Used RIs - No Upfront", "RIs", 3),
				new Test("Amortized RIs - No Upfront", "Amortized RIs", 4),
				new Test("Borrowed RIs - No Upfront", "RIs", 3),
				new Test("Unused RIs - No Upfront", "Unused RIs", 5),
				new Test("Bonus RIs - No Upfront", "RIs", 3),
				new Test("Savings - No Upfront", "Savings", 0),
				new Test("Savings - Spot", "Savings", 0),
				new Test("Spot Instances", "Spot Instances", 1),
				new Test("On-Demand Instances", "On-Demand Instances", 2),
				new Test("foobar", "foobar", Integer.MAX_VALUE),
		};
		
		for (Test t: tests) {
			ConsolidatedOperation tag = new ConsolidatedOperation(t.in);
			assertEquals("Bad consolidated name", t.out, tag.name);
			assertEquals("Bad consolidated sequence for " + t.in, t.seq, tag.getSeq());
		}
	}

}
