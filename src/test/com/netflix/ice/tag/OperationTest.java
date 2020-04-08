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

import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;

public class OperationTest {

	@Test
	public void testIsSavingsPlanBonus() {
		assertTrue(Operation.savingsPlanBonusNoUpfront.isBonus());
	}

	@Test
	public void testGetSavingsPlanOperations() {
		assertEquals("wrong number of savings plan operations", 26, Operation.getSavingsPlanOperations().size());
	}
	
	@Test
	public void testGetOperations() {
		String op = "SavingsPlan Used - All Upfront";
		List<String> ops = Lists.newArrayList(op);
		assertEquals("missing operation", op, Operation.getOperations(ops).get(0).name);
	}
}
