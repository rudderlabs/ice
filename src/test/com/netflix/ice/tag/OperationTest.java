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
		assertEquals("wrong number of savings plan operations", 21, Operation.getSavingsPlanOperations(false).size());
	}
	
	@Test
	public void testGetOperations() {
		String op = "SavingsPlan Used - All Upfront";
		List<String> ops = Lists.newArrayList(op);
		assertEquals("missing operation", op, Operation.getOperations(ops).get(0).name);
	}
	
	@Test
	public void testTax() {
		Operation.getTaxOperation("VAT");
		List<String> opStrs = Lists.newArrayList("Tax - VAT");
		List<Operation> operations = Operation.getOperations(opStrs);
		assertEquals("did not find tax entry", 1, operations.size());
		assertEquals("wrong name for tax entry", "Tax - VAT", operations.get(0).name);
	}
	
	@Test
	public void testIdentity() {
		List<Operation> operations = Operation.getReservationOperations(false);
		List<Operation> excluded = Lists.newArrayList();
		List<Operation.Identity.Value> exclude = Lists.newArrayList(Operation.Identity.Value.Amortized);
		int bits = Operation.Identity.getIdentitySet(exclude);
		for (Operation op: operations) {
			if (op.isOneOf(bits))
				excluded.add(op);
		}
		assertEquals("wrong number of amortized items", 15, excluded.size());
		
		// Test tax
		Operation.getTaxOperation("VAT");
		List<String> opStrs = Lists.newArrayList("Tax - VAT");
		operations = Operation.getOperations(opStrs);
		exclude = Lists.newArrayList(Operation.Identity.Value.Tax);
		bits = Operation.Identity.getIdentitySet(exclude);
		excluded = Lists.newArrayList();
		for (Operation op: operations) {
			if (op.isOneOf(bits))
				excluded.add(op);
		}
		assertEquals("wrong number of tax items", 1, excluded.size());
		assertTrue("Tax not categorized as tax", excluded.get(0).isTax());
		assertFalse("Tax incorrectly categorized as recurring", excluded.get(0).isRecurring());
		
		// Test credit
		Operation.getCreditOperation("Foo");
		opStrs = Lists.newArrayList("Credit - Foo");
		operations = Operation.getOperations(opStrs);
		exclude = Lists.newArrayList(Operation.Identity.Value.Credit);
		bits = Operation.Identity.getIdentitySet(exclude);
		excluded = Lists.newArrayList();
		for (Operation op: operations) {
			if (op.isOneOf(bits))
				excluded.add(op);
		}
		assertEquals("wrong number of credit items", 1, excluded.size());
		assertTrue("Credit not categorized as credit", excluded.get(0).isCredit());
		assertFalse("Credit incorrectly categorized as recurring", excluded.get(0).isRecurring());
		assertFalse("Credit incorrectly categorized as tax", excluded.get(0).isTax());
		assertFalse("Credit incorrectly categorized as tax", excluded.get(0).isAmortized());
	}
	
	@Test
	public void testDeserializeCreditAndTax() {
		// Make sure we properly categorize tax and credit entries
		Operation taxOp = Operation.deserializeOperation("Tax - VAT");
		assertTrue("tax operation not categorized correctly", taxOp.isTax());
		assertEquals("tax operation has wrong name", "Tax - VAT", taxOp.name);
		
		Operation creditOp = Operation.deserializeOperation("Credit - Foo");
		assertTrue("credit operation not categorized correctly", creditOp.isCredit());
		assertEquals("credit operation has wrong name", "Credit - Foo", creditOp.name);
	}
	
	@Test
	public void testEmptyNameCreditAndTax() {
		Operation taxOp = Operation.getTaxOperation("");
		assertTrue("tax operation not categorized correctly", taxOp.isTax());
		assertEquals("tax operation has wrong name", "Tax - None", taxOp.name);
		
		Operation creditOp = Operation.getCreditOperation("");
		assertTrue("credit operation not categorized correctly", creditOp.isCredit());
		assertEquals("credit operation has wrong name", "Credit - None", creditOp.name);
	}
	
	
    public static final Operation AATag = Operation.getOperation("AA");
    public static final Operation BBTag = Operation.getOperation("BB");
    public static final Operation aaTag = Operation.getOperation("aa");
    public static final Operation bbTag = Operation.getOperation("bb");
    
	@Test
	public void testCompareTo() {		
		assertTrue("aa tag not less than bb", aaTag.compareTo(bbTag) < 0);
		assertTrue("bb tag not greater than aa", bbTag.compareTo(aaTag) > 0);
		assertTrue("AA tag not less than aa", AATag.compareTo(aaTag) < 0);
		assertTrue("aa tag not greater than AA", aaTag.compareTo(AATag) > 0);
		assertTrue("aa tag not less than BB", aaTag.compareTo(BBTag) < 0);
		assertTrue("BB tag not greater than aa", BBTag.compareTo(aaTag) > 0);
	}

}
