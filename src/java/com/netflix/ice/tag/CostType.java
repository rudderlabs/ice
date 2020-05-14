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

public class CostType extends Tag {
	private static final long serialVersionUID = 1L;	
	private int seq;
	
    private static int sequence = 0;
    public static final CostType savings = new CostType("Savings");
    public static final CostType recurring = new CostType("Recurring");
    public static final CostType amortization = new CostType("Amortization");
    public static final CostType credits = new CostType("Credits");
    public static final CostType taxes = new CostType("Taxes");
    public static final CostType other = new CostType("Other");

	private CostType(String name) {
		super(name);
		seq = sequence++;
	}
	
	public static CostType getCostType(Operation op) {
		if (op.isRecurring())
			return recurring;
		else if (op.isSavings())
			return savings;
		else if (op.isAmortized())
			return amortization;
		else if (op.isCredit())
			return credits;
		else if (op.isTax())
			return taxes;
		else
			return other;
	}
	
    @Override
    public int compareTo(Tag t) {
        if (t instanceof CostType) {
        	CostType o = (CostType)t;
	        return this.seq - o.seq;
        }
        else {
        	return super.compareTo(t);
        }
    }

}
