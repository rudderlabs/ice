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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.processor.ReservationService.ReservationUtilization;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

public class Operation extends Tag {

    /**
	 * 
	 */
	private static final long serialVersionUID = 2L;

	protected int seq = Integer.MAX_VALUE;
	
    private Operation (String name) {
        super(name);
    }
    
    private static ConcurrentMap<String, Operation> operations = Maps.newConcurrentMap();

    public static final ReservationOperation spotInstanceSavings = new ReservationOperation("Savings - Spot", 0, null);
    public static final ReservationOperation spotInstances = new ReservationOperation("Spot Instances", 1, null);
    public static final ReservationOperation ondemandInstances = new ReservationOperation("On-Demand Instances", 2, null);

    // Heavy and Fixed map to the new "No Upfront", "Partial Upfront", and "All Upfront" reservation types
    public static final ReservationOperation savingsHeavy = new ReservationOperation("Savings - No Upfront", 3, ReservationUtilization.HEAVY);
    public static final ReservationOperation reservedInstancesHeavy = new ReservationOperation("Used RIs - No Upfront", 4, ReservationUtilization.HEAVY);
    public static final ReservationOperation familyReservedInstancesHeavy = new ReservationOperation("Family RIs - No Upfront", 5, ReservationUtilization.HEAVY);
    public static final ReservationOperation bonusReservedInstancesHeavy = new ReservationOperation("Bonus RIs - No Upfront", 6, ReservationUtilization.HEAVY);
    public static final ReservationOperation borrowedInstancesHeavy = new ReservationOperation("Borrowed RIs - No Upfront", 7, ReservationUtilization.HEAVY);
    public static final ReservationOperation lentInstancesHeavy = new ReservationOperation("Lent RIs - No Upfront", 8, ReservationUtilization.HEAVY);
    public static final ReservationOperation unusedInstancesHeavy = new ReservationOperation("Unused RIs - No Upfront", 9, ReservationUtilization.HEAVY);
    public static final ReservationOperation upfrontAmortizedHeavy = new ReservationOperation("Amortized RIs - No Upfront", 10, ReservationUtilization.HEAVY);

    public static final ReservationOperation savingsPartial = new ReservationOperation("Savings - Partial Upfront", 11, ReservationUtilization.PARTIAL);
    public static final ReservationOperation reservedInstancesPartial = new ReservationOperation("Used RIs - Partial Upfront", 12, ReservationUtilization.PARTIAL);
    public static final ReservationOperation familyReservedInstancesPartial = new ReservationOperation("Family RIs - Partial Upfront", 13, ReservationUtilization.PARTIAL);
    public static final ReservationOperation bonusReservedInstancesPartial = new ReservationOperation("Bonus RIs - Partial Upfront", 14, ReservationUtilization.PARTIAL);
    public static final ReservationOperation borrowedInstancesPartial = new ReservationOperation("Borrowed RIs - Partial Upfront", 15, ReservationUtilization.PARTIAL);
    public static final ReservationOperation lentInstancesPartial = new ReservationOperation("Lent RIs - Partial Upfront", 16, ReservationUtilization.PARTIAL);
    public static final ReservationOperation unusedInstancesPartial = new ReservationOperation("Unused RIs - Partial Upfront", 17, ReservationUtilization.PARTIAL);
    public static final ReservationOperation upfrontAmortizedPartial = new ReservationOperation("Amortized RIs - Partial Upfront", 18, ReservationUtilization.PARTIAL);    

    public static final ReservationOperation savingsFixed = new ReservationOperation("Savings - All Upfront", 19, ReservationUtilization.FIXED);
    public static final ReservationOperation reservedInstancesFixed = new ReservationOperation("Used RIs - All Upfront", 20, ReservationUtilization.FIXED);
    public static final ReservationOperation familyReservedInstancesFixed = new ReservationOperation("Family RIs - All Upfront", 21, ReservationUtilization.FIXED);
    public static final ReservationOperation bonusReservedInstancesFixed = new ReservationOperation("Bonus RIs - All Upfront", 22, ReservationUtilization.FIXED);
    public static final ReservationOperation borrowedInstancesFixed = new ReservationOperation("Borrowed RIs - All Upfront", 23, ReservationUtilization.FIXED);
    public static final ReservationOperation lentInstancesFixed = new ReservationOperation("Lent RIs - All Upfront", 24, ReservationUtilization.FIXED);
    public static final ReservationOperation unusedInstancesFixed = new ReservationOperation("Unused RIs - All Upfront", 25, ReservationUtilization.FIXED);
    public static final ReservationOperation upfrontAmortizedFixed = new ReservationOperation("Amortized RIs - All Upfront", 26, ReservationUtilization.FIXED);

    public static ReservationOperation getReservedInstances(ReservationUtilization utilization) {
        switch (utilization) {
            case FIXED: return reservedInstancesFixed;
            case HEAVY: return reservedInstancesHeavy;
            case PARTIAL: return reservedInstancesPartial;
            default: throw new RuntimeException("Unknown ReservationUtilization " + utilization);
        }
    }

    public static ReservationOperation getFamilyReservedInstances(ReservationUtilization utilization) {
        switch (utilization) {
            case FIXED: return familyReservedInstancesFixed;
            case HEAVY: return familyReservedInstancesHeavy;
            case PARTIAL: return familyReservedInstancesPartial;
            default: throw new RuntimeException("Unknown ReservationUtilization " + utilization);
        }
    }

    public static ReservationOperation getBonusReservedInstances(ReservationUtilization utilization) {
        switch (utilization) {
            case FIXED: return bonusReservedInstancesFixed;
            case HEAVY: return bonusReservedInstancesHeavy;
            case PARTIAL: return bonusReservedInstancesPartial;
            default: throw new RuntimeException("Unknown ReservationUtilization " + utilization);
        }
    }

    public static ReservationOperation getBorrowedInstances(ReservationUtilization utilization) {
        switch (utilization) {
            case FIXED: return borrowedInstancesFixed;
            case HEAVY: return borrowedInstancesHeavy;
            case PARTIAL: return borrowedInstancesPartial;
            default: throw new RuntimeException("Unknown ReservationUtilization " + utilization);
        }
    }

    public static List<ReservationOperation> getLentOperations() {
        return Lists.newArrayList(lentInstancesFixed, lentInstancesHeavy, lentInstancesPartial);
    }

    public static ReservationOperation getLentInstances(ReservationUtilization utilization) {
        switch (utilization) {
            case FIXED: return lentInstancesFixed;
            case HEAVY: return lentInstancesHeavy;
            case PARTIAL: return lentInstancesPartial;
            default: throw new RuntimeException("Unknown ReservationUtilization " + utilization);
        }
    }

    public static ReservationOperation getUnusedInstances(ReservationUtilization utilization) {
        switch (utilization) {
            case FIXED: return unusedInstancesFixed;
            case HEAVY: return unusedInstancesHeavy;
            case PARTIAL: return unusedInstancesPartial;
            default: throw new RuntimeException("Unknown ReservationUtilization " + utilization);
        }
    }

    public static List<ReservationOperation> getAmortizationOperations() {
        return Lists.newArrayList(upfrontAmortizedHeavy, upfrontAmortizedPartial, upfrontAmortizedFixed);
    }

    public static ReservationOperation getUpfrontAmortized(ReservationUtilization utilization) {
        switch (utilization) {
            case FIXED: return upfrontAmortizedFixed;
            case HEAVY: return upfrontAmortizedHeavy;
            case PARTIAL: return upfrontAmortizedPartial;
            default: throw new RuntimeException("Unknown ReservationUtilization " + utilization);
        }
    }

    public static List<ReservationOperation> getSavingsOperations() {
        return Lists.newArrayList(spotInstanceSavings, savingsFixed, savingsHeavy, savingsPartial);
    }

    public static ReservationOperation getSavings(ReservationUtilization utilization) {
        switch (utilization) {
            case FIXED: return savingsFixed;
            case HEAVY: return savingsHeavy;
            case PARTIAL: return savingsPartial;
            default: throw new RuntimeException("Unknown ReservationUtilization " + utilization);
        }
    }

    public static Operation getOperation(String name) {
    	if (name.isEmpty()) {
    		// Support entries don't have an operation field
    		name = "None";
    	}
        Operation operation = operations.get(name);
        if (operation == null) {
            operations.putIfAbsent(name, new Operation(name));
            operation = operations.get(name);
        }

        return operation;
    }

    public static List<Operation> getOperations(List<String> names) {
        List<Operation> result = Lists.newArrayList();
        for (String name: names)
            result.add(operations.get(name));
        return result;
    }

    public static class ReservationOperation extends Operation {
		private static final long serialVersionUID = 1L;
		private ReservationUtilization utilization = null;

		private ReservationOperation(String name, int seq, ReservationUtilization utilization) {
            super(name);
            this.seq = seq;
            this.utilization = utilization;
            operations.put(name, this);
        }
		
		public ReservationUtilization getUtilization() {
			return utilization;
		}
    }

    @Override
    public int compareTo(Tag t) {
        if (t instanceof Operation) {
            Operation o = (Operation)t;
            int result = this.seq - o.seq;
            return result == 0 ? this.name.compareTo(t.name) : result;
        }
        else
            return super.compareTo(t);
    }
    
    public boolean isBonus() {
    	return name.startsWith("Bonus RIs - ");
    }
}
