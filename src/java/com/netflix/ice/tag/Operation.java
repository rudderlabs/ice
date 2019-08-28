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
    private static List<Operation> reservationOperations = Lists.newArrayList();

    public static final ReservationOperation spotInstanceSavings = new ReservationOperation("Savings - Spot", 0, null);
    public static final ReservationOperation spotInstances = new ReservationOperation("Spot Instances", 1, null);
    public static final ReservationOperation ondemandInstances = new ReservationOperation("On-Demand Instances", 2, null);

    public static final ReservationOperation savingsNo = new ReservationOperation("Savings - No Upfront", 3, ReservationUtilization.NO);
    public static final ReservationOperation reservedInstancesNo = new ReservationOperation("Used RIs - No Upfront", 4, ReservationUtilization.NO);
    public static final ReservationOperation familyReservedInstancesNo = new ReservationOperation("Family RIs - No Upfront", 5, ReservationUtilization.NO);
    public static final ReservationOperation bonusReservedInstancesNo = new ReservationOperation("Bonus RIs - No Upfront", 6, ReservationUtilization.NO);
    public static final ReservationOperation borrowedInstancesNo = new ReservationOperation("Borrowed RIs - No Upfront", 7, ReservationUtilization.NO);
    public static final ReservationOperation lentInstancesNo = new ReservationOperation("Lent RIs - No Upfront", 8, ReservationUtilization.NO);
    public static final ReservationOperation unusedInstancesNo = new ReservationOperation("Unused RIs - No Upfront", 9, ReservationUtilization.NO);

    public static final ReservationOperation savingsPartial = new ReservationOperation("Savings - Partial Upfront", 11, ReservationUtilization.PARTIAL);
    public static final ReservationOperation reservedInstancesPartial = new ReservationOperation("Used RIs - Partial Upfront", 12, ReservationUtilization.PARTIAL);
    public static final ReservationOperation familyReservedInstancesPartial = new ReservationOperation("Family RIs - Partial Upfront", 13, ReservationUtilization.PARTIAL);
    public static final ReservationOperation bonusReservedInstancesPartial = new ReservationOperation("Bonus RIs - Partial Upfront", 14, ReservationUtilization.PARTIAL);
    public static final ReservationOperation borrowedInstancesPartial = new ReservationOperation("Borrowed RIs - Partial Upfront", 15, ReservationUtilization.PARTIAL);
    public static final ReservationOperation lentInstancesPartial = new ReservationOperation("Lent RIs - Partial Upfront", 16, ReservationUtilization.PARTIAL);
    public static final ReservationOperation upfrontAmortizedPartial = new ReservationOperation("Amortized RIs - Partial Upfront", 17, ReservationUtilization.PARTIAL);    
    public static final ReservationOperation unusedInstancesPartial = new ReservationOperation("Unused RIs - Partial Upfront", 18, ReservationUtilization.PARTIAL);

    public static final ReservationOperation savingsAll = new ReservationOperation("Savings - All Upfront", 19, ReservationUtilization.ALL);
    public static final ReservationOperation reservedInstancesAll = new ReservationOperation("Used RIs - All Upfront", 20, ReservationUtilization.ALL);
    public static final ReservationOperation familyReservedInstancesAll = new ReservationOperation("Family RIs - All Upfront", 21, ReservationUtilization.ALL);
    public static final ReservationOperation bonusReservedInstancesAll = new ReservationOperation("Bonus RIs - All Upfront", 22, ReservationUtilization.ALL);
    public static final ReservationOperation borrowedInstancesAll = new ReservationOperation("Borrowed RIs - All Upfront", 23, ReservationUtilization.ALL);
    public static final ReservationOperation lentInstancesAll = new ReservationOperation("Lent RIs - All Upfront", 24, ReservationUtilization.ALL);
    public static final ReservationOperation upfrontAmortizedAll = new ReservationOperation("Amortized RIs - All Upfront", 25, ReservationUtilization.ALL);
    public static final ReservationOperation unusedInstancesAll = new ReservationOperation("Unused RIs - All Upfront", 26, ReservationUtilization.ALL);

    // Legacy Heavy/Medium/Light Utilization types used only by ElastiCache. No family sharing or account borrowing
    public static final ReservationOperation savingsHeavy = new ReservationOperation("Savings - Heavy Utilization", 27, ReservationUtilization.HEAVY);
    public static final ReservationOperation reservedInstancesHeavy = new ReservationOperation("Used RIs - Heavy Utilization", 28, ReservationUtilization.HEAVY);
    public static final ReservationOperation bonusReservedInstancesHeavy = new ReservationOperation("Bonus RIs - Heavy Utilization", 29, ReservationUtilization.HEAVY);
    public static final ReservationOperation upfrontAmortizedHeavy = new ReservationOperation("Amortized RIs - Heavy Utilization", 30, ReservationUtilization.HEAVY);
    public static final ReservationOperation unusedInstancesHeavy = new ReservationOperation("Unused RIs - Heavy Utilization", 31, ReservationUtilization.HEAVY);

    public static final ReservationOperation savingsMedium = new ReservationOperation("Savings - Medium Utilization", 32, ReservationUtilization.MEDIUM);
    public static final ReservationOperation reservedInstancesMedium = new ReservationOperation("Used RIs - Medium Utilization", 33, ReservationUtilization.MEDIUM);
    public static final ReservationOperation bonusReservedInstancesMedium = new ReservationOperation("Bonus RIs - Medium Utilization", 34, ReservationUtilization.MEDIUM);
    public static final ReservationOperation upfrontAmortizedMedium = new ReservationOperation("Amortized RIs - Medium Utilization", 35, ReservationUtilization.MEDIUM);
    public static final ReservationOperation unusedInstancesMedium = new ReservationOperation("Unused RIs - Medium Utilization", 36, ReservationUtilization.MEDIUM);

    public static final ReservationOperation savingsLight = new ReservationOperation("Savings - Light Utilization", 37, ReservationUtilization.LIGHT);
    public static final ReservationOperation reservedInstancesLight = new ReservationOperation("Used RIs - Light Utilization", 38, ReservationUtilization.LIGHT);
    public static final ReservationOperation bonusReservedInstancesLight = new ReservationOperation("Bonus RIs - Light Utilization", 39, ReservationUtilization.LIGHT);
    public static final ReservationOperation upfrontAmortizedLight = new ReservationOperation("Amortized RIs - Light Utilization", 40, ReservationUtilization.LIGHT);
    public static final ReservationOperation unusedInstancesLight = new ReservationOperation("Unused RIs - Light Utilization", 41, ReservationUtilization.LIGHT);

    public static ReservationOperation getReservedInstances(ReservationUtilization utilization) {
        switch (utilization) {
            case ALL: return reservedInstancesAll;
            case NO: return reservedInstancesNo;
            case PARTIAL: return reservedInstancesPartial;
            case HEAVY: return reservedInstancesHeavy;
            case MEDIUM: return reservedInstancesMedium;
            case LIGHT: return reservedInstancesLight;
            default: throw new RuntimeException("Unknown ReservationUtilization " + utilization);
        }
    }

    public static ReservationOperation getFamilyReservedInstances(ReservationUtilization utilization) {
        switch (utilization) {
            case ALL: return familyReservedInstancesAll;
            case NO: return familyReservedInstancesNo;
            case PARTIAL: return familyReservedInstancesPartial;
            default: throw new RuntimeException("Unknown ReservationUtilization " + utilization);
        }
    }

    public static ReservationOperation getBonusReservedInstances(ReservationUtilization utilization) {
        switch (utilization) {
            case ALL: return bonusReservedInstancesAll;
            case NO: return bonusReservedInstancesNo;
            case PARTIAL: return bonusReservedInstancesPartial;
            case HEAVY: return bonusReservedInstancesHeavy;
            case MEDIUM: return bonusReservedInstancesMedium;
            case LIGHT: return bonusReservedInstancesLight;
            default: throw new RuntimeException("Unknown ReservationUtilization " + utilization);
        }
    }

    public static ReservationOperation getBorrowedInstances(ReservationUtilization utilization) {
        switch (utilization) {
            case ALL: return borrowedInstancesAll;
            case NO: return borrowedInstancesNo;
            case PARTIAL: return borrowedInstancesPartial;
            default: throw new RuntimeException("Unknown ReservationUtilization " + utilization);
        }
    }

    public static List<ReservationOperation> getLentOperations() {
        return Lists.newArrayList(lentInstancesAll, lentInstancesNo, lentInstancesPartial);
    }

    public static ReservationOperation getLentInstances(ReservationUtilization utilization) {
        switch (utilization) {
            case ALL: return lentInstancesAll;
            case NO: return lentInstancesNo;
            case PARTIAL: return lentInstancesPartial;
            default: throw new RuntimeException("Unknown ReservationUtilization " + utilization);
        }
    }

    public static ReservationOperation getUnusedInstances(ReservationUtilization utilization) {
        switch (utilization) {
            case ALL: return unusedInstancesAll;
            case NO: return unusedInstancesNo;
            case PARTIAL: return unusedInstancesPartial;
            case HEAVY: return unusedInstancesHeavy;
            case MEDIUM: return unusedInstancesMedium;
            case LIGHT: return unusedInstancesLight;
            default: throw new RuntimeException("Unknown ReservationUtilization " + utilization);
        }
    }

    public static List<ReservationOperation> getAmortizationOperations() {
        return Lists.newArrayList(upfrontAmortizedPartial, upfrontAmortizedAll, upfrontAmortizedHeavy, upfrontAmortizedMedium, upfrontAmortizedLight);
    }

    public static ReservationOperation getUpfrontAmortized(ReservationUtilization utilization) {
        switch (utilization) {
            case ALL: return upfrontAmortizedAll;
            case PARTIAL: return upfrontAmortizedPartial;
            case HEAVY: return upfrontAmortizedHeavy;
            case MEDIUM: return upfrontAmortizedMedium;
            case LIGHT: return upfrontAmortizedLight;
            default: throw new RuntimeException("Unknown ReservationUtilization " + utilization);
        }
    }

    public static List<ReservationOperation> getSavingsOperations() {
        return Lists.newArrayList(spotInstanceSavings, savingsAll, savingsNo, savingsPartial, savingsHeavy, savingsMedium, savingsLight);
    }

    public static ReservationOperation getSavings(ReservationUtilization utilization) {
        switch (utilization) {
            case ALL: return savingsAll;
            case NO: return savingsNo;
            case PARTIAL: return savingsPartial;
            case HEAVY: return savingsHeavy;
            case MEDIUM: return savingsMedium;
            case LIGHT: return savingsLight;
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

    public static List<Operation> getReservationOperations() {
        return reservationOperations;
    }

    public static class ReservationOperation extends Operation {
		private static final long serialVersionUID = 1L;
		private ReservationUtilization utilization = null;

		private ReservationOperation(String name, int seq, ReservationUtilization utilization) {
            super(name);
            this.seq = seq;
            this.utilization = utilization;
            operations.put(name, this);
            reservationOperations.add(this);
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
    public boolean isLent() {
    	return name.startsWith("Lent RIs - ");
    }
    public boolean isBorrowed() {
    	return name.startsWith("Borrowed RIs - ");
    }
    public boolean isFamily() {
    	return name.startsWith("Family RIs - ");
    }
    public boolean isUsed() {
    	return name.startsWith("Used RIs -");
    }
    public boolean isUnused() {
    	return name.startsWith("Unused RIs - ");
    }
    public boolean isAmortized() {
    	return name.startsWith("Amortized RIs - ");
    }
    public boolean isOnDemand() {
    	return this == ondemandInstances;
    }
    public boolean isSpot() {
    	return this == spotInstances;
    }
    public boolean isSavings() {
    	return name.startsWith("Savings - ");
    }
}
