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
    private static List<Operation> savingsPlanOperations = Lists.newArrayList();
    
    private enum Category {
    	Used,
    	Bonus,
    	Borrowed,
    	Lent,
    	Amortized,
    	Unused,
    	UnusedAmortized;
    }

    public static final ReservationOperation spotInstanceSavings = new ReservationOperation("Savings - Spot", 0, null);
    public static final ReservationOperation spotInstances = new ReservationOperation("Spot Instances", 1, null);
    public static final ReservationOperation ondemandInstances = new ReservationOperation("On-Demand Instances", 2, null);

    public static final ReservationOperation savingsNoUpfront = new ReservationOperation("Savings - No Upfront", 3, ReservationUtilization.NO);
    public static final ReservationOperation reservedInstancesNoUpfront = new ReservationOperation("Used RIs - No Upfront", 4, ReservationUtilization.NO);
    public static final ReservationOperation familyReservedInstancesNoUpfront = new ReservationOperation("Family RIs - No Upfront", 5, ReservationUtilization.NO);
    public static final ReservationOperation bonusReservedInstancesNoUpfront = new ReservationOperation("Bonus RIs - No Upfront", 6, ReservationUtilization.NO);
    public static final ReservationOperation borrowedInstancesNoUpfront = new ReservationOperation("Borrowed RIs - No Upfront", 7, ReservationUtilization.NO);
    public static final ReservationOperation lentInstancesNoUpfront = new ReservationOperation("Lent RIs - No Upfront", 8, ReservationUtilization.NO);
    public static final ReservationOperation unusedInstancesNoUpfront = new ReservationOperation("Unused RIs - No Upfront", 9, ReservationUtilization.NO);

    public static final ReservationOperation savingsPartialUpfront = new ReservationOperation("Savings - Partial Upfront", 11, ReservationUtilization.PARTIAL);
    public static final ReservationOperation reservedInstancesPartialUpfront = new ReservationOperation("Used RIs - Partial Upfront", 12, ReservationUtilization.PARTIAL);
    public static final ReservationOperation familyReservedInstancesPartialUpfront = new ReservationOperation("Family RIs - Partial Upfront", 13, ReservationUtilization.PARTIAL);
    public static final ReservationOperation bonusReservedInstancesPartialUpfront = new ReservationOperation("Bonus RIs - Partial Upfront", 14, ReservationUtilization.PARTIAL);
    public static final ReservationOperation borrowedInstancesPartialUpfront = new ReservationOperation("Borrowed RIs - Partial Upfront", 15, ReservationUtilization.PARTIAL);
    public static final ReservationOperation lentInstancesPartialUpfront = new ReservationOperation("Lent RIs - Partial Upfront", 16, ReservationUtilization.PARTIAL);
    public static final ReservationOperation upfrontAmortizedPartialUpfront = new ReservationOperation("Amortized RIs - Partial Upfront", 17, ReservationUtilization.PARTIAL);    
    public static final ReservationOperation unusedInstancesPartialUpfront = new ReservationOperation("Unused RIs - Partial Upfront", 18, ReservationUtilization.PARTIAL);

    public static final ReservationOperation savingsAllUpfront = new ReservationOperation("Savings - All Upfront", 19, ReservationUtilization.ALL);
    public static final ReservationOperation reservedInstancesAllUpfront = new ReservationOperation("Used RIs - All Upfront", 20, ReservationUtilization.ALL);
    public static final ReservationOperation familyReservedInstancesAllUpfront = new ReservationOperation("Family RIs - All Upfront", 21, ReservationUtilization.ALL);
    public static final ReservationOperation bonusReservedInstancesAllUpfront = new ReservationOperation("Bonus RIs - All Upfront", 22, ReservationUtilization.ALL);
    public static final ReservationOperation borrowedInstancesAllUpfront = new ReservationOperation("Borrowed RIs - All Upfront", 23, ReservationUtilization.ALL);
    public static final ReservationOperation lentInstancesAllUpfront = new ReservationOperation("Lent RIs - All Upfront", 24, ReservationUtilization.ALL);
    public static final ReservationOperation upfrontAmortizedAllUpfront = new ReservationOperation("Amortized RIs - All Upfront", 25, ReservationUtilization.ALL);
    public static final ReservationOperation unusedInstancesAllUpfront = new ReservationOperation("Unused RIs - All Upfront", 26, ReservationUtilization.ALL);

    // Legacy Heavy/Medium/Light Utilization types used only by ElastiCache. No family sharing or account borrowing
    public static final ReservationOperation savingsHeavy = new ReservationOperation("Savings - Heavy Utilization", 27, ReservationUtilization.HEAVY);
    public static final ReservationOperation reservedInstancesHeavy = new ReservationOperation("Used RIs - Heavy Utilization", 28, ReservationUtilization.HEAVY);
    public static final ReservationOperation bonusReservedInstancesHeavy = new ReservationOperation("Bonus RIs - Heavy Utilization", 29, ReservationUtilization.HEAVY);
    public static final ReservationOperation borrowedInstancesHeavy = new ReservationOperation("Borrowed RIs - Heavy Utilization", 30, ReservationUtilization.HEAVY);
    public static final ReservationOperation lentInstancesHeavy = new ReservationOperation("Lent RIs - Heavy Utilization", 31, ReservationUtilization.HEAVY);
    public static final ReservationOperation upfrontAmortizedHeavy = new ReservationOperation("Amortized RIs - Heavy Utilization", 32, ReservationUtilization.HEAVY);
    public static final ReservationOperation unusedInstancesHeavy = new ReservationOperation("Unused RIs - Heavy Utilization", 33, ReservationUtilization.HEAVY);

    public static final ReservationOperation savingsMedium = new ReservationOperation("Savings - Medium Utilization", 34, ReservationUtilization.MEDIUM);
    public static final ReservationOperation reservedInstancesMedium = new ReservationOperation("Used RIs - Medium Utilization", 35, ReservationUtilization.MEDIUM);
    public static final ReservationOperation bonusReservedInstancesMedium = new ReservationOperation("Bonus RIs - Medium Utilization", 36, ReservationUtilization.MEDIUM);
    public static final ReservationOperation borrowedInstancesMedium = new ReservationOperation("Borrowed RIs - Medium Utilization", 37, ReservationUtilization.MEDIUM);
    public static final ReservationOperation lentInstancesMedium = new ReservationOperation("Lent RIs - Medium Utilization", 38, ReservationUtilization.MEDIUM);
    public static final ReservationOperation upfrontAmortizedMedium = new ReservationOperation("Amortized RIs - Medium Utilization", 39, ReservationUtilization.MEDIUM);
    public static final ReservationOperation unusedInstancesMedium = new ReservationOperation("Unused RIs - Medium Utilization", 40, ReservationUtilization.MEDIUM);

    public static final ReservationOperation savingsLight = new ReservationOperation("Savings - Light Utilization", 41, ReservationUtilization.LIGHT);
    public static final ReservationOperation reservedInstancesLight = new ReservationOperation("Used RIs - Light Utilization", 42, ReservationUtilization.LIGHT);
    public static final ReservationOperation bonusReservedInstancesLight = new ReservationOperation("Bonus RIs - Light Utilization", 43, ReservationUtilization.LIGHT);
    public static final ReservationOperation borrowedInstancesLight = new ReservationOperation("Borrowed RIs - Light Utilization", 44, ReservationUtilization.LIGHT);
    public static final ReservationOperation lentInstancesLight = new ReservationOperation("Lent RIs - Light Utilization", 45, ReservationUtilization.LIGHT);
    public static final ReservationOperation upfrontAmortizedLight = new ReservationOperation("Amortized RIs - Light Utilization", 46, ReservationUtilization.LIGHT);
    public static final ReservationOperation unusedInstancesLight = new ReservationOperation("Unused RIs - Light Utilization", 47, ReservationUtilization.LIGHT);

    public static final ReservationOperation reservedInstancesCredits = new ReservationOperation("RI Credits", 48, null);
    
    public static final SavingsPlanOperation savingsPlanUsedNoUpfront = new SavingsPlanOperation(Category.Used, 100, SavingsPlanPaymentOption.NoUpfront);
    public static final SavingsPlanOperation savingsPlanBonusNoUpfront = new SavingsPlanOperation(Category.Bonus, 101, SavingsPlanPaymentOption.NoUpfront);
    public static final SavingsPlanOperation savingsPlanBorrowedNoUpfront = new SavingsPlanOperation(Category.Borrowed, 102, SavingsPlanPaymentOption.NoUpfront);
    public static final SavingsPlanOperation savingsPlanLentNoUpfront = new SavingsPlanOperation(Category.Lent, 103, SavingsPlanPaymentOption.NoUpfront);
    public static final SavingsPlanOperation savingsPlanUnusedNoUpfront = new SavingsPlanOperation(Category.Unused, 104, SavingsPlanPaymentOption.NoUpfront);

    public static final SavingsPlanOperation savingsPlanUsedPartialUpfront = new SavingsPlanOperation(Category.Used, 105, SavingsPlanPaymentOption.PartialUpfront);
    public static final SavingsPlanOperation savingsPlanBonusPartialUpfront = new SavingsPlanOperation(Category.Bonus, 106, SavingsPlanPaymentOption.PartialUpfront);
    public static final SavingsPlanOperation savingsPlanBorrowedPartialUpfront = new SavingsPlanOperation(Category.Borrowed, 107, SavingsPlanPaymentOption.PartialUpfront);
    public static final SavingsPlanOperation savingsPlanLentPartialUpfront = new SavingsPlanOperation(Category.Lent, 108, SavingsPlanPaymentOption.PartialUpfront);
    public static final SavingsPlanOperation savingsPlanAmortizedPartialUpfront = new SavingsPlanOperation(Category.Amortized, 109, SavingsPlanPaymentOption.PartialUpfront);
    public static final SavingsPlanOperation savingsPlanUnusedPartialUpfront = new SavingsPlanOperation(Category.Unused, 110, SavingsPlanPaymentOption.PartialUpfront);
    public static final SavingsPlanOperation savingsPlanUnusedAmortizedPartialUpfront = new SavingsPlanOperation(Category.UnusedAmortized, 110, SavingsPlanPaymentOption.PartialUpfront);

    public static final SavingsPlanOperation savingsPlanUsedAllUpfront = new SavingsPlanOperation(Category.Used, 111, SavingsPlanPaymentOption.AllUpfront);
    public static final SavingsPlanOperation savingsPlanBonusAllUpfront = new SavingsPlanOperation(Category.Bonus, 112, SavingsPlanPaymentOption.AllUpfront);
    public static final SavingsPlanOperation savingsPlanBorrowedAllUpfront = new SavingsPlanOperation(Category.Borrowed, 113, SavingsPlanPaymentOption.AllUpfront);
    public static final SavingsPlanOperation savingsPlanLentAllUpfront = new SavingsPlanOperation(Category.Lent, 114, SavingsPlanPaymentOption.AllUpfront);
    public static final SavingsPlanOperation savingsPlanAmortizedAllUpfront = new SavingsPlanOperation(Category.Amortized, 115, SavingsPlanPaymentOption.AllUpfront);
    public static final SavingsPlanOperation savingsPlanUnusedAllUpfront = new SavingsPlanOperation(Category.Unused, 116, SavingsPlanPaymentOption.AllUpfront);
    public static final SavingsPlanOperation savingsPlanUnusedAmortizedAllUpfront = new SavingsPlanOperation(Category.UnusedAmortized, 116, SavingsPlanPaymentOption.AllUpfront);

    public static ReservationOperation getReservedInstances(ReservationUtilization utilization) {
        switch (utilization) {
            case ALL: return reservedInstancesAllUpfront;
            case NO: return reservedInstancesNoUpfront;
            case PARTIAL: return reservedInstancesPartialUpfront;
            case HEAVY: return reservedInstancesHeavy;
            case MEDIUM: return reservedInstancesMedium;
            case LIGHT: return reservedInstancesLight;
            default: throw new RuntimeException("Unknown ReservationUtilization " + utilization);
        }
    }

    public static ReservationOperation getFamilyReservedInstances(ReservationUtilization utilization) {
        switch (utilization) {
            case ALL: return familyReservedInstancesAllUpfront;
            case NO: return familyReservedInstancesNoUpfront;
            case PARTIAL: return familyReservedInstancesPartialUpfront;
            default: throw new RuntimeException("Unknown ReservationUtilization " + utilization);
        }
    }

    public static ReservationOperation getBonusReservedInstances(ReservationUtilization utilization) {
        switch (utilization) {
            case ALL: return bonusReservedInstancesAllUpfront;
            case NO: return bonusReservedInstancesNoUpfront;
            case PARTIAL: return bonusReservedInstancesPartialUpfront;
            case HEAVY: return bonusReservedInstancesHeavy;
            case MEDIUM: return bonusReservedInstancesMedium;
            case LIGHT: return bonusReservedInstancesLight;
            default: throw new RuntimeException("Unknown ReservationUtilization " + utilization);
        }
    }

    public static ReservationOperation getBorrowedInstances(ReservationUtilization utilization) {
        switch (utilization) {
            case ALL: return borrowedInstancesAllUpfront;
            case NO: return borrowedInstancesNoUpfront;
            case PARTIAL: return borrowedInstancesPartialUpfront;
            case HEAVY: return borrowedInstancesHeavy;
            case MEDIUM: return borrowedInstancesMedium;
            case LIGHT: return borrowedInstancesLight;
            default: throw new RuntimeException("Unknown ReservationUtilization " + utilization);
        }
    }

    public static List<ReservationOperation> getLentOperations() {
        return Lists.newArrayList(lentInstancesAllUpfront, lentInstancesNoUpfront, lentInstancesPartialUpfront);
    }

    public static ReservationOperation getLentInstances(ReservationUtilization utilization) {
        switch (utilization) {
            case ALL: return lentInstancesAllUpfront;
            case NO: return lentInstancesNoUpfront;
            case PARTIAL: return lentInstancesPartialUpfront;
            case HEAVY: return lentInstancesHeavy;
            case MEDIUM: return lentInstancesMedium;
            case LIGHT: return lentInstancesLight;
            default: throw new RuntimeException("Unknown ReservationUtilization " + utilization);
        }
    }

    public static ReservationOperation getUnusedInstances(ReservationUtilization utilization) {
        switch (utilization) {
            case ALL: return unusedInstancesAllUpfront;
            case NO: return unusedInstancesNoUpfront;
            case PARTIAL: return unusedInstancesPartialUpfront;
            case HEAVY: return unusedInstancesHeavy;
            case MEDIUM: return unusedInstancesMedium;
            case LIGHT: return unusedInstancesLight;
            default: throw new RuntimeException("Unknown ReservationUtilization " + utilization);
        }
    }

    public static List<ReservationOperation> getAmortizationOperations() {
        return Lists.newArrayList(upfrontAmortizedPartialUpfront, upfrontAmortizedAllUpfront, upfrontAmortizedHeavy, upfrontAmortizedMedium, upfrontAmortizedLight);
    }

    public static ReservationOperation getUpfrontAmortized(ReservationUtilization utilization) {
        switch (utilization) {
            case ALL: return upfrontAmortizedAllUpfront;
            case PARTIAL: return upfrontAmortizedPartialUpfront;
            case HEAVY: return upfrontAmortizedHeavy;
            case MEDIUM: return upfrontAmortizedMedium;
            case LIGHT: return upfrontAmortizedLight;
            default: throw new RuntimeException("Unknown ReservationUtilization " + utilization);
        }
    }

    public static List<ReservationOperation> getSavingsOperations() {
        return Lists.newArrayList(spotInstanceSavings, savingsAllUpfront, savingsNoUpfront, savingsPartialUpfront, savingsHeavy, savingsMedium, savingsLight);
    }

    public static ReservationOperation getSavings(ReservationUtilization utilization) {
        switch (utilization) {
            case ALL: return savingsAllUpfront;
            case NO: return savingsNoUpfront;
            case PARTIAL: return savingsPartialUpfront;
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
    
    
    public static List<Operation> getSavingsPlanOperations() {
        return savingsPlanOperations;
    }

    public enum SavingsPlanPaymentOption {
    	NoUpfront,
    	PartialUpfront,
    	AllUpfront;
    	
    	public static SavingsPlanPaymentOption get(String name) {
    		return valueOf(name.replace(" ", ""));
    	}
    }
    
    public static List<SavingsPlanOperation> getSavingsPlanAmortizedOperations() {
        return Lists.newArrayList(savingsPlanAmortizedPartialUpfront, savingsPlanAmortizedAllUpfront,
        		savingsPlanUnusedAmortizedPartialUpfront, savingsPlanUnusedAmortizedAllUpfront);
    }
    
    public static SavingsPlanOperation getSavingsPlanAmortized(SavingsPlanPaymentOption paymentOption) {
    	switch(paymentOption) {
    	case PartialUpfront: return savingsPlanAmortizedPartialUpfront;
    	case AllUpfront: return savingsPlanAmortizedAllUpfront;
    	default: throw new RuntimeException("Unknown SavingsPlanPaymentOption " + paymentOption);
    	}
    }
    
    public static SavingsPlanOperation getSavingsPlanUnusedAmortized(SavingsPlanPaymentOption paymentOption) {
    	switch(paymentOption) {
    	case PartialUpfront: return savingsPlanUnusedAmortizedPartialUpfront;
    	case AllUpfront: return savingsPlanUnusedAmortizedAllUpfront;
    	default: throw new RuntimeException("Unknown SavingsPlanPaymentOption " + paymentOption);
    	}
    }
    
    public static SavingsPlanOperation getSavingsPlanUnused(SavingsPlanPaymentOption paymentOption) {
    	switch(paymentOption) {
    	case NoUpfront: return savingsPlanUnusedNoUpfront;
    	case PartialUpfront: return savingsPlanUnusedPartialUpfront;
    	case AllUpfront: return savingsPlanUnusedAllUpfront;
    	default: throw new RuntimeException("Unknown SavingsPlanPaymentOption " + paymentOption);
    	}
    }
    
    public static SavingsPlanOperation getSavingsPlanUsed(SavingsPlanPaymentOption paymentOption) {
    	switch(paymentOption) {
    	case NoUpfront: return savingsPlanUsedNoUpfront;
    	case PartialUpfront: return savingsPlanUsedPartialUpfront;
    	case AllUpfront: return savingsPlanUsedAllUpfront;
    	default: throw new RuntimeException("Unknown SavingsPlanPaymentOption " + paymentOption);
    	}
    }
    
    public static SavingsPlanOperation getSavingsPlanBorrowed(SavingsPlanPaymentOption paymentOption) {
    	switch(paymentOption) {
    	case NoUpfront: return savingsPlanBorrowedNoUpfront;
    	case PartialUpfront: return savingsPlanBorrowedPartialUpfront;
    	case AllUpfront: return savingsPlanBorrowedAllUpfront;
    	default: throw new RuntimeException("Unknown SavingsPlanPaymentOption " + paymentOption);
    	}
    }
    
    public static List<SavingsPlanOperation> getSavingsPlanLentOperations() {
        return Lists.newArrayList(savingsPlanLentNoUpfront, savingsPlanLentPartialUpfront, savingsPlanLentAllUpfront);
    }
    
    public static SavingsPlanOperation getSavingsPlanLent(SavingsPlanPaymentOption paymentOption) {
    	switch(paymentOption) {
    	case NoUpfront: return savingsPlanLentNoUpfront;
    	case PartialUpfront: return savingsPlanLentPartialUpfront;
    	case AllUpfront: return savingsPlanLentAllUpfront;
    	default: throw new RuntimeException("Unknown SavingsPlanPaymentOption " + paymentOption);
    	}
    }
    
    public static SavingsPlanOperation getSavingsPlanBonus(SavingsPlanPaymentOption paymentOption) {
    	switch(paymentOption) {
    	case NoUpfront: return savingsPlanBonusNoUpfront;
    	case PartialUpfront: return savingsPlanBonusPartialUpfront;
    	case AllUpfront: return savingsPlanBonusAllUpfront;
    	default: throw new RuntimeException("Unknown SavingsPlanPaymentOption " + paymentOption);
    	}
    }
    
    public boolean isSavingsPlan() {
    	return name.startsWith("SavingsPlan ");
    }
    
    public boolean isSavingsPlanUnused() {
    	return name.startsWith("SavingsPlan Unused - ");
    }
    
    public boolean isSavingsPlanLent() {
    	return name.startsWith("SavingsPlan Lent - ");
    }
    
    public boolean isSavingsPlanUnusedAmortized() {
    	return name.startsWith("SavingsPlan UnusedAmortized - ");
    }
    
    public boolean isSavingsPlanBonus() {
    	return name.startsWith("SavingsPlan Bonus - ");
    }
    
    public static class SavingsPlanOperation extends Operation {
		private static final long serialVersionUID = 1L;
		private final SavingsPlanPaymentOption paymentOption;
		
		private SavingsPlanOperation(Category category, int seq, SavingsPlanPaymentOption paymentOption) {
			super("SavingsPlan " + category.name() + " - " + paymentOption.name());
			this.seq = seq;
			this.paymentOption = paymentOption;
            operations.put(name, this);
            savingsPlanOperations.add(this);
		}
    	
		public SavingsPlanPaymentOption getPaymentOption() {
			return paymentOption;
		}
    }
    
}
