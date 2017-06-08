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
import com.netflix.ice.processor.Ec2InstanceReservationPrice;
import com.netflix.ice.processor.Ec2InstanceReservationPrice.ReservationUtilization.*;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

public class Operation extends Tag {

    protected int seq = Integer.MAX_VALUE;
    private Operation (String name) {
        super(name);
    }
    private static ConcurrentMap<String, Operation> operations = Maps.newConcurrentMap();

    public static final ReservationOperation ondemandInstances = new ReservationOperation("OndemandInstances", 0);

    // Heavy and Fixed map to the new "No Upfront", "Partial Upfront", and "All Upfront" reservation types
    public static final ReservationOperation reservedInstancesHeavy = new ReservationOperation("ReservedInstancesNoUpfront", 1);
    public static final ReservationOperation familyReservedInstancesHeavy = new ReservationOperation("FamilyReservedInstancesNoUpfront", 2);
    public static final ReservationOperation bonusReservedInstancesHeavy = new ReservationOperation("BonusReservedInstancesNoUpfront", 3);
    public static final ReservationOperation borrowedInstancesHeavy = new ReservationOperation("BorrowedInstancesNoUpfront", 4);
    public static final ReservationOperation lentInstancesHeavy = new ReservationOperation("LentInstancesNoUpfront", 5);
    public static final ReservationOperation unusedInstancesHeavy = new ReservationOperation("UnusedInstancesNoUpfront", 6);
    public static final ReservationOperation upfrontAmortizedHeavy = new ReservationOperation("AmortizedNoUpfront", 7);

    public static final ReservationOperation reservedInstancesFixed = new ReservationOperation("ReservedInstancesAllUpfront", 8);
    public static final ReservationOperation familyReservedInstancesFixed = new ReservationOperation("FamilyReservedInstancesAllUpfront", 9);
    public static final ReservationOperation bonusReservedInstancesFixed = new ReservationOperation("BonusReservedInstancesAllUpfront", 10);
    public static final ReservationOperation borrowedInstancesFixed = new ReservationOperation("BorrowedInstancesAllUpfront", 11);
    public static final ReservationOperation lentInstancesFixed = new ReservationOperation("LentInstancesAllUpfront", 12);
    public static final ReservationOperation unusedInstancesFixed = new ReservationOperation("UnusedInstancesAllUpfront", 13);
    public static final ReservationOperation upfrontAmortizedFixed = new ReservationOperation("AmortizedAllUpfront", 14);

    public static final ReservationOperation reservedInstancesHeavyPartial = new ReservationOperation("ReservedInstancesPartialUpfront", 15);
    public static final ReservationOperation familyReservedInstancesHeavyPartial = new ReservationOperation("FamilyReservedInstancesPartialUpfront", 16);
    public static final ReservationOperation bonusReservedInstancesHeavyPartial = new ReservationOperation("BonusReservedInstancesPartialUpfront", 17);
    public static final ReservationOperation borrowedInstancesHeavyPartial = new ReservationOperation("BorrowedInstancesPartialUpfront", 18);
    public static final ReservationOperation lentInstancesHeavyPartial = new ReservationOperation("LentInstancesPartialUpfront", 19);
    public static final ReservationOperation unusedInstancesHeavyPartial = new ReservationOperation("UnusedInstancesPartialUpfront", 20);
    public static final ReservationOperation upfrontAmortizedHeavyPartial = new ReservationOperation("AmortizedPartialUpfront", 21);    

    public static final ReservationOperation spotInstances = new ReservationOperation("SpotInstances", 0);

    public static ReservationOperation getReservedInstances(Ec2InstanceReservationPrice.ReservationUtilization utilization) {
        switch (utilization) {
            case FIXED: return reservedInstancesFixed;
            case HEAVY: return reservedInstancesHeavy;
            case HEAVY_PARTIAL: return reservedInstancesHeavyPartial;
            default: throw new RuntimeException("Unknown ReservationUtilization " + utilization);
        }
    }

    public static ReservationOperation getFamilyReservedInstances(Ec2InstanceReservationPrice.ReservationUtilization utilization) {
        switch (utilization) {
            case FIXED: return familyReservedInstancesFixed;
            case HEAVY: return familyReservedInstancesHeavy;
            case HEAVY_PARTIAL: return familyReservedInstancesHeavyPartial;
            default: throw new RuntimeException("Unknown ReservationUtilization " + utilization);
        }
    }

    public static ReservationOperation getBonusReservedInstances(Ec2InstanceReservationPrice.ReservationUtilization utilization) {
        switch (utilization) {
            case FIXED: return bonusReservedInstancesFixed;
            case HEAVY: return bonusReservedInstancesHeavy;
            case HEAVY_PARTIAL: return bonusReservedInstancesHeavyPartial;
            default: throw new RuntimeException("Unknown ReservationUtilization " + utilization);
        }
    }

    public static ReservationOperation getBorrowedInstances(Ec2InstanceReservationPrice.ReservationUtilization utilization) {
        switch (utilization) {
            case FIXED: return borrowedInstancesFixed;
            case HEAVY: return borrowedInstancesHeavy;
            case HEAVY_PARTIAL: return borrowedInstancesHeavyPartial;
            default: throw new RuntimeException("Unknown ReservationUtilization " + utilization);
        }
    }

    public static List<ReservationOperation> getLentInstances() {
        return Lists.newArrayList(lentInstancesFixed, lentInstancesHeavy, lentInstancesHeavyPartial);
    }

    public static ReservationOperation getLentInstances(Ec2InstanceReservationPrice.ReservationUtilization utilization) {
        switch (utilization) {
            case FIXED: return lentInstancesFixed;
            case HEAVY: return lentInstancesHeavy;
            case HEAVY_PARTIAL: return lentInstancesHeavyPartial;
            default: throw new RuntimeException("Unknown ReservationUtilization " + utilization);
        }
    }

    public static ReservationOperation getUnusedInstances(Ec2InstanceReservationPrice.ReservationUtilization utilization) {
        switch (utilization) {
            case FIXED: return unusedInstancesFixed;
            case HEAVY: return unusedInstancesHeavy;
            case HEAVY_PARTIAL: return unusedInstancesHeavyPartial;
            default: throw new RuntimeException("Unknown ReservationUtilization " + utilization);
        }
    }

    public static ReservationOperation getUpfrontAmortized(Ec2InstanceReservationPrice.ReservationUtilization utilization) {
        switch (utilization) {
            case FIXED: return upfrontAmortizedFixed;
            case HEAVY: return upfrontAmortizedHeavy;
            case HEAVY_PARTIAL: return upfrontAmortizedHeavyPartial;
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
        private ReservationOperation(String name, int seq) {
            super(name);
            this.seq = seq;
            operations.put(name, this);
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
}
