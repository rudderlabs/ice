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
    public static final ReservationOperation reservedInstances = new ReservationOperation("ReservedInstances", 1);

    public static final ReservationOperation reservedInstancesLight = new ReservationOperation("ReservedInstancesLight", 2);
    public static final ReservationOperation bonusReservedInstancesLight = new ReservationOperation("BonusReservedInstancesLight", 3);
    public static final ReservationOperation borrowedInstancesLight = new ReservationOperation("BorrowedInstancesLight", 4);
    public static final ReservationOperation lentInstancesLight = new ReservationOperation("LentInstancesLight", 5);
    public static final ReservationOperation unusedInstancesLight = new ReservationOperation("UnusedInstancesLight", 6);
    public static final ReservationOperation upfrontAmortizedLight = new ReservationOperation("UpfrontAmortizedLight", 7);

    public static final ReservationOperation reservedInstancesMedium = new ReservationOperation("ReservedInstancesMedium", 8);
    public static final ReservationOperation bonusReservedInstancesMedium = new ReservationOperation("BonusReservedInstancesMedium", 9);
    public static final ReservationOperation borrowedInstancesMedium = new ReservationOperation("BorrowedInstancesMedium", 10);
    public static final ReservationOperation lentInstancesMedium = new ReservationOperation("LentInstancesMedium", 11);
    public static final ReservationOperation unusedInstancesMedium = new ReservationOperation("UnusedInstancesMedium", 12);
    public static final ReservationOperation upfrontAmortizedMedium = new ReservationOperation("UpfrontAmortizedMedium", 13);

    public static final ReservationOperation reservedInstancesHeavy = new ReservationOperation("ReservedInstancesHeavy", 14);
    public static final ReservationOperation bonusReservedInstancesHeavy = new ReservationOperation("BonusReservedInstancesHeavy", 15);
    public static final ReservationOperation borrowedInstancesHeavy = new ReservationOperation("BorrowedInstancesHeavy", 16);
    public static final ReservationOperation lentInstancesHeavy = new ReservationOperation("LentInstancesHeavy", 17);
    public static final ReservationOperation unusedInstancesHeavy = new ReservationOperation("UnusedInstancesHeavy", 18);
    public static final ReservationOperation upfrontAmortizedHeavy = new ReservationOperation("UpfrontAmortizedHeavy", 19);

    public static final ReservationOperation reservedInstancesFixed = new ReservationOperation("ReservedInstancesFixed", 20);
    public static final ReservationOperation bonusReservedInstancesFixed = new ReservationOperation("BonusReservedInstancesFixed", 21);
    public static final ReservationOperation borrowedInstancesFixed = new ReservationOperation("BorrowedInstancesFixed", 22);
    public static final ReservationOperation lentInstancesFixed = new ReservationOperation("LentInstancesFixed", 23);
    public static final ReservationOperation unusedInstancesFixed = new ReservationOperation("UnusedInstancesFixed", 24);
    public static final ReservationOperation upfrontAmortizedFixed = new ReservationOperation("UpfrontAmortizedFixed", 25);

    public static final ReservationOperation reservedInstancesHeavyPartial = new ReservationOperation("ReservedInstancesHeavyPartial", 26);
    public static final ReservationOperation bonusReservedInstancesHeavyPartial = new ReservationOperation("BonusReservedInstancesHeavyPartial", 27);
    public static final ReservationOperation borrowedInstancesHeavyPartial = new ReservationOperation("BorrowedInstancesHeavyPartial", 28);
    public static final ReservationOperation lentInstancesHeavyPartial = new ReservationOperation("LentInstancesHeavyPartial", 29);
    public static final ReservationOperation unusedInstancesHeavyPartial = new ReservationOperation("UnusedInstancesHeavyPartial", 30);
    public static final ReservationOperation upfrontAmortizedHeavyPartial = new ReservationOperation("UpfrontAmortizedHeavyPartial", 31);

    /*
     * Future renaming
     * 
    // Light and Medium reservations types no longer exist as of 2014
    public static final ReservationOperation reservedInstancesLight = new ReservationOperation("ReservedInstancesLight", 2);
    public static final ReservationOperation bonusReservedInstancesLight = new ReservationOperation("BonusReservedInstancesLight", 3);
    public static final ReservationOperation borrowedInstancesLight = new ReservationOperation("BorrowedInstancesLight", 4);
    public static final ReservationOperation lentInstancesLight = new ReservationOperation("LentInstancesLight", 5);
    public static final ReservationOperation unusedInstancesLight = new ReservationOperation("UnusedInstancesLight", 6);
    public static final ReservationOperation upfrontAmortizedLight = new ReservationOperation("UpfrontAmortizedLight", 7);

    public static final ReservationOperation reservedInstancesMedium = new ReservationOperation("ReservedInstancesMedium", 8);
    public static final ReservationOperation bonusReservedInstancesMedium = new ReservationOperation("BonusReservedInstancesMedium", 9);
    public static final ReservationOperation borrowedInstancesMedium = new ReservationOperation("BorrowedInstancesMedium", 10);
    public static final ReservationOperation lentInstancesMedium = new ReservationOperation("LentInstancesMedium", 11);
    public static final ReservationOperation unusedInstancesMedium = new ReservationOperation("UnusedInstancesMedium", 12);
    public static final ReservationOperation upfrontAmortizedMedium = new ReservationOperation("UpfrontAmortizedMedium", 13);

    // Heavy and Fixed map to the new "No Upfront", "Partial Upfront", and "All Upfront" reservation types
    public static final ReservationOperation reservedInstancesHeavy = new ReservationOperation("ReservedInstancesNoUpfront", 14);
    public static final ReservationOperation bonusReservedInstancesHeavy = new ReservationOperation("BonusReservedInstancesNoUpfront", 15);
    public static final ReservationOperation borrowedInstancesHeavy = new ReservationOperation("BorrowedInstancesNoUpfront", 16);
    public static final ReservationOperation lentInstancesHeavy = new ReservationOperation("LentInstancesNoUpfront", 17);
    public static final ReservationOperation unusedInstancesHeavy = new ReservationOperation("UnusedInstancesNoUpfront", 18);
    public static final ReservationOperation upfrontAmortizedHeavy = new ReservationOperation("UpfrontAmortizedNoUpfront", 19);

    public static final ReservationOperation reservedInstancesFixed = new ReservationOperation("ReservedInstancesAllUpfront", 20);
    public static final ReservationOperation bonusReservedInstancesFixed = new ReservationOperation("BonusReservedInstancesAllUpfront", 21);
    public static final ReservationOperation borrowedInstancesFixed = new ReservationOperation("BorrowedInstancesAllUpfront", 22);
    public static final ReservationOperation lentInstancesFixed = new ReservationOperation("LentInstancesAllUpfront", 23);
    public static final ReservationOperation unusedInstancesFixed = new ReservationOperation("UnusedInstancesAllUpfront", 24);
    public static final ReservationOperation upfrontAmortizedFixed = new ReservationOperation("UpfrontAmortizedAllUpfront", 25);

    public static final ReservationOperation reservedInstancesHeavyPartial = new ReservationOperation("ReservedInstancesPartialUpfront", 26);
    public static final ReservationOperation bonusReservedInstancesHeavyPartial = new ReservationOperation("BonusReservedInstancesPartialUpfront", 27);
    public static final ReservationOperation borrowedInstancesHeavyPartial = new ReservationOperation("BorrowedInstancesPartialUpfront", 28);
    public static final ReservationOperation lentInstancesHeavyPartial = new ReservationOperation("LentInstancesPartialUpfront", 29);
    public static final ReservationOperation unusedInstancesHeavyPartial = new ReservationOperation("UnusedInstancesPartialUpfront", 30);
    public static final ReservationOperation upfrontAmortizedHeavyPartial = new ReservationOperation("UpfrontAmortizedPartialUpfront", 31);
     * 
     * 
     */
    
    
    
    public static ReservationOperation getReservedInstances(Ec2InstanceReservationPrice.ReservationUtilization utilization) {
        switch (utilization) {
            case FIXED: return reservedInstancesFixed;
            case HEAVY: return reservedInstancesHeavy;
            case HEAVY_PARTIAL: return reservedInstancesHeavyPartial;
            case MEDIUM: return reservedInstancesMedium;
            case LIGHT: return reservedInstancesLight;
            default: throw new RuntimeException("Unknown ReservationUtilization " + utilization);
        }
    }

    public static ReservationOperation getBonusReservedInstances(Ec2InstanceReservationPrice.ReservationUtilization utilization) {
        switch (utilization) {
            case FIXED: return bonusReservedInstancesFixed;
            case HEAVY: return bonusReservedInstancesHeavy;
            case HEAVY_PARTIAL: return bonusReservedInstancesHeavyPartial;
            case MEDIUM: return bonusReservedInstancesMedium;
            case LIGHT: return bonusReservedInstancesLight;
            default: throw new RuntimeException("Unknown ReservationUtilization " + utilization);
        }
    }

    public static ReservationOperation getBorrowedInstances(Ec2InstanceReservationPrice.ReservationUtilization utilization) {
        switch (utilization) {
            case FIXED: return borrowedInstancesFixed;
            case HEAVY: return borrowedInstancesHeavy;
            case HEAVY_PARTIAL: return borrowedInstancesHeavyPartial;
            case MEDIUM: return borrowedInstancesMedium;
            case LIGHT: return borrowedInstancesLight;
            default: throw new RuntimeException("Unknown ReservationUtilization " + utilization);
        }
    }

    public static List<ReservationOperation> getLentInstances() {
        return Lists.newArrayList(lentInstancesFixed, lentInstancesHeavy, lentInstancesHeavyPartial, lentInstancesMedium, lentInstancesLight);
    }

    public static ReservationOperation getLentInstances(Ec2InstanceReservationPrice.ReservationUtilization utilization) {
        switch (utilization) {
            case FIXED: return lentInstancesFixed;
            case HEAVY: return lentInstancesHeavy;
            case HEAVY_PARTIAL: return lentInstancesHeavyPartial;
            case MEDIUM: return lentInstancesMedium;
            case LIGHT: return lentInstancesLight;
            default: throw new RuntimeException("Unknown ReservationUtilization " + utilization);
        }
    }

    public static ReservationOperation getUnusedInstances(Ec2InstanceReservationPrice.ReservationUtilization utilization) {
        switch (utilization) {
            case FIXED: return unusedInstancesFixed;
            case HEAVY: return unusedInstancesHeavy;
            case HEAVY_PARTIAL: return unusedInstancesHeavyPartial;
            case MEDIUM: return unusedInstancesMedium;
            case LIGHT: return unusedInstancesLight;
            default: throw new RuntimeException("Unknown ReservationUtilization " + utilization);
        }
    }

    public static ReservationOperation getUpfrontAmortized(Ec2InstanceReservationPrice.ReservationUtilization utilization) {
        switch (utilization) {
            case FIXED: return upfrontAmortizedFixed;
            case HEAVY: return upfrontAmortizedHeavy;
            case HEAVY_PARTIAL: return upfrontAmortizedHeavyPartial;
            case MEDIUM: return upfrontAmortizedMedium;
            case LIGHT: return upfrontAmortizedLight;
            default: throw new RuntimeException("Unknown ReservationUtilization " + utilization);
        }
    }

    public static Operation getOperation(String name) {

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
