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

public enum InstanceOs {
    linux("", "", "Linux/UNIX"),
    sqlserverweb(".sqlserverweb", ":0202", "Windows with SQL Server Web"),
    sqlserverstd(".sqlserverstd", ":0006", "Windows with SQL Server Standard"),
    sles(".sles", ":000g", "SUSE Linux"),
    rhel(".rhel", ":0010", "Red Hat Enterprise Linux"),
    rhbl(".rhbl", ":00g0", "Red Hat BYOL Linux"),
    windows(".windows", ":0002", "Windows"),
    dw(".dw", ":0001", "redshift"),
    // Linux/UNIX Spot Instance - Number is the Zone # (Haven't seen example for windows yet -jroth)
    // VPC Spot instance starts with SV followed by three digit zone number - ":SV000"
    spot(".spot", ":S0000", ""),
    others(".others", ":others", "others");

    public final String usageType;
    public final String code;
    public final String description;

    InstanceOs(String usageType, String code, String description) {
        this.usageType = usageType;
        this.code = code.toLowerCase();
        this.description = description.toLowerCase();
    }

    public static InstanceOs withCode(String code) {
        for (InstanceOs os: InstanceOs.values()) {
            if (code.toLowerCase().equals(os.code))
                return os;
        }
        if (code.startsWith(":S"))
        	return spot;
        
        return others;
    }

    /**
     * withDescription() returns the InstanceOs value based on the product description in a
     * EC2 ReservedInstance object.
     */
    public static InstanceOs withDescription(String description) {
    	String descLC = description.toLowerCase();
        for (InstanceOs os: InstanceOs.values()) {
            if (descLC.startsWith(os.description.toLowerCase()))
                return os;
        }
        return others;
    }
}
