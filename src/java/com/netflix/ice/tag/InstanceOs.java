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
    linux(Product.ec2,"", "", "Linux/UNIX"),
    sqlserverweb(Product.ec2,".sqlserverweb", ":0202", "Windows with SQL Server Web"),
    sqlserverstd(Product.ec2,".sqlserverstd", ":0006", "Windows with SQL Server Standard"),
    sles(Product.ec2,".sles", ":000g", "SUSE Linux"),
    rhel(Product.ec2,".rhel", ":0010", "Red Hat Enterprise Linux"),
    rhbl(Product.ec2,".rhbl", ":00g0", "Red Hat BYOL Linux"),
    windows(Product.ec2,".windows", ":0002", "Windows"),
    dw(Product.redshift,".dw", ":0001", "redshift"),
    mysql(Product.rds,"", ":0002", "RDS running MySQL = BYOL"),
    sqlserver(Product.rds,"", ":0012", "RDS running SQL Server SE (LI)"),
    others(null,".others", ":others", "others");

    public final Product product;
    public final String usageType;
    public final String code;
    public final String description;

    InstanceOs(Product product, String usageType, String code, String description) {
    	this.product = product;
        this.usageType = usageType;
        this.code = code.toLowerCase();
        this.description = description.toLowerCase();
    }

    public static InstanceOs withCode(Product product, String code) {
        for (InstanceOs os: InstanceOs.values()) {
            if (product == os.product && code.toLowerCase().equals(os.code))
                return os;
        }
        return others;
    }

    public static InstanceOs withDescription(String description) {
        for (InstanceOs os: InstanceOs.values()) {
            if (description.toLowerCase().startsWith(os.description))
                return os;
        }
        return others;
    }
}
