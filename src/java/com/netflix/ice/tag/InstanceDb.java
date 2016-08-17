package com.netflix.ice.tag;

public enum InstanceDb {
	// I have no idea what description fields are used for reservations other than mysql which is the
	// only one we're making reservations for at present.  jmr 8/11/16
	
    aurora(".aurora", ":0016", "aurora"), // "RDS running Amazon Aurora"
    mariadb(".mariadb", ":0018", "mariadb"), // "RDS running MariaDB (LI)"
    mysql(".mysql", ":0002", "mysql"), // "RDS running MySQL (BYOL)"
    oraclestd1byol(".oraclestd1byol", ":0003", "oraclestd1byol"), // "RDS running Oracle Standard One (BYOL)"
    oraclestdbyol(".oraclestdbyol", ":0004", "oraclestdbyol"), // "RDS running Oracle Standard (BYOL)"
    oracleentbyol(".oracleentbyol", ":0005", "oracleentbyol"), // "RDS running Oracle Enterprise (BYOL)"
    oraclestd1li(".oraclestd1li", ":0006", "oraclestd1li"), // "RDS running Oracle Standard One (LI)"
    oraclestd2byol(".oraclestd2byol", ":0019", "oraclestd2byol"), // "RDS running Oracle Standard Two (BYOL)"
    postgresql(".postgresql", ":0014", "postgresql"), // "RDS running PostgreSQL (BYOL)"
    sqlserverstdbyol(".sqlserverstdbyol", ":0008", "sqlserverstdbyol"), // "RDS running SQL Server Standard (BYOL)"
    sqlserverentbyol(".sqlserverentbyol", ":0009", "sqlserverentbyol"), // "RDS running SQL Server Enterprise (BYOL)"
    sqlserverexpli(".sqlserverexpli", ":0010", "sqlserverexpli"), // "RDS running SQL Server Express (LI)"
    sqlserverwebli(".sqlserverwebli", ":0011", "sqlserverwebli"), // "RDS running SQL Server Web (LI)"
    sqlserverstdli(".sqlserverstdli", ":0012", "sqlserverstdli"), // "RDS running SQL Server Standard (LI)"
    sqlserverentli(".sqlserverentli", ":0015", "sqlserverentli"), // "RDS running SQL Server Enterprise (LI)"
    others(".others", ":others", "others");

    public final String usageType;
    public final String code;
    public final String description;

    InstanceDb(String usageType, String code, String description) {
        this.usageType = usageType;
        this.code = code.toLowerCase();
        this.description = description.toLowerCase();
    }

    public static InstanceDb withCode(String code) {
        for (InstanceDb db: InstanceDb.values()) {
            if (code.toLowerCase().equals(db.code))
                return db;
        }
        return others;
    }

    /**
     * withDescription() returns the InstanceDb value based on the product description in an
     * RDS ReservedDBInstance object.
     */
    public static InstanceDb withDescription(String description) {
    	String descLC = description.toLowerCase();
        for (InstanceDb db: InstanceDb.values()) {
            if (descLC.toLowerCase().startsWith(db.description.toLowerCase()))
                return db;
        }
        return others;
    }
}
