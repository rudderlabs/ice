package com.netflix.ice.tag;

public enum InstanceDb {
    aurora("", ":0016", "RDS running Amazon Aurora"),
    mariadb("", ":0018", "RDS running MariaDB (LI)"),
    mysql("", ":0002", "RDS running MySQL (BYOL)"),
    oraclestd1byol("", ":0003", "RDS running Oracle Standard One (BYOL)"),
    oraclestdbyol("", ":0004", "RDS running Oracle Standard (BYOL)"),
    oracleentbyol("", ":0005", "RDS running Oracle Enterprise (BYOL)"),
    oraclestd1li("", ":0006", "RDS running Oracle Standard One (LI)"),
    oraclestd2byol("", ":0019", "RDS running Oracle Standard Two (BYOL)"),
    postgresql("", ":0014", "RDS running PostgreSQL (BYOL)"),
    sqlserverstdbyol("", ":0008", "RDS running SQL Server Standard (BYOL)"),
    sqlserverentbyol("", ":0009", "RDS running SQL Server Enterprise (BYOL)"),
    sqlserverexpli("", ":0010", "RDS running SQL Server Express (LI)"),
    sqlserverwebli("", ":0011", "RDS running SQL Server Web (LI)"),
    sqlserverstdli("", ":0012", "RDS running SQL Server Standard (LI)"),
    sqlserverentli("", ":0015", "RDS running SQL Server Enterprise (LI)"),
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

    public static InstanceDb withDescription(String description) {
        for (InstanceDb db: InstanceDb.values()) {
            if (description.toLowerCase().startsWith(db.description))
                return db;
        }
        return others;
    }
}
