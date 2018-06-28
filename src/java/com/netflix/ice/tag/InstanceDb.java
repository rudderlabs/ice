package com.netflix.ice.tag;

public enum InstanceDb {
	// I have no idea what description fields are used for reservations other than mysql which is the
	// only one we're making reservations for at present.  jmr 8/11/16
	
    aurora(".aurora", ":0016", "aurora"), // "RDS running Amazon Aurora"
    aurorapg(".aurorapg", ":0021", "aurorapg"), // "RDS running Amazon Aurora PostgreSQL"
    mariadb(".mariadb", ":0018", "mariadb"), // "RDS running MariaDB (LI)"
    mysql(".mysql", ":0002", "mysql"), // "RDS running MySQL (BYOL)"
    orclstd1byol(".orclstd1byol", ":0003", "orclstd1byol"), // "RDS running Oracle Standard One (BYOL)"
    orclstdbyol(".orclstdbyol", ":0004", "orclstdbyol"), // "RDS running Oracle Standard (BYOL)"
    orclentbyol(".orclentbyol", ":0005", "orclentbyol"), // "RDS running Oracle Enterprise (BYOL)"
    orclstd1(".orclstd1", ":0006", "orclstd1"), // "RDS running Oracle Standard One"
    orclstd2byol(".orclstd2byol", ":0019", "orclstd2byol"), // "RDS running Oracle Standard Two (BYOL)"
    orclstd2(".orclstd2", ":0020", "orclstd2"), // "RDS running Oracle Standard Two"
    postgres(".postgres", ":0014", "postgres"), // "RDS running PostgreSQL (BYOL)"
    sqlstdbyol(".sqlstdbyol", ":0008", "sqlstdbyol"), // "RDS running SQL Server Standard (BYOL)"
    sqlentbyol(".sqlentbyol", ":0009", "sqlentbyol"), // "RDS running SQL Server Enterprise (BYOL)"
    sqlexp(".sqlexp", ":0010", "sqlexp"), // "RDS running SQL Server Express"
    sqlweb(".sqlweb", ":0011", "sqlweb"), // "RDS running SQL Server Web"
    sqlstd(".sqlstd", ":0012", "sqlstd"), // "RDS running SQL Server Standard"
    sqlent(".sqlent", ":0015", "sqlent"), // "RDS running SQL Server Enterprise"
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
