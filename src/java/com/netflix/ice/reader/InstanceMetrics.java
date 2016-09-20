package com.netflix.ice.reader;

import java.util.Map;

import com.google.common.collect.Maps;
import com.netflix.ice.tag.UsageType;

public class InstanceMetrics {
    private static Map<String, Metrics> metricsByInstanceType = Maps.newHashMap();
    
    private static final Metrics[] metrics = new Metrics[]{
    	/* 			name,			vCPU,	ECU */
    	new Metrics("t1.micro",		1,		3.0),	// t1 and t2 ECUs are really variable

    	new Metrics("t2.nano",		1,		3.0),
    	new Metrics("t2.micro",		1,		3.0),
    	new Metrics("t2.small",		1,		3.0),
    	new Metrics("t2.medium",	2,		6.0),
    	new Metrics("t2.large",		2,		6.0),

    	new Metrics("m1.small",		1,		1),
    	new Metrics("m1.medium",	1,		2),
    	new Metrics("m1.large",		2,		4),
    	new Metrics("m1.xlarge",	4,		8),

    	new Metrics("m2.xlarge",	2,		6.5),
    	new Metrics("m2.2xlarge",	4,		13),
    	new Metrics("m2.4xlarge",	8,		26),    	
    	new Metrics("cr1.8xlarge",	32,		88),

    	new Metrics("m3.medium",	1,		3),
    	new Metrics("m3.large",		2,		6.5),
    	new Metrics("m3.xlarge",	4,		13),
    	new Metrics("m3.2xlarge",	8,		26),

    	new Metrics("m4.large",		2,		6.5),
    	new Metrics("m4.xlarge",	4,		13),
    	new Metrics("m4.2xlarge",	8,		26),
    	new Metrics("m4.4xlarge",	16,		53.5),
    	new Metrics("m4.10xlarge",	40,		124.5),

    	new Metrics("c1.medium",	2,		5),
    	new Metrics("c1.xlarge",	8,		20),
    	new Metrics("cc2.8xlarge",	32,		88),

    	new Metrics("c3.large",		2,		7),
    	new Metrics("c3.xlarge",	4,		14),
    	new Metrics("c3.2xlarge",	8,		28),
    	new Metrics("c3.4xlarge",	16,		55),
    	new Metrics("c3.8xlarge",	32,		108),

    	new Metrics("c4.large",		2,		8),
    	new Metrics("c4.xlarge",	4,		16),
    	new Metrics("c4.2xlarge",	8,		31),
    	new Metrics("c4.4xlarge",	16,		62),
    	new Metrics("c4.8xlarge",	36,		132),

    	new Metrics("cg1.4xlarge",	16,		33.5),
    	
    	new Metrics("g2.2xlarge",	8,		26),
    	new Metrics("g2.8xlarge",	32,		104),

    	new Metrics("x1.32xlarge",	128,	349),
    	
    	new Metrics("r3.large",		2,		6.5),
    	new Metrics("r3.xlarge",	4,		13),
    	new Metrics("r3.2xlarge",	8,		26),
    	new Metrics("r3.4xlarge",	16,		52),
    	new Metrics("r3.8xlarge",	32,		104),

    	new Metrics("hi1.4xlarge",	16,		35),
    	new Metrics("hs1.8xlarge",	16,		35),
    	
    	new Metrics("i2.xlarge",	4,		14),
    	new Metrics("i2.2xlarge",	8,		27),
    	new Metrics("i2.4xlarge",	16,		53),
    	new Metrics("i2.8xlarge",	32,		104),

    	new Metrics("d2.xlarge",	4,		14),
    	new Metrics("d2.2xlarge",	8,		28),
    	new Metrics("d2.4xlarge",	16,		56),
    	new Metrics("d2.8xlarge",	36,		116),
    	
    	// Redshift Instances
    	new Metrics("dc1.large",	2,		7),
    	new Metrics("dc1.8xlarge",	32,		104),
    	
    	new Metrics("ds1.xlarge",	2,		4.4),
    	new Metrics("ds1.8xlarge",	16,		35),
    	
    	new Metrics("ds2.xlarge",	4,		14),
    	new Metrics("ds2.8xlarge",	36,		116),
    	
    };
    
    static {
    	for (Metrics m: metrics) {
    		metricsByInstanceType.put(m.name, m);
    	}
    }
    
    private static String getKey(UsageType usageType) {
    	String name = usageType.name;
    	
    	// Strip off "db." if present - used by RDS
    	if (name.startsWith("db.")) {
    		name = name.substring("db.".length());
    	}
    	
    	// instance usage types can have multiple suffixes, so only take the first two elements
       	String[] parts = name.split("\\.", 3);
       	if (parts.length < 2) {
       		return null;
       	}
       	return parts[0] + "." + parts[1];
    }
    public static double getECU(UsageType usageType) {
    	String key = getKey(usageType);
    	if (key == null)
    		return 1.0;
    	
    	Metrics m = metricsByInstanceType.get(key);
    	if (m == null)
    		return 1.0;
    	return m.ecu;
    }

    public static double getVCpu(UsageType usageType) {
    	String key = getKey(usageType);
    	if (key == null)
    		return 1.0;
    	
    	Metrics m = metricsByInstanceType.get(key);
    	if (m == null)
    		return 1.0;
    	return m.vCPU;
    }

    private static class Metrics {
    	private final String name;
        private final double ecu;
        private final double vCPU;
        Metrics(String name, int vCPU, double ecu) {
        	this.name = name;
            this.vCPU = vCPU;
            this.ecu = ecu;
        }
    }
}
