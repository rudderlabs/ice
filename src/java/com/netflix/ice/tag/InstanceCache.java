package com.netflix.ice.tag;

public enum InstanceCache {
	memcached(".memcached", ":0001", "Running Memcached"),
	redis(".redis", ":0002", "Running Redis"),
	others(".others", ":others", "");
	
	public final String usageType;
	public final String code;
	public final String description;
	
	InstanceCache(String usageType, String code, String description) {
		this.usageType = usageType;
		this.code = code;
		this.description = description;
	}
	
    public static InstanceCache withCode(String code) {
        for (InstanceCache c: InstanceCache.values()) {
            if (code.toLowerCase().equals(c.code))
                return c;
        }
        return others;
    }

    /**
     * withDescription() returns the InstanceCache value based on the product description in an
     * ElasticCache ReservedCacheNode object.
     */
    public static InstanceCache withDescription(String description) {
    	String descLC = description.toLowerCase();
        for (InstanceCache c: InstanceCache.values()) {
            if (descLC.startsWith(c.description.toLowerCase()))
                return c;
        }
        return others;
    }

}
