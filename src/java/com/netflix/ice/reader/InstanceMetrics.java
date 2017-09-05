package com.netflix.ice.reader;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Maps;
import com.netflix.ice.tag.UsageType;

public class InstanceMetrics {
    public static final String dbName = "instanceMetrics";

    private Map<String, Metrics> metrics = Maps.newHashMap();
    
    public void load(DataInput in) throws IOException {
    	metrics = Serializer.deserialize(in);
    }
    
    public void archive(DataOutput out) throws IOException {
    	Serializer.serialize(out, metrics);
    }
    
    public void add(String name, int vCPU, double ecu, double normalizationSizeFactor) {
    	metrics.put(name, new Metrics(name, vCPU, ecu, normalizationSizeFactor));
    }
    
    private String getKey(UsageType usageType) {
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
    
    public double getECU(UsageType usageType) {
    	String key = getKey(usageType);
    	if (key == null)
    		return 1.0;
    	
    	Metrics m = metrics.get(key);
    	if (m == null)
    		return 1.0;
    	return m.ecu;
    }

    public double getVCpu(UsageType usageType) {
    	String key = getKey(usageType);
    	if (key == null)
    		return 1.0;
    	
    	Metrics m = metrics.get(key);
    	if (m == null)
    		return 1.0;
    	return m.vCPU;
    }
    
    public double getNormalizationFactor(UsageType usageType) {
    	String key = getKey(usageType);
    	if (key == null)
    		return 1.0;
    	
    	Metrics m = metrics.get(key);
    	if (m == null)
    		return 1.0;
    	return m.normalizationSizeFactor;
    }

    protected static class Metrics {
    	private final String name;
        private final int vCPU;
        private final double ecu;
        private final double normalizationSizeFactor;	// used for converting reservation units when sharing across family instance types
        Metrics(String name, int vCPU, double ecu, double normalizationSizeFactor) {
        	this.name = name;
            this.vCPU = vCPU;
            this.ecu = ecu;
            this.normalizationSizeFactor = normalizationSizeFactor;
        }
        
        public static class Serializer {
        	public static void serialize(DataOutput out, Metrics m) throws IOException {
        		out.writeUTF(m.name);
        		out.writeInt(m.vCPU);
        		out.writeDouble(m.ecu);
        		out.writeDouble(m.normalizationSizeFactor);
        	}
        	public static Metrics deserialize(DataInput in) throws IOException {
        		return new Metrics(in.readUTF(), in.readInt(), in.readDouble(), in.readDouble());
        	}
        }
    }
    
    public static class Serializer {
    	
        public static void serialize(DataOutput out, Map<String, Metrics> metrics) throws IOException {
        	out.writeInt(metrics.size());
        	for (Metrics m: metrics.values()) {
        		Metrics.Serializer.serialize(out, m);
        	}
        }
    	
        public static Map<String, Metrics> deserialize(DataInput in) throws IOException {
        	Map<String, Metrics> metrics = Maps.newHashMap();
        	
        	int size = in.readInt();
        	for (int i = 0; i < size; i++) {
	        	Metrics m = Metrics.Serializer.deserialize(in);
	        	metrics.put(m.name, m);
        	}
        	return metrics;
        }
    }	

}
