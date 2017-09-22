package com.netflix.ice.reader;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Maps;
import com.netflix.ice.processor.pricelist.InstancePrices.ServiceCode;
import com.netflix.ice.tag.UsageType;

public class InstanceMetrics {
    public static final String dbName = "instanceMetrics";

    private Map<ServiceCode, String> priceListVersions = Maps.newHashMap();
    
    private Map<String, Metrics> metrics = Maps.newHashMap();
    
	public String getPriceListVersion(ServiceCode sc) {
		return priceListVersions.get(sc);
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
    	
        public static void serialize(DataOutput out, InstanceMetrics im) throws IOException {
        	out.writeInt(im.priceListVersions.size());
        	for (Entry<ServiceCode, String> v: im.priceListVersions.entrySet()) {
            	out.writeUTF(v.getKey().name());
            	out.writeUTF(v.getValue());
        	}
        	
        	out.writeInt(im.metrics.size());
        	for (Metrics m: im.metrics.values()) {
        		Metrics.Serializer.serialize(out, m);
        	}
        }
    	
        public static InstanceMetrics deserialize(DataInput in) throws IOException {
        	InstanceMetrics im = new InstanceMetrics();
        	
        	im.priceListVersions = Maps.newHashMap();
        	int size = in.readInt();
        	for (int i = 0; i < size; i++) {
        		ServiceCode sc = ServiceCode.valueOf(in.readUTF());
        		im.priceListVersions.put(sc, in.readUTF());
        	}
        	
        	im.metrics = Maps.newHashMap();
        	
        	size = in.readInt();
        	for (int i = 0; i < size; i++) {
	        	Metrics m = Metrics.Serializer.deserialize(in);
	        	im.metrics.put(m.name, m);
        	}
        	return im;
        }
    }	

}
