package com.netflix.ice.processor;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.concurrent.ConcurrentMap;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.Zone;

public class Instances {
    private final static Logger logger = LoggerFactory.getLogger(Instances.class);
    
    class InstanceData {
    	private String type;
    	private String tags;
    	private Account account;
    	private Region region;
    	private Zone zone;
    	
    	InstanceData(String type, String tags, Account account, Region region, Zone zone) {
    		this.type = type;
    		this.tags = tags;
    		this.account = account;
    		this.region = region;
    		this.zone = zone;
    	}
    	
		protected String csv() {
			return type + "," + tags + "," + account.id + "," + account.name + "," + region + "," + (zone == null ? "" : zone);
		}
	}
	private ConcurrentMap<String, InstanceData> data;

	Instances() {
		data = Maps.newConcurrentMap();
	}
	
	public void add(String id, String type, String tags, Account account, Region region, Zone zone) {
		if (id.isEmpty()) {
			return;
		}
		data.put(id, new InstanceData(type, tags, account, region, zone));
	}
	
    public void archive(ProcessorConfig config, String filename) throws IOException {
        File file = new File(config.localDir, filename + ".csv.gz");
    	OutputStream os = new FileOutputStream(file);
		os = new GZIPOutputStream(os);        
		Writer out = new OutputStreamWriter(os);
        
        try {
        	// Write the header
        	out.write("InstanceID,InstanceType,Tags,AccountId,AccountName,Region,Zone\n");
            for (String key: data.keySet()) {
                out.write(key + "," + data.get(key).csv() + "\n");
            }
        	out.flush();
        }
        finally {
            out.close();
        }

        // archive to s3
        logger.info("uploading " + file + "...");
        AwsUtils.upload(config.workS3BucketName, config.workS3BucketPrefix, config.localDir, file.getName());
        logger.info("uploaded " + file);
    }	
}
