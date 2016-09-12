package com.netflix.ice.processor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.ice.common.AwsUtils;

public class Instances {
    private final static Logger logger = LoggerFactory.getLogger(Instances.class);
    
    class InstanceData {
    	private String type;
    	private String tags;
    	
    	InstanceData(String type, String tags) {
    		this.type = type;
    		this.tags = tags;
    	}
    	
		protected String csv() {
			return type + "," + tags;
		}
	}
	private Map<String, InstanceData> data;

	Instances() {
		data = new HashMap<String, InstanceData>();
	}
	
	public void add(String id, String type, String tags) {
		if (id.isEmpty()) {
			return;
		}
		data.put(id, new InstanceData(type, tags));
	}
	
    public void archive(ProcessorConfig config, String filename) throws IOException {
        File file = new File(config.localDir, filename + ".csv");

        // archive to disk
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(file));
            // Write the header
            writer.write("InstanceID,InstanceType,Tags");
            writer.newLine();
            for (String key: data.keySet()) {
                writer.write(key + "," + data.get(key).csv());
                writer.newLine();
            }
        }
        catch (Exception e) {
            logger.error("",  e);
        }
        finally {
            if (writer != null)
                try {writer.close();} catch (Exception e) {}
        }

        // archive to s3
        logger.info("uploading " + file + "...");
        AwsUtils.upload(config.workS3BucketName, config.workS3BucketPrefix, config.localDir, file.getName());
        logger.info("uploaded " + file);
    }	
}
