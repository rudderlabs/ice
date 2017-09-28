/*
 *
 *  Copyright 2016 TiVo, Inc.
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
package com.netflix.ice.processor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
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
	private ConcurrentMap<String, InstanceData> data;

	Instances() {
		data = Maps.newConcurrentMap();
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
