/*
 *
 *  Copyright 2013 Netflix, Inc.
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
package com.netflix.ice.reader;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;

import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.StalePoller;

public class InstanceMetricsService extends StalePoller {
	private final String localDir;
	private final String workS3BucketName;
	private final String workS3BucketPrefix;
	private InstanceMetrics instanceMetrics;

    public InstanceMetricsService(String localDir, String workS3BucketName, String workS3BucketPrefix) {
    	this.localDir = localDir;
		this.workS3BucketName = workS3BucketName;
		this.workS3BucketPrefix = workS3BucketPrefix;
		this.instanceMetrics = null;
		
		start();
    }
	    
    /**
     * We check if new data is available periodically
     * @throws Exception
     */
    @Override
    protected boolean stalePoll() throws Exception {    	
        logger.info(InstanceMetrics.dbName + " start polling...");
        File file = new File(localDir, InstanceMetrics.dbName);
        try {
            logger.info("trying to download " + file);
            boolean downloaded = downloadFile(file);
            if (downloaded || (instanceMetrics == null && file.exists())) {
                loadDataFromFile(file);
            }
        }
        catch (Exception e) {
            logger.error("failed to download " + file, e);
            return true;
        }
        return false;
    }
    
    private void loadDataFromFile(File file) throws Exception {
        logger.info("trying to load data from " + file);
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        try {
            instanceMetrics = InstanceMetrics.Serializer.deserialize(in);
            logger.info("done loading data from " + file);
        }
        finally {
            in.close();
        }
    }

    private boolean downloadFile(File file) {
        try {
            return AwsUtils.downloadFileIfChanged(workS3BucketName, workS3BucketPrefix, file);
        }
        catch (Exception e) {
            logger.error("error downloading " + file + " from " + workS3BucketName + "/" + workS3BucketPrefix + file.getName(), e);
            return false;
        }
    }

    public InstanceMetrics getInstanceMetrics() {
    	if (instanceMetrics == null)
    		logger.error("No instance metrics");
    	return instanceMetrics;
    }
}
