package com.netflix.ice.reader;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.Poller;

public class InstanceMetricsService extends Poller {
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
    protected void poll() throws Exception {
        logger.info(InstanceMetrics.dbName + " start polling...");
        File file = new File(localDir, InstanceMetrics.dbName);
        try {
            logger.info("trying to download " + file);
            boolean downloaded = downloadFile(file);
            if (downloaded) {
                loadDataFromFile(file);
            }
        }
        catch (Exception e) {
            logger.error("failed to download " + file, e);
        }
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
            return AwsUtils.downloadFileIfChanged(workS3BucketName, workS3BucketPrefix, file, 0);
        }
        catch (Exception e) {
            logger.error("error downloading " + file, e);
            return false;
        }
    }

    public InstanceMetrics getInstanceMetrics() {
    	return instanceMetrics;
    }
}
