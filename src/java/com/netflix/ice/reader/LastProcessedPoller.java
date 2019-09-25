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

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.Poller;

/**
 * LastProcessedPoller will periodically scan the timestamps from all the lastProcessedMillis_YYYY-MM files
 * to determine the latest timestamp from all the monthly files.
 */
public class LastProcessedPoller extends Poller {
	
    private ReaderConfig config = ReaderConfig.getInstance();
    private DateTime startDate;
    private final String dbName;
    private Long lastProcessedMillis;

	public LastProcessedPoller(DateTime startDate) {
		this.startDate = startDate;
		this.dbName = "lastProcessMillis";
		this.lastProcessedMillis = 0L;
		
		// Do the initial poll now
		try {
			poll();
		} catch (Exception e) {
			logger.error("Initial poll failed", e);
		}
		
		// Check every 5 minutes
		start(5*60, 5*60, false);
	}
	
	public Long getLastProcessedMillis() {
		return lastProcessedMillis;
	}

	@Override
	protected void poll() throws Exception {
        logger.info(dbName + " start polling...");
        Long oldLastProcessedMillis = lastProcessedMillis;
        for (DateTime month = startDate; month.isBefore(DateTime.now()); month = month.plusMonths(1)) {
        	Long lastProcessedForMonth = getLastMillis(month);
        	if (lastProcessedForMonth > lastProcessedMillis)
        		lastProcessedMillis = lastProcessedForMonth;
        }
        if (lastProcessedMillis > oldLastProcessedMillis)
        	logger.info("Data updated at " + lastProcessedMillis);
        //else
        //	logger.info("No updates since " + oldLastProcessedMillis);
	}
	
    private Long getLastMillis(DateTime monthDate) {
    	String filename = dbName + "_" + AwsUtils.monthDateFormat.print(monthDate);
    	
        AmazonS3Client s3Client = AwsUtils.getAmazonS3Client();
        InputStream in = null;
        try {
            in = s3Client.getObject(config.workS3BucketName, config.workS3BucketPrefix + filename).getObjectContent();
            Long millis = Long.parseLong(IOUtils.toString(in, StandardCharsets.UTF_8));
            //logger.info(filename + ": " + millis);
            return millis;
        }
        catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404)
            	logger.warn("File not found in s3: " + filename);
            else
            	logger.error("Error reading from file " + filename, e);
            return 0L;
        }
        catch (Exception e) {
            logger.error("Error reading from file " + filename, e);
            return 0L;
        }
        finally {
            if (in != null)
                try {in.close();} catch (Exception e){}
        }
    }
}
