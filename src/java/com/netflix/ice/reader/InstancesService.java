package com.netflix.ice.reader;

import org.joda.time.DateTime;

import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.Poller;
import com.netflix.ice.processor.Instances;

public class InstancesService extends Poller {
	private Instances instances;
	private AccountService accountService;

	public InstancesService(String localDir, String workS3BucketName, String workS3BucketPrefix, AccountService accountService) {
		instances = new Instances(localDir, workS3BucketName, workS3BucketPrefix);
		this.accountService = accountService;
		
		start();
	}
	
	public Instances getInstances() {
		return instances;
	}

	@Override
	protected void poll() throws Exception {
        logger.info("Instances start polling...");
        try {
        	// Ask for one day prior to make sure we've processed a report if at
        	// start of month.
        	instances.retrieve(DateTime.now().minusDays(1).getMillis(), accountService);
        }
        catch (Exception e) {
            logger.error("failed to download instances data", e);
        }
	}

}
