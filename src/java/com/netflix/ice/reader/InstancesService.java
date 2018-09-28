package com.netflix.ice.reader;

import org.joda.time.DateTime;

import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.StalePoller;
import com.netflix.ice.processor.Instances;

public class InstancesService extends StalePoller {
	private final Instances instances;
	private final AccountService accountService;
	private final ProductService productService;

	public InstancesService(String localDir, String workS3BucketName, String workS3BucketPrefix, AccountService accountService, ProductService productService) {
		instances = new Instances(localDir, workS3BucketName, workS3BucketPrefix);
		this.accountService = accountService;
		this.productService = productService;
		
		start();
	}
	
	public Instances getInstances() {
		return instances;
	}

	@Override
	protected boolean stalePoll() throws Exception {
        logger.info("Instances start polling...");
        try {
        	// Ask for one day prior to make sure we've processed a report if at
        	// start of month.
        	instances.retrieve(DateTime.now().minusDays(1).getMillis(), accountService, productService);
        }
        catch (Exception e) {
            logger.error("failed to download instances data", e);
            return true;
        }
        return false;
	}

}
