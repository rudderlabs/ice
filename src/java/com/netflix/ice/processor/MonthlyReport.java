package com.netflix.ice.processor;

import com.amazonaws.services.s3.model.S3ObjectSummary;

public abstract class MonthlyReport extends Report {
    final MonthlyReportProcessor processor;

    MonthlyReport(S3ObjectSummary s3ObjectSummary, String region, String accountId, String accessRoleName, String externalId, String prefix, MonthlyReportProcessor processor) {
    	super(s3ObjectSummary, region, accountId, accessRoleName, externalId, prefix);
        this.processor = processor;
    }
    
	public MonthlyReportProcessor getProcessor() {
		return processor;
	}
	
	
	abstract public boolean hasTags();
	
	/*
	 * If report has multiple files, getReportKeys will return the keys to the files.
	 */
	abstract public String[] getReportKeys();

}
