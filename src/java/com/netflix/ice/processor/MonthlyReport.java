package com.netflix.ice.processor;

import com.amazonaws.services.s3.model.S3ObjectSummary;

public abstract class MonthlyReport {
    final S3ObjectSummary s3ObjectSummary;
    final String accountId;
    final String accessRoleName;
    final String externalId;
    final String prefix;
    final MonthlyReportProcessor processor;

    MonthlyReport(S3ObjectSummary s3ObjectSummary, String accountId, String accessRoleName, String externalId, String prefix, MonthlyReportProcessor processor) {
        this.s3ObjectSummary = s3ObjectSummary;
        this.accountId = accountId;
        this.accessRoleName = accessRoleName;
        this.externalId = externalId;
        this.prefix = prefix;
        this.processor = processor;
    }
    
	public long getLastModifiedMillis() {
		return s3ObjectSummary.getLastModified().getTime();
	}

	public String getReportKey() {
		return s3ObjectSummary.getKey();
	}
	
	public S3ObjectSummary getS3ObjectSummary() {
		return s3ObjectSummary;
	}

	public String getAccountId() {
		return accountId;
	}

	public String getAccessRoleName() {
		return accessRoleName;
	}

	public String getExternalId() {
		return externalId;
	}

	public String getPrefix() {
		return prefix;
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
