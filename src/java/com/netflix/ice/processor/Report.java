package com.netflix.ice.processor;

import com.amazonaws.services.s3.model.S3ObjectSummary;

public class Report {

	protected final S3ObjectSummary s3ObjectSummary;
	protected final String region;
	protected final String accountId;
	protected final String accessRoleName;
	protected final String externalId;
	protected final String prefix;

	public Report(S3ObjectSummary s3ObjectSummary, String region, String accountId, String accessRoleName, String externalId, String prefix) {
        this.s3ObjectSummary = s3ObjectSummary;
        this.region = region;
        this.accountId = accountId;
        this.accessRoleName = accessRoleName;
        this.externalId = externalId;
        this.prefix = prefix;
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

	public String getRegion() {
		return region;
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

}