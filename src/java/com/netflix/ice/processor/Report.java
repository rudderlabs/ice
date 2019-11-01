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
package com.netflix.ice.processor;

import com.amazonaws.services.s3.model.S3ObjectSummary;

public class Report {

	protected final S3ObjectSummary s3ObjectSummary;
	protected final BillingBucket billingBucket;

	public Report(S3ObjectSummary s3ObjectSummary, BillingBucket billingBucket) {
        this.s3ObjectSummary = s3ObjectSummary;
        this.billingBucket = billingBucket;
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

	public BillingBucket getBillingBucket() {
		return billingBucket;
	}

}