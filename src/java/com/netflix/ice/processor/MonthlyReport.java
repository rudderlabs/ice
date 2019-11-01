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

public abstract class MonthlyReport extends Report {
    final MonthlyReportProcessor processor;

    MonthlyReport(S3ObjectSummary s3ObjectSummary, BillingBucket billingBucket, MonthlyReportProcessor processor) {
    	super(s3ObjectSummary, billingBucket);
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
