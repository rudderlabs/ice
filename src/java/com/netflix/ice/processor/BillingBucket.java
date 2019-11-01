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

public class BillingBucket {
    public final String accountId;
    public final String s3BucketName;
    public final String s3BucketRegion;
    public final String s3BucketPrefix;
    public final String accessRoleName;
    public final String accessExternalId;
    public final String rootName;
    
    public BillingBucket(String s3BucketName, String s3BucketRegion, String s3BucketPrefix, String accountId, String accessRoleName, String accessExternalId, String rootName) {
    	this.s3BucketName = s3BucketName;
    	this.s3BucketRegion = s3BucketRegion;
    	this.s3BucketPrefix = s3BucketPrefix;
    	this.accountId = accountId;
    	this.accessRoleName = accessRoleName;
    	this.accessExternalId = accessExternalId;
    	this.rootName = rootName;
    }
}