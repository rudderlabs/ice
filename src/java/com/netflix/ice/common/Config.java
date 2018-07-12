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
package com.netflix.ice.common;

import java.util.Properties;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.AvailabilityZone;
import com.amazonaws.services.ec2.model.DescribeAvailabilityZonesResult;
import com.netflix.ice.tag.Region;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public abstract class Config {

    public final String workS3BucketName;
    public final String workS3BucketRegion;
    public final String workS3BucketPrefix;
    public final String localDir;
    public final AccountService accountService;
    public final ProductService productService;
    public final ResourceService resourceService;
    public final DateTime startDate;
    public final AWSCredentialsProvider credentialsProvider;
    public final boolean familyRiBreakout;

    /**
     *
     * @param properties (required)
     * @param credentialsProvider (required)
     * @param accountService (required)
     * @param productService (required)
     * @param resourceService (optional)
     */
    public Config(
            Properties properties,
            AWSCredentialsProvider credentialsProvider,
            AccountService accountService,
            ProductService productService,
            ResourceService resourceService) {
        if (properties == null) throw new IllegalArgumentException("properties must be specified");
        if (properties.getProperty(IceOptions.START_MONTH) == null) throw new IllegalArgumentException("IceOptions.START_MONTH must be specified");
        if (credentialsProvider == null) throw new IllegalArgumentException("credentialsProvider must be specified");
        if (accountService == null) throw new IllegalArgumentException("accountService must be specified");
        if (productService == null) throw new IllegalArgumentException("productService must be specified");

        String[] yearMonth = properties.getProperty(IceOptions.START_MONTH).split("-");
        DateTime startDate = new DateTime(Integer.parseInt(yearMonth[0]), Integer.parseInt(yearMonth[1]), 1, 0, 0, DateTimeZone.UTC);
        workS3BucketName = properties.getProperty(IceOptions.WORK_S3_BUCKET_NAME);
        workS3BucketRegion = properties.getProperty(IceOptions.WORK_S3_BUCKET_REGION);
        workS3BucketPrefix = properties.getProperty(IceOptions.WORK_S3_BUCKET_PREFIX);
        localDir = properties.getProperty(IceOptions.LOCAL_DIR);
        
        // whether to separate out the family RI usage into its own operation category
        familyRiBreakout = properties.getProperty(IceOptions.FAMILY_RI_BREAKOUT) == null ? false : Boolean.parseBoolean(properties.getProperty(IceOptions.FAMILY_RI_BREAKOUT));

        if (workS3BucketName == null) throw new IllegalArgumentException("IceOptions.WORK_S3_BUCKET_NAME must be specified");
        if (workS3BucketRegion == null) throw new IllegalArgumentException("IceOptions.WORK_S3_BUCKET_REGION must be specified");

        this.credentialsProvider = credentialsProvider;
        this.startDate = startDate;
        this.accountService = accountService;
        this.productService = productService;
        this.resourceService = resourceService;

        AwsUtils.init(credentialsProvider, workS3BucketRegion);
        
        initZones();
    }
    
    protected void initZones() {
        AmazonEC2ClientBuilder ec2Builder = AmazonEC2ClientBuilder.standard().withClientConfiguration(AwsUtils.clientConfig).withCredentials(AwsUtils.awsCredentialsProvider);
    	for (Region region: Region.getAllRegions()) {
            AmazonEC2 ec2 = ec2Builder.withRegion(region.name).build();
            DescribeAvailabilityZonesResult result = ec2.describeAvailabilityZones();
            for (AvailabilityZone az: result.getAvailabilityZones()) {
            	region.addZone(az.getZoneName());
            }
    	}
    }
}
