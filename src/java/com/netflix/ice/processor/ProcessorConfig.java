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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.AmazonEC2Exception;
import com.amazonaws.services.ec2.model.AvailabilityZone;
import com.amazonaws.services.ec2.model.DescribeAvailabilityZonesResult;
import com.amazonaws.services.organizations.model.Account;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.common.*;
import com.netflix.ice.processor.pricelist.PriceListService;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.Zone;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;

public class ProcessorConfig extends Config {
    private static final Logger logger = LoggerFactory.getLogger(ProcessorConfig.class);
    private static ProcessorConfig instance;
    protected static BillingFileProcessor billingFileProcessor;

    public final String startMonth;
    public final DateTime startDate;
    public final AccountService accountService;
    public final ResourceService resourceService;
    public final boolean familyRiBreakout;
    public final String[] billingAccountIds;
    public final String[] billingS3BucketNames;
    public final String[] billingS3BucketRegions;
    public final String[] billingS3BucketPrefixes;
    public final String[] billingAccessRoleNames;
    public final String[] billingAccessExternalIds;
    public final String[] kubernetesAccountIds;
    public final String[] kubernetesS3BucketNames;
    public final String[] kubernetesS3BucketRegions;
    public final String[] kubernetesS3BucketPrefixes;
    public final String[] kubernetesAccessRoleNames;
    public final String[] kubernetesAccessExternalIds;
    public final DateTime costAndUsageStartDate;
    public final DateTime costAndUsageNetUnblendedStartDate;
    public final SortedMap<DateTime,Double> edpDiscounts;

    public final ReservationService reservationService;
    public final PriceListService priceListService;
    public final boolean useBlended;
    public final boolean processOnce;
    public final String processorRegion;
    public final String processorInstanceId;
    public final int numthreads;

    public final String useCostForResourceGroup;
    public final JsonFiles writeJsonFiles;
    
    public enum JsonFiles {
    	no,
    	ndjson,
    	bulk   	
    }
    
    public final String kubernetesComputeTag;
    public final String kubernetesNamespaceTag;
    public final String[] kubernetesUserTags;
    public final String kubernetesClusterNameFormula;
    public final Map<String, String> kubernetesNamespaceMappingRules;

    /**
     *
     * @param properties (required)
     * @param accountService (required)
     * @param productService (required)
     * @param reservationService (required)
     * @param resourceService (optional)
     * @param randomizer (optional)
     */
    public ProcessorConfig(
            Properties properties,
            AWSCredentialsProvider credentialsProvider,
            ProductService productService,
            ReservationService reservationService,
            ResourceService resourceService,
            PriceListService priceListService,
            boolean compress) throws Exception {

        super(properties, credentialsProvider, productService);
        
        initZones();
        
        billingS3BucketNames = properties.getProperty(IceOptions.BILLING_S3_BUCKET_NAME).split(",");
        billingS3BucketRegions = properties.getProperty(IceOptions.BILLING_S3_BUCKET_REGION).split(",");
        billingS3BucketPrefixes = properties.getProperty(IceOptions.BILLING_S3_BUCKET_PREFIX, "").split(",");
        billingAccountIds = properties.getProperty(IceOptions.BILLING_PAYER_ACCOUNT_ID, "").split(",");
        billingAccessRoleNames = properties.getProperty(IceOptions.BILLING_ACCESS_ROLENAME, "").split(",");
        billingAccessExternalIds = properties.getProperty(IceOptions.BILLING_ACCESS_EXTERNALID, "").split(",");
        kubernetesS3BucketNames = properties.getProperty(IceOptions.KUBERNETES_S3_BUCKET_NAME, "").split(",");
        kubernetesS3BucketRegions = properties.getProperty(IceOptions.KUBERNETES_S3_BUCKET_REGION, "").split(",");
        kubernetesS3BucketPrefixes = properties.getProperty(IceOptions.KUBERNETES_S3_BUCKET_PREFIX, "").split(",");
        kubernetesAccountIds = properties.getProperty(IceOptions.KUBERNETES_ACCOUNT_ID, "").split(",");
        kubernetesAccessRoleNames = properties.getProperty(IceOptions.KUBERNETES_ACCESS_ROLENAME, "").split(",");
        kubernetesAccessExternalIds = properties.getProperty(IceOptions.KUBERNETES_ACCESS_EXTERNALID, "").split(",");
        
        kubernetesClusterNameFormula = properties.getProperty(IceOptions.KUBERNETES_CLUSTER_NAME_FORMULA, "");
        kubernetesComputeTag = properties.getProperty(IceOptions.KUBERNETES_COMPUTE_TAG, "");
        kubernetesNamespaceTag = properties.getProperty(IceOptions.KUBERNETES_NAMESPACE_TAG, "");
        
        String k8sUserTags = properties.getProperty(IceOptions.KUBERNETES_USER_TAGS, "");
        kubernetesUserTags = k8sUserTags.isEmpty() ? null : properties.getProperty(IceOptions.KUBERNETES_USER_TAGS, "").split(",");
        kubernetesNamespaceMappingRules = Maps.newHashMap();
        for (String name: properties.stringPropertyNames()) {
        	if (name.startsWith(IceOptions.KUBERNETES_NAMESPACE_TAGGING_RULE))
        		kubernetesNamespaceMappingRules.put(name.substring(IceOptions.KUBERNETES_NAMESPACE_TAGGING_RULE.length()), properties.getProperty(name));
        }

        Map<String, String> defaultNames = getDefaultAccountNames();
        this.accountService = new BasicAccountService(properties, defaultNames);
        
        if (properties.getProperty(IceOptions.START_MONTH) == null) throw new IllegalArgumentException("IceOptions.START_MONTH must be specified");
        this.startMonth = properties.getProperty(IceOptions.START_MONTH);        
        this.startDate = new DateTime(startMonth, DateTimeZone.UTC);
        this.resourceService = resourceService;
        
        // whether to separate out the family RI usage into its own operation category
        familyRiBreakout = properties.getProperty(IceOptions.FAMILY_RI_BREAKOUT) == null ? false : Boolean.parseBoolean(properties.getProperty(IceOptions.FAMILY_RI_BREAKOUT));
        
        if (reservationService == null) throw new IllegalArgumentException("reservationService must be specified");

        this.reservationService = reservationService;
        this.priceListService = priceListService;

        String[] yearMonth = properties.getProperty(IceOptions.COST_AND_USAGE_START_DATE, "").split("-");
        if (yearMonth.length < 2)
            costAndUsageStartDate = new DateTime(3000, 1, 1, 0, 0, DateTimeZone.UTC); // Arbitrary year in the future
        else
        	costAndUsageStartDate = new DateTime(Integer.parseInt(yearMonth[0]), Integer.parseInt(yearMonth[1]), 1, 0, 0, DateTimeZone.UTC);
        
        yearMonth = properties.getProperty(IceOptions.COST_AND_USAGE_NET_UNBLENDED_START_DATE, "").split("-");
        costAndUsageNetUnblendedStartDate = yearMonth.length < 2 ? null : new DateTime(Integer.parseInt(yearMonth[0]), Integer.parseInt(yearMonth[1]), 1, 0, 0, DateTimeZone.UTC);
        
        edpDiscounts = Maps.newTreeMap();
        String[] rates = properties.getProperty(IceOptions.EDP_DISCOUNTS, "").split(",");
        for (String rate: rates) {
        	String[] parts = rate.split(":");
        	if (parts.length < 2)
        		break;
        	edpDiscounts.put(new DateTime(parts[0], DateTimeZone.UTC), Double.parseDouble(parts[1]) / 100);
        }

        useBlended = properties.getProperty(IceOptions.USE_BLENDED) == null ? false : Boolean.parseBoolean(properties.getProperty(IceOptions.USE_BLENDED));

        //useCostForResourceGroup = properties.getProperty(IceOptions.RESOURCE_GROUP_COST, "modeled");
        useCostForResourceGroup = properties.getProperty(IceOptions.RESOURCE_GROUP_COST, "");
        writeJsonFiles = properties.getProperty(IceOptions.WRITE_JSON_FILES) == null ? JsonFiles.no : JsonFiles.valueOf(properties.getProperty(IceOptions.WRITE_JSON_FILES));
        
        processOnce = properties.getProperty(IceOptions.PROCESS_ONCE) == null ? false : Boolean.parseBoolean(properties.getProperty(IceOptions.PROCESS_ONCE));
        processorRegion = properties.getProperty(IceOptions.PROCESSOR_REGION);
        processorInstanceId = properties.getProperty(IceOptions.PROCESSOR_INSTANCE_ID);
        numthreads = properties.getProperty(IceOptions.PROCESSOR_THREADS) == null ? 5 : Integer.parseInt(properties.getProperty(IceOptions.PROCESSOR_THREADS));
        
        ProcessorConfig.instance = this;

        billingFileProcessor = new BillingFileProcessor(this, compress);
    }

    public void start () throws Exception {
        logger.info("starting up...");

        reservationService.init();
        if (resourceService != null)
            resourceService.init();

        priceListService.init();
        billingFileProcessor.start();
    }

    public void shutdown() {
        logger.info("Shutting down...");

        billingFileProcessor.shutdown();
        reservationService.shutdown();
    }
    
    /**
     * Return the EDP discount for the requested time
     * E.G. a 5% discount will return 0.05
     * @param dt
     * @return discount
     */
    public double getDiscount(DateTime dt) {
    	SortedMap<DateTime, Double> subMap = edpDiscounts.headMap(dt.plusSeconds(1));
    	return subMap.size() == 0 ? 0.0 : subMap.get(subMap.lastKey());
    }

    public double getDiscount(long startMillis) {
    	return getDiscount(new DateTime(startMillis));
    }

    /**
     * Return the discounted price
     * @param dt
     * @return discount
     */
    public double getDiscountedCost(DateTime dt, Double cost) {
    	return cost * (1 - getDiscount(dt));
    }

    /**
     *
     * @return singleton instance
     */
    public static ProcessorConfig getInstance() {
        return instance;
    }

    protected void initZones() {
        AmazonEC2ClientBuilder ec2Builder = AmazonEC2ClientBuilder.standard().withClientConfiguration(AwsUtils.clientConfig).withCredentials(AwsUtils.awsCredentialsProvider);
    	for (Region region: Region.getAllRegions()) {
            AmazonEC2 ec2 = ec2Builder.withRegion(region.name).build();
            try {
	            DescribeAvailabilityZonesResult result = ec2.describeAvailabilityZones();
	            for (AvailabilityZone az: result.getAvailabilityZones()) {
	            	region.addZone(az.getZoneName());
	            }
            }
            catch(AmazonEC2Exception e) {
            	logger.info("failed to get zones for region " + region + ", " + e.getErrorMessage());
            }
    	}
    }
        
    /**
     * Save the configuration items for the reader in the work bucket
     * @throws IOException 
     */
    public void saveWorkBucketDataConfig() throws IOException {
    	Map<String, List<String>> zones = Maps.newHashMap();
    	for (Region r: Region.getAllRegions()) {
    		List<String> zlist = Lists.newArrayList();
    		for (Zone z: r.getZones())
    			zlist.add(z.name);
    		zones.put(r.name, zlist);
    	}
    	WorkBucketDataConfig wbdc = new WorkBucketDataConfig(startMonth, accountService.getAccounts(), zones, resourceService == null ? null : resourceService.getUserTags(), familyRiBreakout);
        File file = new File(localDir, workBucketDataConfigFilename);
    	OutputStream os = new FileOutputStream(file);
    	OutputStreamWriter writer = new OutputStreamWriter(os);
    	writer.write(wbdc.toJSON());
    	writer.close();
    	
    	logger.info("Upload work bucket data config file");
    	AwsUtils.upload(workS3BucketName, workS3BucketPrefix, file);
    }
    
    /**
     * get all the accounts from the organizations service so we can add the names if they're not specified in the ice.properties file.
     */
    protected Map<String, String> getDefaultAccountNames() {
    	Map<String, String> result = Maps.newHashMap();
    	
        for (int i = 0; i < billingS3BucketNames.length; i++) {
            String accountId = billingAccountIds.length > i ? billingAccountIds[i] : "";
            String assumeRole = billingAccessRoleNames.length > i ? billingAccessRoleNames[i] : "";
            String externalId = billingAccessExternalIds.length > i ? billingAccessExternalIds[i] : "";
            
            List<Account> accounts = AwsUtils.listAccounts(accountId, assumeRole, externalId);
            for (Account a: accounts)
            	result.put(a.getId(), a.getName());
        }
    	return result;
    }
}
