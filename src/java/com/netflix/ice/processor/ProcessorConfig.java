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
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.common.*;
import com.netflix.ice.processor.config.AccountConfig;
import com.netflix.ice.processor.config.BillingDataConfig;
import com.netflix.ice.processor.config.KubernetesConfig;
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
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
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
    
    // Kubernetes configuration data keyed by payer account ID
    public List<KubernetesConfig> kubernetesConfigs;
    
    private static final String billingDataConfigBasename = "ice_config.";

    /**
     *
     * @param properties (required)
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
                
        if (reservationService == null)
        	throw new IllegalArgumentException("reservationService must be specified");

        this.reservationService = reservationService;
        this.priceListService = priceListService;
        this.resourceService = resourceService;

    	Map<String, String> defaultNames = getDefaultAccountNames();
        Map<String, AccountConfig> accountConfigs = getAccountConfigs(properties, defaultNames);
        processBillingDataConfig(accountConfigs, defaultNames);
        this.accountService = new BasicAccountService(accountConfigs);
        
        if (properties.getProperty(IceOptions.START_MONTH) == null) throw new IllegalArgumentException("IceOptions.START_MONTH must be specified");
        this.startMonth = properties.getProperty(IceOptions.START_MONTH);        
        this.startDate = new DateTime(startMonth, DateTimeZone.UTC);
        
        // whether to separate out the family RI usage into its own operation category
        familyRiBreakout = properties.getProperty(IceOptions.FAMILY_RI_BREAKOUT) == null ? false : Boolean.parseBoolean(properties.getProperty(IceOptions.FAMILY_RI_BREAKOUT));
        

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
    	WorkBucketDataConfig wbdc = new WorkBucketDataConfig(startMonth, accountService.getAccounts(), zones,
    			resourceService == null ? null : resourceService.getUserTags(), familyRiBreakout, getTagCoverage());
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
    	Set<String> done = Sets.newHashSet();
    	
        for (int i = 0; i < billingS3BucketNames.length; i++) {
            String accountId = billingAccountIds[i];
            String assumeRole = billingAccessRoleNames.length > i ? billingAccessRoleNames[i] : "";
            String externalId = billingAccessExternalIds.length > i ? billingAccessExternalIds[i] : "";
            
            // Only process each payer account once. Can have two if processing both DBRs and CURs
            if (done.contains(accountId))
            	continue;            
            done.add(accountId);
            
            logger.info("Get default account names for organization " + accountId +
            		" using assume role \"" + assumeRole + "\", and external id \"" + externalId + "\"");
            List<Account> accounts = AwsUtils.listAccounts(accountId, assumeRole, externalId);
            for (Account a: accounts)
            	result.put(a.getId(), a.getName());
        }
    	return result;
    }
    
    /*
     * Pull account configs from the properties
     */
    private Map<String, AccountConfig> getAccountConfigs(Properties properties, Map<String, String> defaultNames) {
    	Map<String, AccountConfig> accountConfigs = Maps.newHashMap();
    	
        if (defaultNames != null) {
	        // Start with all the default names in the map
	        for (Entry<String, String> entry: defaultNames.entrySet()) {
	            accountConfigs.put(entry.getKey(), new AccountConfig(entry.getKey(), entry.getValue(), entry.getValue(), null, null, null, null));
	        }
        }
    	
        /* 
         * Overwrite with any definitions in the properties. The following property key values are used:
         * 
         * Account definition:
         * 		name:	account name
         * 		id:		account id
         * 
         *	ice.account.{name}={id}
         *
         *		example: ice.account.myAccount=123456789012
         * 
         * Reservation Owner Account
         * 		name: account name
    	 *		product: codes for products with purchased reserved instances. Possible values are ec2, rds, redshift
         * 
         *	ice.owneraccount.{name}={products}
         *
         *		example: ice.owneraccount.resHolder=ec2,rds
         *
         * Reservation Owner Account Role
         * 		name: account name
         * 		role: IAM role name to assume when pulling reservations from an owner account
         * 
         * 	ice.owneraccount.{name}.role={role}
         * 
         * 		example: ice.owneraccount.resHolder.role=ice
         * 
         * Reservation Owner Account ExternalId
         * 		name: account name
         * 		externalId: external ID for the reservation owner account
         * 
         * 	ice.owneraccount.{name}.externalId={externalId}
         * 
         * 		example: ice.owneraccount.resHolder.externalId=112233445566
         */
        for (String name: properties.stringPropertyNames()) {
            if (name.startsWith("ice.account.")) {
                String accountName = name.substring("ice.account.".length());
                String id = properties.getProperty(name);
                String ownerAccount = "ice.owneraccount." + accountName;
                String[] products = properties.getProperty(ownerAccount, "").split(",");
                String role = null;
                String externalId = null;
                List<String> riProducts = null;
                if (!products[0].isEmpty()) {
                	role = properties.getProperty(ownerAccount + ".role");
                	externalId = properties.getProperty(ownerAccount + ".externalId");
                    riProducts = Lists.newArrayList(products);
                }
                accountConfigs.put(id, new AccountConfig(id, accountName, defaultNames.get(id), null, riProducts, role, externalId));
            }
        }
        return accountConfigs;
    }
    

    
    /**
     * get the billing data configurations specified along side the billing reports and override any account names and default tagging
     */
    protected void processBillingDataConfig(Map<String, AccountConfig> accountConfigs, Map<String, String> defaultNames) {
    	kubernetesConfigs = Lists.newArrayList();
    	
        for (int i = 0; i < billingS3BucketNames.length; i++) {
        	String bucket = billingS3BucketNames[i];
        	String region = billingS3BucketRegions[i];
        	String prefix = billingS3BucketPrefixes[i];
            String accountId = billingAccountIds[i];
            String roleName = billingAccessRoleNames.length > i ? billingAccessRoleNames[i] : "";
            String externalId = billingAccessExternalIds.length > i ? billingAccessExternalIds[i] : "";
            
        	BillingDataConfig billingDataConfig = readBillingDataConfig(bucket, region, prefix, accountId, roleName, externalId);
        	if (billingDataConfig == null)
        		continue;
        	
        	for (AccountConfig account: billingDataConfig.accounts) {
        		if (account.id == null || account.id.isEmpty())
        			continue;
        		account.awsName = defaultNames.get(account.id);
        		accountConfigs.put(account.id, account);
    			
            	resourceService.putDefaultTags(account.id, account.tags);        			
        	}
        	
        	resourceService.setTagConfigs(accountId, billingDataConfig.tags);
        	List<KubernetesConfig> k = billingDataConfig.getKubernetes();
        	if (k != null)
        		kubernetesConfigs.addAll(k);
        }
    }
    
    private BillingDataConfig readBillingDataConfig(String bucket, String region, String prefix, String accountId, String roleName, String externalId) {
    	// Make sure prefix ends with /
    	prefix = prefix.endsWith("/") ? prefix : prefix + "/";
    	
    	List<S3ObjectSummary> configFiles = AwsUtils.listAllObjects(bucket, region, prefix + billingDataConfigBasename, accountId, roleName, externalId);
    	if (configFiles.size() == 0)
    		return null;
    	
    	String fileKey = configFiles.get(0).getKey();
        File file = new File(localDir, fileKey.substring(prefix.length()));
        
		boolean downloaded = AwsUtils.downloadFileIfChangedSince(bucket, region, prefix, file, 0, accountId, roleName, externalId);
    	if (downloaded) {
        	String body;
			try {
				body = new String(Files.readAllBytes(file.toPath()), "UTF-8");
			} catch (IOException e) {
				logger.error("Error reading account properties " + e);
				return null;
			}
        	logger.info("downloaded billing data config: " + bucket + "/" + fileKey);
        	try {
				return new BillingDataConfig(body);
			} catch (Exception e) {
				logger.error("Failed to parse billing data config: " + bucket + "/" + fileKey);
				e.printStackTrace();
				return null;
			}    	
    	}
    	return null;
    }

}
