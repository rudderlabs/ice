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
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.AmazonEC2Exception;
import com.amazonaws.services.ec2.model.AvailabilityZone;
import com.amazonaws.services.ec2.model.DescribeAvailabilityZonesResult;
import com.amazonaws.services.ec2.model.DescribeRegionsResult;
import com.amazonaws.services.organizations.model.Account;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicResourceService;
import com.netflix.ice.common.*;
import com.netflix.ice.processor.config.AccountConfig;
import com.netflix.ice.processor.config.BillingDataConfig;
import com.netflix.ice.processor.config.KubernetesConfig;
import com.netflix.ice.processor.postproc.RuleConfig;
import com.netflix.ice.processor.pricelist.PriceListService;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.Zone;
import com.netflix.ice.tag.Zone.BadZone;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
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
    public final List<BillingBucket> billingBuckets;
    public final List<BillingBucket> kubernetesBuckets;
    public final DateTime costAndUsageStartDate;
    public final DateTime costAndUsageNetUnblendedStartDate;
    public final SortedMap<DateTime, Double> edpDiscounts;

    public final ReservationService reservationService;
    public final ReservationCapacityPoller reservationCapacityPoller;
    public final PriceListService priceListService;
    public final boolean useBlended;
    public final boolean processOnce;
    public final String processorRegion;
    public final String processorInstanceId;

    public final String useCostForResourceGroup;
    public final List<JsonFileType> jsonFiles;

    public enum JsonFileType {
        hourly, // generate hourly newline delimited JSON records - one record per line
        hourlyRI, // generate hourly newline delimited JSON records with RI rates for
                  // product/operations that offer reserved instances
        daily; // generate daily newline delimited JSON records - one record per line
    }

    // Kubernetes configuration data keyed by payer account ID
    public List<KubernetesConfig> kubernetesConfigs;
    // Post=processor configuration rules
    public List<RuleConfig> postProcessorRules;

    private static final String billingDataConfigBasename = "ice_config";
    private static final String debugCacheAccounts = "cacheAccounts";

    /**
     *
     * @param properties         (required)
     * @param productService     (required)
     * @param reservationService (required)
     * @param resourceService    (optional)
     * @param randomizer         (optional)
     */
    public ProcessorConfig(Properties properties, AWSCredentialsProvider credentialsProvider,
            ProductService productService, ReservationService reservationService, PriceListService priceListService)
            throws Exception {

        super(properties, credentialsProvider, productService);

        initZones();

        this.billingBuckets = Lists.newArrayList();
        initBillingBuckets(properties);
        this.kubernetesBuckets = Lists.newArrayList();
        initKubernetesBuckets(properties);

        if (reservationService == null)
            throw new IllegalArgumentException("reservationService must be specified");

        this.reservationService = reservationService;
        this.priceListService = priceListService;

        String customTags = properties.getProperty(IceOptions.CUSTOM_TAGS, "");
        String additionalTags = properties.getProperty(IceOptions.ADDITIONAL_TAGS, "");
        boolean includeReservationIds = properties.getProperty(IceOptions.RESERVATION_ID_TAGS) == null ? false
                : Boolean.parseBoolean(properties.getProperty(IceOptions.RESERVATION_ID_TAGS));
        resourceService = customTags.isEmpty() ? null
                : new BasicResourceService(productService, customTags.split(","), additionalTags.split(","),
                        includeReservationIds);

        Map<String, AccountConfig> orgAccounts = getAccountsFromOrganizations();
        Map<String, AccountConfig> accountConfigs = overlayAccountConfigsFromProperties(properties, orgAccounts);
        processBillingDataConfig(accountConfigs);
        processWorkBucketConfig(accountConfigs);
        for (AccountConfig ac : accountConfigs.values()) {
            logger.debug("  Account " + ac.toString());
            if (resourceService != null)
                resourceService.putDefaultTags(ac.getId(), ac.getDefaultTags());
        }

        accountService = new BasicAccountService(accountConfigs);

        if (properties.getProperty(IceOptions.START_MONTH) == null)
            throw new IllegalArgumentException("IceOptions.START_MONTH must be specified");
        this.startMonth = properties.getProperty(IceOptions.START_MONTH);
        this.startDate = new DateTime(startMonth, DateTimeZone.UTC);

        String[] yearMonth = properties.getProperty(IceOptions.COST_AND_USAGE_START_DATE, "").split("-");
        if (yearMonth.length < 2)
            costAndUsageStartDate = new DateTime(3000, 1, 1, 0, 0, DateTimeZone.UTC); // Arbitrary year in the future
        else
            costAndUsageStartDate = new DateTime(Integer.parseInt(yearMonth[0]), Integer.parseInt(yearMonth[1]), 1, 0,
                    0, DateTimeZone.UTC);

        yearMonth = properties.getProperty(IceOptions.COST_AND_USAGE_NET_UNBLENDED_START_DATE, "").split("-");
        costAndUsageNetUnblendedStartDate = yearMonth.length < 2 ? null
                : new DateTime(Integer.parseInt(yearMonth[0]), Integer.parseInt(yearMonth[1]), 1, 0, 0,
                        DateTimeZone.UTC);

        edpDiscounts = Maps.newTreeMap();
        String[] rates = properties.getProperty(IceOptions.EDP_DISCOUNTS, "").split(",");
        for (String rate : rates) {
            String[] parts = rate.split(":");
            if (parts.length < 2)
                break;
            edpDiscounts.put(new DateTime(parts[0], DateTimeZone.UTC), Double.parseDouble(parts[1]) / 100);
        }

        useBlended = properties.getProperty(IceOptions.USE_BLENDED) == null ? false
                : Boolean.parseBoolean(properties.getProperty(IceOptions.USE_BLENDED));

        // useCostForResourceGroup =
        // properties.getProperty(IceOptions.RESOURCE_GROUP_COST, "modeled");
        useCostForResourceGroup = properties.getProperty(IceOptions.RESOURCE_GROUP_COST, "");
        jsonFiles = Lists.newArrayList();
        if (!properties.getProperty(IceOptions.WRITE_JSON_FILES, "").isEmpty()) {
            for (String t : properties.getProperty(IceOptions.WRITE_JSON_FILES).split(",")) {
                jsonFiles.add(JsonFileType.valueOf(t));
            }
        }

        processOnce = properties.getProperty(IceOptions.PROCESS_ONCE) == null ? false
                : Boolean.parseBoolean(properties.getProperty(IceOptions.PROCESS_ONCE));
        processorRegion = properties.getProperty(IceOptions.PROCESSOR_REGION);
        processorInstanceId = properties.getProperty(IceOptions.PROCESSOR_INSTANCE_ID);

        ProcessorConfig.instance = this;

        billingFileProcessor = new BillingFileProcessor(this);

        boolean needPoller = Boolean.parseBoolean(properties.getProperty(IceOptions.RESERVATION_CAPACITY_POLLER))
                && (startDate.isBefore(CostAndUsageReportLineItemProcessor.jan1_2018)
                        || new DateTime(CostAndUsageReportLineItemProcessor.jan1_2018).isBefore(costAndUsageStartDate));

        reservationCapacityPoller = needPoller ? new ReservationCapacityPoller(this) : null;
    }

    private void initBillingBuckets(Properties properties) {
        String[] billingS3BucketNames = properties.getProperty(IceOptions.BILLING_S3_BUCKET_NAME).split(",");
        String[] billingS3BucketRegions = properties.getProperty(IceOptions.BILLING_S3_BUCKET_REGION).split(",");
        String[] billingS3BucketPrefixes = properties.getProperty(IceOptions.BILLING_S3_BUCKET_PREFIX, "").split(",");
        String[] billingAccountIds = properties.getProperty(IceOptions.BILLING_PAYER_ACCOUNT_ID, "").split(",");
        String[] billingAccessRoleNames = properties.getProperty(IceOptions.BILLING_ACCESS_ROLENAME, "").split(",");
        String[] billingAccessExternalIds = properties.getProperty(IceOptions.BILLING_ACCESS_EXTERNALID, "").split(",");
        String[] rootNames = properties.getProperty(IceOptions.ROOT_NAME, "").split(",");
        String[] configBasenames = properties.getProperty(IceOptions.BILLING_ICE_CONFIG_BASENAME, "").split(",");

        for (int i = 0; i < billingS3BucketNames.length; i++) {
            BillingBucket bb = new BillingBucket(billingS3BucketNames.length > i ? billingS3BucketNames[i] : "",
                    billingS3BucketRegions.length > i ? billingS3BucketRegions[i] : "",
                    billingS3BucketPrefixes.length > i ? billingS3BucketPrefixes[i] : "",
                    billingAccountIds.length > i ? billingAccountIds[i] : "",
                    billingAccessRoleNames.length > i ? billingAccessRoleNames[i] : "",
                    billingAccessExternalIds.length > i ? billingAccessExternalIds[i] : "",
                    rootNames.length > i ? rootNames[i] : "", configBasenames.length > i ? configBasenames[i] : "");
            billingBuckets.add(bb);
        }
    }

    private void initKubernetesBuckets(Properties properties) {
        String[] kubernetesS3BucketNames = properties.getProperty(IceOptions.KUBERNETES_S3_BUCKET_NAME, "").split(",");
        String[] kubernetesS3BucketRegions = properties.getProperty(IceOptions.KUBERNETES_S3_BUCKET_REGION, "")
                .split(",");
        String[] kubernetesS3BucketPrefixes = properties.getProperty(IceOptions.KUBERNETES_S3_BUCKET_PREFIX, "")
                .split(",");
        String[] kubernetesAccountIds = properties.getProperty(IceOptions.KUBERNETES_ACCOUNT_ID, "").split(",");
        String[] kubernetesAccessRoleNames = properties.getProperty(IceOptions.KUBERNETES_ACCESS_ROLENAME, "")
                .split(",");
        String[] kubernetesAccessExternalIds = properties.getProperty(IceOptions.KUBERNETES_ACCESS_EXTERNALID, "")
                .split(",");

        for (int i = 0; i < kubernetesS3BucketNames.length; i++) {
            BillingBucket bb = new BillingBucket(kubernetesS3BucketNames.length > i ? kubernetesS3BucketNames[i] : "",
                    kubernetesS3BucketRegions.length > i ? kubernetesS3BucketRegions[i] : "",
                    kubernetesS3BucketPrefixes.length > i ? kubernetesS3BucketPrefixes[i] : "",
                    kubernetesAccountIds.length > i ? kubernetesAccountIds[i] : "",
                    kubernetesAccessRoleNames.length > i ? kubernetesAccessRoleNames[i] : "",
                    kubernetesAccessExternalIds.length > i ? kubernetesAccessExternalIds[i] : "", "", "");
            kubernetesBuckets.add(bb);
        }
    }

    public void start() throws Exception {
        logger.info("starting up...");

        productService.initProcessor(workBucketConfig.localDir, workBucketConfig.workS3BucketName,
                workBucketConfig.workS3BucketPrefix);

        if (reservationCapacityPoller != null)
            reservationCapacityPoller.init();
        if (resourceService != null)
            resourceService.init();

        priceListService.init();
        billingFileProcessor.start();
    }

    public void shutdown() {
        logger.info("Shutting down...");

        billingFileProcessor.shutdown();
        if (reservationCapacityPoller != null)
            reservationCapacityPoller.shutdown();
    }

    /**
     * Return the EDP discount for the requested time E.G. a 5% discount will return
     * 0.05
     * 
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
     * 
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

    protected void initZones() throws BadZone {
        AmazonEC2ClientBuilder ec2Builder = AmazonEC2ClientBuilder.standard()
                .withClientConfiguration(AwsUtils.clientConfig).withCredentials(AwsUtils.awsCredentialsProvider);
        AmazonEC2 ec2 = ec2Builder.withRegion(Regions.US_EAST_1).build();
        DescribeRegionsResult regionResult = ec2.describeRegions();

        for (com.amazonaws.services.ec2.model.Region r : regionResult.getRegions()) {
            Region region = Region.getRegionByName(r.getRegionName());
            if (region == null) {
                logger.error("Unknown region: " + r.getRegionName());
                continue;
            }

            ec2 = ec2Builder.withRegion(region.name).build();
            try {
                DescribeAvailabilityZonesResult result = ec2.describeAvailabilityZones();
                for (AvailabilityZone az : result.getAvailabilityZones()) {
                    region.getZone(az.getZoneName());
                }
            } catch (AmazonEC2Exception e) {
                logger.error("failed to get zones for region " + region + ", " + e.getErrorMessage());
            }
        }
    }

    /**
     * Save the configuration items for the reader in the work bucket
     * 
     * @throws IOException
     */
    public void saveWorkBucketDataConfig() throws IOException {
        Map<String, List<String>> zones = Maps.newHashMap();
        for (Region r : Region.getAllRegions()) {
            List<String> zlist = Lists.newArrayList();
            for (Zone z : r.getZones())
                zlist.add(z.name);
            zones.put(r.name, zlist);
        }
        WorkBucketDataConfig wbdc = new WorkBucketDataConfig(startMonth, accountService.getAccounts(), zones,
                resourceService == null ? null : resourceService.getUserTags(), getTagCoverage(),
                resourceService.getTagConfigs());
        File file = new File(workBucketConfig.localDir, workBucketDataConfigFilename);
        OutputStream os = new FileOutputStream(file);
        OutputStreamWriter writer = new OutputStreamWriter(os);
        writer.write(wbdc.toJSON());
        writer.close();

        logger.info("Upload work bucket data config file");
        AwsUtils.upload(workBucketConfig.workS3BucketName, workBucketConfig.workS3BucketPrefix, file);
    }

    /**
     * get all the accounts from the organizations service so we can add the names
     * if they're not specified in the ice.properties file.
     * 
     * @throws Exception
     */
    protected Map<String, AccountConfig> getAccountsFromOrganizations() throws Exception {
        final String localAccountConfigsFilename = "accountConfigs.json";
        File file = new File(workBucketConfig.localDir, localAccountConfigsFilename);
        Map<String, AccountConfig> result = Maps.newHashMap();

        boolean cacheAccounts = debugProperties.containsKey(debugCacheAccounts)
                && Boolean.parseBoolean(debugProperties.get(debugCacheAccounts));
        if (cacheAccounts && file.exists() && file.lastModified() > DateTime.now().minusHours(12).getMillis()) {
            // Save time by grabbing the accounts from the cached copy
            String json;
            try {
                json = new String(Files.readAllBytes(file.toPath()), "UTF-8");
                BillingDataConfig bdc = new BillingDataConfig(json);
                for (AccountConfig ac : bdc.getAccounts()) {
                    result.put(ac.getId(), ac);
                }
                logger.info("Accounts read from recent local copy");
                return result;
            } catch (IOException e) {
                // ignore error and pull accounts from org service.
                file.delete();
            }
        }

        Set<String> done = Sets.newHashSet();

        List<String> customTags = Lists.newArrayList();
        if (resourceService != null)
            customTags = resourceService.getCustomTags();

        for (BillingBucket bb : billingBuckets) {
            // Only process each payer account once. Can have two if processing both DBRs
            // and CURs
            if (done.contains(bb.accountId))
                continue;
            done.add(bb.accountId);

            if (!AwsUtils.isMasterAccount(bb.accountId, bb.accessRoleName, bb.accessExternalId)) {
                // Billing bucket account is no longer a master account, so don't look up linked
                // accounts.
                logger.info("Billing account " + bb.accountId
                        + " is no longer a master account. Not pulling accounts from organizations service.");
                continue;
            }

            Map<String, AccountConfig> accounts = getOrganizationAccounts(bb, customTags);
            for (Entry<String, AccountConfig> entry : accounts.entrySet()) {
                result.put(entry.getKey(), entry.getValue());
            }
        }

        if (cacheAccounts) {
            // Save a local copy
            BillingDataConfig bdc = new BillingDataConfig();
            bdc.accounts = Lists.newArrayList();
            for (AccountConfig ac : result.values())
                bdc.accounts.add(ac);
            OutputStream os = new FileOutputStream(file);
            OutputStreamWriter writer = new OutputStreamWriter(os);
            writer.write(bdc.toJSON());
            writer.close();
        }

        return result;
    }

    protected static Map<String, AccountConfig> getOrganizationAccounts(BillingBucket bb, List<String> customTags) {
        logger.info("Get account/organizational unit hierarchy for " + bb.accountId + " using assume role \""
                + bb.accessRoleName + "\", and external id \"" + bb.accessExternalId + "\"");

        Map<String, List<String>> accountParents = AwsUtils.getAccountParents(bb.accountId, bb.accessRoleName,
                bb.accessExternalId, bb.rootName);

        logger.info("Get accounts for organization " + bb.accountId + " using assume role \"" + bb.accessRoleName
                + "\", and external id \"" + bb.accessExternalId + "\"");
        List<Account> accounts = AwsUtils.listAccounts(bb.accountId, bb.accessRoleName, bb.accessExternalId);
        Map<String, AccountConfig> result = Maps.newHashMap();
        for (Account a : accounts) {
            // Get tags for the account
            List<com.amazonaws.services.organizations.model.Tag> tags = AwsUtils.listAccountTags(a.getId(),
                    bb.accountId, bb.accessRoleName, bb.accessExternalId);
            AccountConfig ac = new AccountConfig(a, accountParents.get(a.getId()), tags, customTags);
            result.put(ac.getId(), ac);
        }
        return result;
    }

    /*
     * Pull account configs from the properties
     */
    private Map<String, AccountConfig> overlayAccountConfigsFromProperties(Properties properties,
            Map<String, AccountConfig> orgAccounts) {
        Map<String, AccountConfig> accountConfigs = Maps.newHashMap();
        accountConfigs.putAll(orgAccounts);

        /*
         * Overwrite with any definitions in the properties. The following property key
         * values are used:
         * 
         * Account definition: name: account name id: account id
         * 
         * ice.account.{name}={id}
         *
         * example: ice.account.myAccount=123456789012
         * 
         * Reservation Owner Account name: account name product: codes for products with
         * purchased reserved instances. Possible values are ec2, rds, redshift
         * 
         * ice.owneraccount.{name}={products}
         *
         * example: ice.owneraccount.resHolder=ec2,rds
         *
         * Reservation Owner Account Role name: account name role: IAM role name to
         * assume when pulling reservations from an owner account
         * 
         * ice.owneraccount.{name}.role={role}
         * 
         * example: ice.owneraccount.resHolder.role=ice
         * 
         * Reservation Owner Account ExternalId name: account name externalId: external
         * ID for the reservation owner account
         * 
         * ice.owneraccount.{name}.externalId={externalId}
         * 
         * example: ice.owneraccount.resHolder.externalId=112233445566
         */
        for (String name : properties.stringPropertyNames()) {
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
                String awsName = orgAccounts.containsKey(id) ? orgAccounts.get(id).getAwsName() : null;
                accountConfigs.put(id,
                        new AccountConfig(id, accountName, awsName, null, null, riProducts, role, externalId));
                logger.warn("Using ice.properties config for account " + id + ": " + accountName);
            }
        }
        return accountConfigs;
    }

    /**
     * Get the billing data configurations specified along side the billing reports
     * and override any account names and default tagging
     */
    protected void processBillingDataConfig(Map<String, AccountConfig> accountConfigs) {
        kubernetesConfigs = Lists.newArrayList();
        postProcessorRules = Lists.newArrayList();

        for (BillingBucket bb : billingBuckets) {

            BillingDataConfig bdc = readBillingDataConfig(bb);
            if (bdc == null)
                continue;

            logger.info("Billing Data Configuration: Found "
                    + (bdc.getAccounts() == null ? "null" : bdc.getAccounts().size()) + " accounts, "
                    + (bdc.getTags() == null ? "null" : bdc.getTags().size()) + " tags, "
                    + (bdc.getKubernetes() == null ? "null" : bdc.getKubernetes().size()) + " kubernetes, "
                    + (bdc.getPostprocrules() == null ? "null" : bdc.getPostprocrules().size()) + " post-proc");

            if (bdc.getAccounts() != null) {
                for (AccountConfig account : bdc.getAccounts()) {
                    if (account.id == null || account.id.isEmpty()) {
                        logger.warn("Ignoring billing data config for account with no id: " + account.name);
                        continue;
                    }

                    if (accountConfigs.containsKey(account.id)) {
                        logger.warn("Ignoring billing data config for account " + account.id + ": " + account.name
                                + ". Config already defined in Organizations service or ice.properties.");
                        continue;
                    }
                    accountConfigs.put(account.id, account);
                    logger.debug("Adding billing data config account: " + account.toString());
                }
            }

            if (resourceService != null && bdc.getTags() != null)
                resourceService.setTagConfigs(bb.accountId, bdc.getTags());
            List<KubernetesConfig> k = bdc.getKubernetes();
            if (k != null)
                kubernetesConfigs.addAll(k);

            List<RuleConfig> ruleConfigs = bdc.getPostprocrules();
            if (ruleConfigs != null)
                postProcessorRules.addAll(ruleConfigs);
        }
    }

    protected BillingDataConfig readBillingDataConfig(BillingBucket bb) {
        // Make sure prefix ends with /
        String prefix = bb.s3BucketPrefix.endsWith("/") ? bb.s3BucketPrefix : bb.s3BucketPrefix + "/";
        prefix = "";
        String basename = bb.configBasename.isEmpty() ? billingDataConfigBasename : bb.configBasename;
        logger.info("basename= " + basename);
        logger.info("Look for data config: " + bb.s3BucketName + ", " + bb.s3BucketRegion + ", " + prefix + basename
                + ", " + bb.accountId);
        List<S3ObjectSummary> configFiles = AwsUtils.listAllObjects(bb.s3BucketName, bb.s3BucketRegion,
                prefix + basename + ".", bb.accountId, bb.accessRoleName, bb.accessExternalId);
        logger.info("configFiles= " + configFiles.size());
        if (configFiles.size() == 0 && !basename.equals(billingDataConfigBasename)) {
            // Default baseName was overridden but we didn't find it. Fall back to the
            // default config file basename
            logger.info("===inside if====");
            logger.info("Look for data config: " + bb.s3BucketName + ", " + bb.s3BucketRegion + ", " + prefix
                    + billingDataConfigBasename + ", " + bb.accountId);
            configFiles = AwsUtils.listAllObjects(bb.s3BucketName, bb.s3BucketRegion,
                    prefix + billingDataConfigBasename + ".", bb.accountId, bb.accessRoleName, bb.accessExternalId);
            if (configFiles.size() == 0) {
                return null;
            }
        }
        logger.info("=== is the error here ?? ====");
        String fileKey = configFiles.get(0).getKey();
        logger.info("file key: " + fileKey);
        File file = new File(workBucketConfig.localDir, fileKey.substring(prefix.length()));
        logger.info("file fetched: " + file);
        // Always download - specify 0 for time since.
        boolean downloaded = AwsUtils.downloadFileIfChangedSince(bb.s3BucketName, bb.s3BucketRegion, prefix, file, 0,
                bb.accountId, bb.accessRoleName, bb.accessExternalId);
        if (downloaded) {
            String body;
            try {
                body = new String(Files.readAllBytes(file.toPath()), "UTF-8");
            } catch (IOException e) {
                logger.error("Error reading account properties " + e);
                return null;
            }
            logger.info("downloaded billing data config: " + bb.s3BucketName + "/" + fileKey);
            try {
                return new BillingDataConfig(body);
            } catch (Exception e) {
                logger.error("Failed to parse billing data config: " + bb.s3BucketName + "/" + fileKey);
                e.printStackTrace();
                return null;
            }
        }
        return null;
    }

    /**
     * Read the work bucket data configuration file and add any account configs that
     * aren't already in our map.
     * 
     * @param accountConfigs
     * @throws UnsupportedEncodingException
     * @throws InterruptedException
     * @throws IOException
     */
    protected void processWorkBucketConfig(Map<String, AccountConfig> accountConfigs)
            throws UnsupportedEncodingException, InterruptedException, IOException {
        WorkBucketDataConfig wbdc = downloadWorkBucketDataConfig(true);
        if (wbdc == null)
            return;

        for (com.netflix.ice.tag.Account a : wbdc.getAccounts()) {
            if (accountConfigs.containsKey(a.getId()))
                continue;

            AccountConfig ac = new AccountConfig(a);
            accountConfigs.put(a.getId(), ac);
            logger.warn("Adding work bucket data config account - needs to be added to billing bucket config: " + ac);
        }
    }
}
