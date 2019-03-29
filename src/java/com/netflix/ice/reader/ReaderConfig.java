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
package com.netflix.ice.reader;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.google.common.collect.Lists;
import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.common.*;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.TagType;
import com.netflix.ice.tag.UserTag;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Configuration class for reader/UI.
 */
public class ReaderConfig extends Config {
    private static ReaderConfig instance;
    private static final Logger logger = LoggerFactory.getLogger(ReaderConfig.class);
    
    private final int numThreads = 16;

    public final BasicAccountService accountService;
    public final DateTime startDate;
    public final String companyName;
    public final String currencySign;
    public final double currencyRate;
    public final String highstockUrl;
    public final ThroughputMetricService throughputMetricService;
    public final Managers managers;
    public final int monthlyCacheSize;
    public final boolean enableTagCoverageWithUserTag;
    public final List<String> userTags;
    public final boolean familyRiBreakout;
    public final String dashboardNotice;

    /**
     *
     * @param properties (required)
     * @param managers (required)
     * @param productService (required)
     * @param applicationGroupService (optional)
     * @param throughputMetricService (optional)
     * @throws IOException 
     * @throws InterruptedException 
     * @throws UnsupportedEncodingException 
     */
    public ReaderConfig(
            Properties properties,
            AWSCredentialsProvider credentialsProvider,
            Managers managers,
            ProductService productService,
            ThroughputMetricService throughputMetricService) throws UnsupportedEncodingException, InterruptedException, IOException {
        super(properties, credentialsProvider, productService);
        
        
        WorkBucketDataConfig dataConfig = readWorkBucketDataConfig();
        this.startDate = new DateTime(dataConfig.getStartMonth(), DateTimeZone.UTC);
        this.userTags = dataConfig.getUserTags();
        this.familyRiBreakout = dataConfig.getFamilyRiBreakout();
        
        // Account service is initialized here and refreshed while running by the DataManager
        this.accountService = new BasicAccountService(dataConfig.getAccounts());
        
        updateZones(dataConfig.getZones());

        companyName = properties.getProperty(IceOptions.COMPANY_NAME, "");
        dashboardNotice = properties.getProperty(IceOptions.DASHBOARD_NOTICE, "");
        currencySign = properties.getProperty(IceOptions.CURRENCY_SIGN, "$");
        currencyRate = Double.parseDouble(properties.getProperty(IceOptions.CURRENCY_RATE, "1"));
        highstockUrl = properties.getProperty(IceOptions.HIGHSTOCK_URL, "http://code.highcharts.com/stock/highstock.js");

        this.managers = managers;
        this.throughputMetricService = throughputMetricService;
        this.monthlyCacheSize = Integer.parseInt(properties.getProperty(IceOptions.MONTHLY_CACHE_SIZE, "12"));
        this.enableTagCoverageWithUserTag = properties.getProperty(IceOptions.TAG_COVERAGE_WITH_USER_TAGS) == null ? false : Boolean.parseBoolean(properties.getProperty(IceOptions.TAG_COVERAGE_WITH_USER_TAGS));

        ReaderConfig.instance = this;

        if (throughputMetricService != null)
            throughputMetricService.init();
        managers.init();
    }

    /**
     *
     * @return singleton instance
     */
    public static ReaderConfig getInstance() {
        return instance;
    }

    public void start() throws InterruptedException, ExecutionException {

    	// Prime the data caches
        Managers managers = ReaderConfig.getInstance().managers;
        Collection<Product> products = managers.getProducts();
        List<UserTag> userTagList = Lists.newArrayList();
        for (String ut: userTags)
        	userTagList.add(UserTag.get(ut));
        
    	ExecutorService pool = Executors.newFixedThreadPool(numThreads);
    	List<Future<Void>> futures = Lists.newArrayList();

    	
        for (Product product: products) {
        	futures.add(readDataForProduct(product, userTagList, pool));
        }
		// Wait for completion
		for (Future<Void> f: futures) {
			f.get();
		}
    }
    
    public Future<Void> readDataForProduct(final Product product, final List<UserTag> userTagList, ExecutorService pool) {
    	return pool.submit(new Callable<Void>() {
    		@Override
    		public Void call() throws Exception {
                TagGroupManager tagGroupManager = managers.getTagGroupManager(product);
                Interval interval = tagGroupManager.getOverlapInterval(new Interval(new DateTime(DateTimeZone.UTC).minusMonths(monthlyCacheSize), new DateTime(DateTimeZone.UTC)));
                if (interval == null)
                    return null;
                for (ConsolidateType consolidateType: ConsolidateType.values()) {
                    readData(product, managers.getCostManager(product, consolidateType), interval, consolidateType, UsageUnit.Instances, null);
                    readData(product, managers.getUsageManager(product, consolidateType), interval, consolidateType, UsageUnit.Instances, null);
                    // Prime the tag coverage cache
                    if ((product == null || enableTagCoverageWithUserTag) && consolidateType != ConsolidateType.hourly) {
                    	readData(product, managers.getTagCoverageManager(product, consolidateType), interval, consolidateType, UsageUnit.Instances, userTagList);
                    }
                }
    	        return null;
    		}
    	});        
    }

    public void shutdown() {
        logger.info("Shutting down...");

        instance.managers.shutdown();
    }

    private void readData(Product product, DataManager dataManager, Interval interval, ConsolidateType consolidateType, UsageUnit usageUnit, List<UserTag> userTagList) {
        if (consolidateType == ConsolidateType.hourly) {
            DateTime start = interval.getStart().withDayOfMonth(1).withMillisOfDay(0);
            do {
                int hours = dataManager.getDataLength(start);
                logger.info("found " + hours + " hours data for " + product + " "  + interval);
                start = start.plusMonths(1);
            }
            while (start.isBefore(interval.getEnd()));
        }
        else if (consolidateType == ConsolidateType.daily) {
            DateTime start = interval.getStart().withDayOfYear(1).withMillisOfDay(0);
            do {
                dataManager.getDataLength(start);
                start = start.plusYears(1);
            }
            while (start.isBefore(interval.getEnd()));
        }
        else {
        	if (userTagList == null)
        		dataManager.getData(interval, new TagLists(), TagType.Account, AggregateType.both, false, usageUnit, 0);
        	else
        		dataManager.getData(interval, new TagLists(), TagType.Account, AggregateType.both, 0, userTagList);
        }
    }
    
    private WorkBucketDataConfig readWorkBucketDataConfig() throws InterruptedException, UnsupportedEncodingException, IOException {
    	// Try to download the work bucket data configuration.
    	// Keep polling if file doesn't exist yet (Can happen if processor hasn't run yet for the first time)
    	WorkBucketDataConfig config = null;
    	for (config = downloadConfig(); config == null; config = downloadConfig()) {
    		Thread.sleep(60 * 1000L);
    	}
    	return config;    	
    }
    
    private WorkBucketDataConfig downloadConfig() {
    	File file = new File(localDir, workBucketDataConfigFilename);
    	file.delete(); // Delete if it exists so we get a fresh copy from S3 each time
		boolean downloaded = AwsUtils.downloadFileIfNotExist(this.workS3BucketName, this.workS3BucketPrefix, file);
    	if (downloaded) {
        	String json;
			try {
				json = new String(Files.readAllBytes(file.toPath()), "UTF-8");
			} catch (IOException e) {
				logger.error("Error reading work bucket data configuration " + e);
				return null;
			}
        	return new WorkBucketDataConfig(json);    	
    	}
    	return null;
    }
    
    public void update() {
    	// Update the account list from the work bucket data configuration file
    	WorkBucketDataConfig config = downloadConfig();
    	if (config == null)
    		return; // Fail silently
    	
    	accountService.updateAccounts(config.getAccounts());
    	updateZones(config.getZones());
    }
    
    private void updateZones(Map<String, List<String>> zones) {
    	for (String regionName: zones.keySet()) {
    		Region r = Region.getRegionByName(regionName);
    		if (r == null) {
    			logger.error("Unknown region: " + regionName);
    			continue;
    		}
    		for (String zoneName: zones.get(regionName))
    			r.addZone(zoneName);
    	}
    }
}
