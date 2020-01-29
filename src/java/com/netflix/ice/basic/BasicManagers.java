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
package com.netflix.ice.basic;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.ice.common.*;
import com.netflix.ice.common.Config.TagCoverage;
import com.netflix.ice.common.Config.WorkBucketConfig;
import com.netflix.ice.processor.Instances;
import com.netflix.ice.processor.TagGroupWriter;
import com.netflix.ice.reader.*;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.Tag;
import com.netflix.ice.tag.TagType;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.UserTag;
import com.netflix.ice.tag.Zone;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.StopWatch;
import org.joda.time.DateTime;
import org.joda.time.Interval;

/**
 * This class manages all BasicTagGroupManager and BasicDataManager instances.
 */
public class BasicManagers extends Poller implements Managers {
    private ReaderConfig config;
    private boolean compress;

    private Set<Product> products = Sets.newHashSet();
    private LastProcessedPoller lastProcessedPoller = null;
    private Map<Product, BasicTagGroupManager> tagGroupManagers = Maps.newHashMap();
    private TreeMap<Key, BasicDataManager> costManagers = Maps.newTreeMap();
    private TreeMap<Key, BasicDataManager> usageManagers = Maps.newTreeMap();
    private TreeMap<Key, TagCoverageDataManager> tagCoverageManagers = Maps.newTreeMap();
    private InstanceMetricsService instanceMetricsService = null;
    private InstancesService instancesService = null;
    private Long lastPollMillis = 0L;
	private ExecutorService pool;
    
    BasicManagers(boolean compress) {
    	this.compress = compress;
    }
    
    public void shutdown() {
    	lastProcessedPoller.shutdown();
    	
        for (Poller tagGroupManager: tagGroupManagers.values()) {
            tagGroupManager.shutdown();
        }
        for (Poller dataManager: costManagers.values()) {
            dataManager.shutdown();
        }
        for (Poller dataManager: usageManagers.values()) {
            dataManager.shutdown();
        }
        for (Poller dataManager: tagCoverageManagers.values()) {
        	dataManager.shutdown();
        }
    }

    public void init() {
        config = ReaderConfig.getInstance();
        lastProcessedPoller = new LastProcessedPoller(config.startDate, config.workBucketConfig);
        pool = Executors.newFixedThreadPool(config.numthreads);
                		
        doWork();
        start(1*60, 1*60, false);
    }

    public Collection<Product> getProducts() {
        return products;
    }

    public TagGroupManager getTagGroupManager(Product product) {
        return tagGroupManagers.get(product);
    }

    public DataManager getCostManager(Product product, ConsolidateType consolidateType) {
        return costManagers.get(new Key(product, consolidateType));
    }

    public DataManager getUsageManager(Product product, ConsolidateType consolidateType) {
        return usageManagers.get(new Key(product, consolidateType));
    }
    
    public DataManager getTagCoverageManager(Product product, ConsolidateType consolidateType) {
        return tagCoverageManagers.get(new Key(product, consolidateType));
    }
    
    public Instances getInstances() {
    	return instancesService.getInstances();
    }

    @Override
    protected void poll() throws Exception {
        doWork();
    }

    private void doWork() {
    	// Update the reader configuration from the work bucket data configuration
    	config.update();
    	
    	if (lastPollMillis >= lastProcessedPoller.getLastProcessedMillis())
    		return;	// nothing to do
    	
    	// Mark all the data managers so they update their caches
    	for (StalePoller m: tagGroupManagers.values()) {
    		m.stale();
    	}
    	for (StalePoller p: costManagers.values()) {
    		p.stale();
    	}
    	for (StalePoller p: usageManagers.values()) {
    		p.stale();
    	}
    	for (StalePoller p: tagCoverageManagers.values()) {
    		p.stale();
    	}
    	
    	WorkBucketConfig wbc = config.workBucketConfig;
    	
    	if (instancesService == null) {
            instanceMetricsService = new InstanceMetricsService(wbc.localDir, wbc.workS3BucketName, wbc.workS3BucketPrefix);
            instancesService = new InstancesService(wbc.localDir, wbc.workS3BucketName, wbc.workS3BucketPrefix, config.accountService, config.productService);
    	}
    	else {
	    	instancesService.stale();
	    	instanceMetricsService.stale();
    	}
    	
    	lastPollMillis = DateTime.now().getMillis();
    	
        logger.info("trying to find new tag group and data managers...");
        Set<Product> products = Sets.newHashSet(this.products);
        Map<Product, BasicTagGroupManager> tagGroupManagers = Maps.newHashMap(this.tagGroupManagers);
        TreeMap<Key, BasicDataManager> costManagers = Maps.newTreeMap(this.costManagers);
        TreeMap<Key, BasicDataManager> usageManagers = Maps.newTreeMap(this.usageManagers);

        Set<Product> newProducts = Sets.newHashSet();
        AmazonS3Client s3Client = AwsUtils.getAmazonS3Client();
        for (S3ObjectSummary s3ObjectSummary: s3Client.listObjects(wbc.workS3BucketName, wbc.workS3BucketPrefix + TagGroupWriter.DB_PREFIX).getObjectSummaries()) {
            String key = s3ObjectSummary.getKey();
            if (key.endsWith(BasicTagGroupManager.compressExtension)) {
            	key = key.substring(0, key.length() - BasicTagGroupManager.compressExtension.length());
            }
            Product product;
            if (key.endsWith("_all")) {
                product = null;
            }
            else {
                String serviceCode = key.substring((wbc.workS3BucketPrefix + TagGroupWriter.DB_PREFIX).length());
                product = config.productService.getProductByServiceCode(serviceCode);
            }
            if (!products.contains(product)) {
                products.add(product);
                newProducts.add(product);
            }
        }

        for (Product product: newProducts) {
        	BasicTagGroupManager tagGroupManager = new BasicTagGroupManager(product, true, config.workBucketConfig, config.accountService, config.productService);
            tagGroupManagers.put(product, tagGroupManager);
            boolean loadTagCoverage = (product == null && config.getTagCoverage() != TagCoverage.none) || (product != null && config.getTagCoverage() == TagCoverage.withUserTags);
            for (ConsolidateType consolidateType: ConsolidateType.values()) {
                Key key = new Key(product, consolidateType);
                
            	if (consolidateType == ConsolidateType.hourly && !config.hourlyData) {
            		if (product != null)
            			continue;
            		
            		// Create hourly cost and usage managers only for reservation and savings plan operations
	                costManagers.put(key, new BasicDataManager(config.startDate, "cost_hourly_all", consolidateType, tagGroupManager, compress, 0,
	                		config.monthlyCacheSize, config.workBucketConfig, config.accountService, config.productService, null, true));
	                usageManagers.put(key, new BasicDataManager(config.startDate, "usage_hourly_all", consolidateType, tagGroupManager, compress, 0,
	                		config.monthlyCacheSize, config.workBucketConfig, config.accountService, config.productService, instanceMetricsService, true));
            	}
            	else {                
	            	String partialDbName = consolidateType + "_" + (product == null ? "all" : product.getServiceCode());
	            	int numUserTags = product == null ? 0 : config.userTags.size();
	               
	                costManagers.put(key, new BasicDataManager(config.startDate, "cost_" + partialDbName, consolidateType, tagGroupManager, compress, numUserTags,
	                		config.monthlyCacheSize, config.workBucketConfig, config.accountService, config.productService, null));
	                usageManagers.put(key, new BasicDataManager(config.startDate, "usage_" + partialDbName, consolidateType, tagGroupManager, compress, numUserTags,
	                		config.monthlyCacheSize, config.workBucketConfig, config.accountService, config.productService, instanceMetricsService));
	                if (loadTagCoverage && consolidateType != ConsolidateType.hourly) {
	    	            tagCoverageManagers.put(key, new TagCoverageDataManager(config.startDate, "coverage_" + partialDbName, consolidateType, tagGroupManager, compress, config.userTags,
	            				config.monthlyCacheSize, config.workBucketConfig, config.accountService, config.productService));
	                }
            	}
            }
        }

        if (newProducts.size() > 0) {
            this.costManagers = costManagers;
            this.usageManagers = usageManagers;
            this.tagGroupManagers = tagGroupManagers;
            this.products = products;
        }
    }

    private static class Key implements Comparable<Key> {
        Product product;
        ConsolidateType consolidateType;
        Key(Product product, ConsolidateType consolidateType) {
            this.product = product;
            this.consolidateType = consolidateType;
        }

        public int compareTo(Key t) {
            int result = this.product == t.product ? 0 : (this.product == null ? 1 : (t.product == null ? -1 : t.product.compareTo(this.product)));
            if (result != 0)
                return result;
            return consolidateType.compareTo(t.consolidateType);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null)
                return false;
            Key other = (Key)o;
            return
                    this.product == other.product &&
                    this.consolidateType == other.consolidateType;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            if (product != null)
                result = prime * result + this.product.hashCode();
            result = prime * result + this.consolidateType.hashCode();

            return result;
        }
    }

    @Override
    public Collection<UserTag> getUserTagValues(List<Account> accounts, List<Region> regions, List<Zone> zones, Collection<Product> products, int index) throws Exception {
    	List<Future<Collection<ResourceGroup>>> futures = Lists.newArrayList();
    	
    	Set<UserTag> userTagValues = Sets.newTreeSet();

		// Add "None" entry
    	userTagValues.add(UserTag.get(UserTag.none));
    	
        for (Product product: products) {
            TagGroupManager tagGroupManager = getTagGroupManager(product);
			if (tagGroupManager == null) {
				//logger.error("No TagGroupManager for product " + product + ", products: " + getProducts().size());
				continue;
			}			
			futures.add(getUserTagValuesForProduct(new TagLists(accounts, regions, zones, Lists.newArrayList(product)), tagGroupManager));
        }
		// Wait for completion
		for (Future<Collection<ResourceGroup>> f: futures) {
			Collection<ResourceGroup> resourceGroups = f.get();
			for (ResourceGroup rg: resourceGroups) {
				// If no separator, it's defaulted to the product name, so skip it
				if (rg.name.contains(ResourceGroup.separator)) {
					UserTag[] tags = rg.getUserTags();
					if (tags.length > index && !StringUtils.isEmpty(tags[index].name))
						userTagValues.add(tags[index]);
				}
			}
		}
		
    	return userTagValues;
    }

    private Future<Collection<ResourceGroup>> getUserTagValuesForProduct(final TagLists tagLists, final TagGroupManager tagGroupManager) {
    	return pool.submit(new Callable<Collection<ResourceGroup>>() {
    		@Override
    		public Collection<ResourceGroup> call() throws Exception {
    			Collection<ResourceGroup> rgs = tagGroupManager.getResourceGroups(tagLists);
    			return rgs;
    		}
    	});    	
    }

    @Override
    public Map<Tag, double[]> getData(
    		Interval interval,
    		List<Account> accounts,
    		List<Region> regions,
    		List<Zone> zones,
    		List<Product> products,
    		List<Operation> operations,
    		List<UsageType> usageTypes,
    		boolean isCost,
    		ConsolidateType consolidateType,
    		TagType groupBy,
    		AggregateType aggregate,
    		boolean forReservation,
    		boolean showLent,
    		UsageUnit usageUnit,
    		List<List<UserTag>> userTagLists,
    		int userTagGroupByIndex) throws Exception {    	
    	
		StopWatch sw = new StopWatch();
		sw.start();
		
		if (products.size() == 0) {
	    	List<Future<Collection<Product>>> futures = Lists.newArrayList();
            TagLists tagLists = new TagLists(accounts, regions, zones);
            for (Product product: getProducts()) {
                if (product == null)
                    continue;

                futures.add(getFilteredProduct(tagLists, getTagGroupManager(product)));
            }
    		// Wait for completion
            Set<Product> productSet = Sets.newTreeSet();
    		for (Future<Collection<Product>> f: futures) {
                productSet.addAll(f.get());
    		}
            products = Lists.newArrayList(productSet);
		}
				
    	List<Future<Map<Tag, double[]>>> futures = Lists.newArrayList();
        for (Product product: products) {
            if (product == null)
                continue;

            DataManager dataManager = isCost ? getCostManager(product, consolidateType) : getUsageManager(product, consolidateType);
			if (dataManager == null) {
				//logger.error("No DataManager for product " + product);
				continue;
			}
			TagLists tagLists = new TagListsWithUserTags(accounts, regions, zones, Lists.newArrayList(product), operations, usageTypes, userTagLists);
			logger.debug("-------------- Process product ----------------" + product);
            futures.add(getDataForProduct(
                    interval,
                    tagLists,
                    groupBy,
                    aggregate,
                    forReservation,
                    showLent,
    				usageUnit,
    				userTagGroupByIndex,
    				dataManager));            
        }
        // Wait for completion
        Map<Tag, double[]> data = Maps.newTreeMap();
        
		for (Future<Map<Tag, double[]>> f: futures) {
			Map<Tag, double[]> dataOfProduct = f.get();
			
            if (groupBy == TagType.Product && dataOfProduct.size() > 0) {
                double[] currentProductValues = dataOfProduct.get(dataOfProduct.keySet().iterator().next());
                dataOfProduct.put(Tag.aggregated, Arrays.copyOf(currentProductValues, currentProductValues.length));
            } 
            
            merge(dataOfProduct, data);
		}
		
		logger.debug("getData() time to process: " + sw);

    	return data;
    }
    
    private Future<Collection<Product>> getFilteredProduct(final TagLists tagLists, final TagGroupManager tagGroupManager) {
    	return pool.submit(new Callable<Collection<Product>>() {
    		@Override
    		public Collection<Product> call() throws Exception {
                return tagGroupManager.getProducts(tagLists);
    		}
    	});    	
    }

    private Future<Map<Tag, double[]>> getDataForProduct(
    		final Interval interval,
    		final TagLists tagLists,
    		final TagType groupBy,
    		final AggregateType aggregate,
    		final boolean forReservation,
    		final boolean showLent,
    		final UsageUnit usageUnit,
    		final int userTagGroupByIndex,
    		final DataManager dataManager) {
    	
    	return pool.submit(new Callable<Map<Tag, double[]>>() {
    		@Override
    		public Map<Tag, double[]> call() throws Exception {
    			Map<Tag, double[]> data = dataManager.getData(
                        interval,
                        tagLists,
                        groupBy,
                        aggregate,
                        forReservation,
                        showLent,
        				usageUnit,
        				userTagGroupByIndex
                    );
    			return data;
    		}
    	});    	
    }

    
    private void merge(Map<Tag, double[]> from, Map<Tag, double[]> to) {
        for (Map.Entry<Tag, double[]> entry: from.entrySet()) {
            Tag tag = entry.getKey();
            double[] newValues = entry.getValue();
            if (to.containsKey(tag)) {
                double[] oldValues = to.get(tag);
                for (int i = 0; i < newValues.length; i++) {
                    oldValues[i] += newValues[i];
                }
            }
            else {
                to.put(tag, newValues);
            }
        }
    }

    public String getStatistics(boolean csv) throws ExecutionException {
    	StringBuilder sb = new StringBuilder();
    	TagGroupManager allTgm = tagGroupManagers.get(null);
		TreeMap<Long, Integer> allTgmSizes = allTgm.getSizes();
		DateTime month = new DateTime(allTgmSizes.lastKey());
		DateTime year = month.withMonthOfYear(1);
		int totalResourceTagGroups = 0;
    	
		if (csv) {
	    	sb.append("Product,TagGroups,Daily Cost TagGroups,Daily Usage TagGroups,Accounts,Regions,Zones,Products,Operations,UsageTypes");
	    	if (config.userTags.size() > 0)
	    		sb.append("," + String.join(",", config.userTags));
	    	sb.append("\n");
		}
		else {
	    	sb.append("<table><tr><td>Product</td><td>TagGroups</td><td>Daily Cost TagGroups</td><td>Daily Usage TagGroups</td><td>Accounts</td><td>Regions</td><td>Zones</td><td>Products</td><td>Operations</td><td>UsageTypes</td>");
	    	if (config.userTags.size() > 0)
	    		sb.append("<td>" + String.join("</td><td>", config.userTags) + "</td>");
	    	sb.append("</tr>");
		}
    	for (Product p: tagGroupManagers.keySet()) {
    		TagGroupManager tgm = tagGroupManagers.get(p);
    		TreeMap<Long, Integer> sizes = tgm.getSizes();
    		TreeMap<Long, List<Integer>> tagValuesSizes = tgm.getTagValueSizes(config.userTags.size());
    		BasicDataManager bdm_cost = costManagers.get(new Key(p, ConsolidateType.daily));
    		BasicDataManager bdm_usage = usageManagers.get(new Key(p, ConsolidateType.daily));
    		
    		if (csv) {
    			sb.append(p + "," + sizes.lastEntry().getValue() + "," + bdm_cost.size(year) + "," + bdm_usage.size(year));
    			for (Integer i: tagValuesSizes.lastEntry().getValue())
    				sb.append("," + i);
    	    	sb.append("\n");
    		}
    		else {
    			sb.append("<tr><td>" + p + "</td><td>" + sizes.lastEntry().getValue() + "</td><td>" + bdm_cost.size(year) + "</td><td>" + bdm_usage.size(year) + "</td>");
    			for (Integer i: tagValuesSizes.lastEntry().getValue())
    				sb.append("<td>" + i + "</td>");
    	    	sb.append("</tr>");
    		}
    		
    		if (p != null) {
    			totalResourceTagGroups += sizes.lastEntry().getValue();
    		}
       	}
    	if (!csv)
    		sb.append("</table>");
    	
    	String intro = "TagGroupManagers: month=" + AwsUtils.monthDateFormat.print(month) + ", size=" + tagGroupManagers.size() + ", total resource TagGroups=" + totalResourceTagGroups;

		if (csv)
			intro += csv ? "\n" : "<br><br>";

    	return intro + sb.toString();
    }
}
