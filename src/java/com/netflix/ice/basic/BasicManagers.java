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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.ice.common.*;
import com.netflix.ice.processor.Instances;
import com.netflix.ice.processor.TagGroupWriter;
import com.netflix.ice.reader.*;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.UserTag;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.joda.time.DateTime;

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
    private TreeMap<UserTag, TagCoverageDataManager> tagCoverageManagers = Maps.newTreeMap();
    private InstanceMetricsService instanceMetricsService = null;
    private InstancesService instancesService = null;
    private Long lastPollMillis = 0L;

    private static final String COVERAGE_PREFIX = "coverage_hourly_";
    
    BasicManagers(boolean compress) {
    	this.compress = compress;
    }
    
    public void shutdown() {
    	lastProcessedPoller.shutdown();
    	
        for (BasicTagGroupManager tagGroupManager: tagGroupManagers.values()) {
            tagGroupManager.shutdown();
        }
        for (BasicDataManager dataManager: costManagers.values()) {
            dataManager.shutdown();
        }
        for (BasicDataManager dataManager: usageManagers.values()) {
            dataManager.shutdown();
        }
        for (TagCoverageDataManager dataManager: tagCoverageManagers.values()) {
            dataManager.shutdown();
        }
    }

    public void init() {
        config = ReaderConfig.getInstance();
        instanceMetricsService = new InstanceMetricsService(config.localDir, config.workS3BucketName, config.workS3BucketPrefix);
        instancesService = new InstancesService(config.localDir, config.workS3BucketName, config.workS3BucketPrefix, config.accountService);
        
        lastProcessedPoller = new LastProcessedPoller(config.startDate);
        		
        doWork();
        start(1*60, 1*60, false);
    }

    public Collection<Product> getProducts() {
        return products;
    }

    @Override
    public Collection<UserTag> getTags() {
        return tagCoverageManagers.keySet();
    }

    @Override
    public String[] getResourceGroupKeys() {
    	return config.resourceService.getCustomTags();
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
    
    public DataManager getTagCoverageManager(UserTag tag) {
        return tagCoverageManagers.get(tag);
    }
    
    public Instances getInstances() {
    	return instancesService.getInstances();
    }

    @Override
    protected void poll() throws Exception {
        doWork();
    }

    private void doWork() {
    	if (lastPollMillis >= lastProcessedPoller.getLastProcessedMillis())
    		return;	// nothing to do
    	
    	// Mark all the data managers so they update their caches
    	for (BasicTagGroupManager m: tagGroupManagers.values()) {
    		m.stale();
    	}
    	for (DataFilePoller p: costManagers.values()) {
    		p.stale();
    	}
    	for (DataFilePoller p: usageManagers.values()) {
    		p.stale();
    	}
    	for (DataFilePoller p: tagCoverageManagers.values()) {
    		p.stale();
    	}
    	instancesService.stale();
    	instanceMetricsService.stale();
    	
    	lastPollMillis = DateTime.now().getMillis();
    	
        logger.info("trying to find new tag group and data managers...");
        Set<Product> products = Sets.newHashSet(this.products);
        Map<Product, BasicTagGroupManager> tagGroupManagers = Maps.newHashMap(this.tagGroupManagers);
        TreeMap<Key, BasicDataManager> costManagers = Maps.newTreeMap(this.costManagers);
        TreeMap<Key, BasicDataManager> usageManagers = Maps.newTreeMap(this.usageManagers);

        Set<Product> newProducts = Sets.newHashSet();
        AmazonS3Client s3Client = AwsUtils.getAmazonS3Client();
        for (S3ObjectSummary s3ObjectSummary: s3Client.listObjects(config.workS3BucketName, config.workS3BucketPrefix + TagGroupWriter.DB_PREFIX).getObjectSummaries()) {
            String key = s3ObjectSummary.getKey();
            Product product;
            if (key.endsWith("_all")) {
                product = null;
            }
            else {
                String name = key.substring((config.workS3BucketPrefix + TagGroupWriter.DB_PREFIX).length());
                product = config.productService.getProductByFileName(name);
            }
            if (!products.contains(product)) {
                products.add(product);
                newProducts.add(product);
            }
        }

        for (Product product: newProducts) {
        	BasicTagGroupManager tagGroupManager = new BasicTagGroupManager(product);
            tagGroupManagers.put(product, tagGroupManager);
            for (ConsolidateType consolidateType: ConsolidateType.values()) {
                Key key = new Key(product, consolidateType);
                
            	String partialDbName = consolidateType + "_" + (product == null ? "all" : product.getFileName());
               
                costManagers.put(key, new BasicDataManager(config.startDate, "cost_" + partialDbName, consolidateType, tagGroupManager, compress,
                		config.monthlyCacheSize, config.accountService, config.productService, null));
                usageManagers.put(key, new BasicDataManager(config.startDate, "usage_" + partialDbName, consolidateType, tagGroupManager, compress,
                		config.monthlyCacheSize, config.accountService, config.productService, instanceMetricsService));
            }
        }

        if (newProducts.size() > 0) {
            this.costManagers = costManagers;
            this.usageManagers = usageManagers;
            this.tagGroupManagers = tagGroupManagers;
            this.products = products;
        }
        
        for (S3ObjectSummary s3ObjectSummary: s3Client.listObjects(config.workS3BucketName, config.workS3BucketPrefix + COVERAGE_PREFIX).getObjectSummaries()) {
            String key = s3ObjectSummary.getKey();
            String tagName = key.substring((config.workS3BucketPrefix + COVERAGE_PREFIX).length());
            tagName = tagName.substring(0, tagName.indexOf("_"));
            if (tagCoverageManagers.containsKey(new UserTag(tagName)))
            	continue;
            
            tagCoverageManagers.put(new UserTag(tagName),
            		new TagCoverageDataManager(config.startDate, "coverage_" + ConsolidateType.hourly + "_" + tagName, ConsolidateType.hourly, getTagGroupManager(null), compress,
            				config.monthlyCacheSize, config.accountService, config.productService));
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


}
