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
package com.netflix.ice.processor.pricelist;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.processor.pricelist.Index.Offer;
import com.netflix.ice.processor.pricelist.InstancePrices.OperatingSystem;
import com.netflix.ice.processor.pricelist.InstancePrices.Product;
import com.netflix.ice.processor.pricelist.InstancePrices.ServiceCode;
import com.netflix.ice.processor.pricelist.InstancePrices.Tenancy;
import com.netflix.ice.processor.pricelist.VersionIndex.Version;
import com.netflix.ice.reader.InstanceMetrics;

public class PriceListService {
    protected Logger logger = LoggerFactory.getLogger(getClass());
	private static final String domain = "https://pricing.us-east-1.amazonaws.com";
	private static final String priceListIndexUrl = "/offers/v1.0/aws/index.json";

	// Add other Tenancy values when needed - must also add to Key if more than one
	public static Set<Tenancy> tenancies = Sets.newHashSet(Tenancy.Shared);
	
	private final String localDir;
	private final String workS3BucketName;
	private final String workS3BucketPrefix;

	/**
	 * A generic cached item with a timestamp indicating when it was last read
	 * An item older than one hour is assumed out-of-date.
	 *
	 * @param <I>
	 */
	class CachedItem<I> {
		public I item;
		public DateTime lastReadAt;
		
		public CachedItem(I item, DateTime lastReadAt) {
			this.item = item;
			this.lastReadAt = lastReadAt;
		}
		
		public boolean isCurrent() {
			return lastReadAt.isAfter(DateTime.now().minusHours(1));
		}
	}
	private CachedItem<Index> index = null;
	
	private Map<String, CachedItem<VersionIndex>> versionIndecies; // Key is URL to version Index
	
	private Map<ServiceCode, Map<String, InstancePrices>> versionedPriceLists; // Keyed by service code. Second key is version ID
	protected InstanceMetrics instanceMetrics;
	
	
	public PriceListService(String localDir, String workS3BucketName, String workS3BucketPrefix) throws Exception {
		this.localDir = localDir;
		this.workS3BucketName = workS3BucketName;
		this.workS3BucketPrefix = workS3BucketPrefix;
		
		versionIndecies = Maps.newHashMap();
		versionedPriceLists = Maps.newHashMap();
		for (ServiceCode code: ServiceCode.values()) {
			Map<String, InstancePrices> versionedPrices = Maps.newHashMap();
			versionedPriceLists.put(code, versionedPrices);
		}
		instanceMetrics = null;
	}
	
	public void init() throws Exception {
		// Build the instance metrics from the latest price lists for EC2 and Redshift.
		// (RDS doesn't contribute anything that EC2 doesn't have)
		// Load our cached copy if we have one and then see what price list versions
		// it was built from.
		loadInstanceMetrics();
		
		ServiceCode[] serviceCodes = { ServiceCode.AmazonEC2, ServiceCode.AmazonRedshift };
		boolean current = true;
		DateTime now = DateTime.now();
		Index index = getIndex();
		
		for (ServiceCode sc: serviceCodes) {
			VersionIndex vi = getVersionIndex(index, sc);
			String id = vi.getVersionId(now);
			if (!StringUtils.equals(instanceMetrics.getPriceListVersion(sc), id)) {
				current = false;
				break;
			}
		}
		
		if (current) {
			// Price lists haven't changed since we last generated the metrics
			return;
		}
		
		// Regenerate metrics from scratch
		instanceMetrics = new InstanceMetrics();
		
		// Load latest price lists
		for (ServiceCode sc: serviceCodes) {
			InstancePrices prices = getPrices(now, sc);
			// scan all the products and grab the metrics
        	for (Product p: prices.getPrices().values()) {
	        	// Add stats to InstanceMetrics
        		if (sc == ServiceCode.AmazonEC2 && OperatingSystem.valueOf(p.operatingSystem) != OperatingSystem.Linux)
        			continue;
	        	instanceMetrics.add(p.instanceType, p.vcpu, p.ecu, p.normalizationSizeFactor);
        	}
		}
       	archiveInstanceMetrics();
	}
	
	public InstanceMetrics getInstanceMetrics() throws IOException {
		return instanceMetrics;
	}
	
	private Index getIndex() throws Exception {
		if (index != null && index.isCurrent() ) {
			// Current cached copy is less than an hour old, so use it.
			return index.item;
		}
		DateTime readTime = DateTime.now();
		InputStream stream = new URL(domain + priceListIndexUrl).openStream();
        index = new CachedItem<Index>(new Index(stream), readTime);
        stream.close();
        return index.item;
	}
	
	private VersionIndex getVersionIndex(Index index, ServiceCode serviceCode) throws Exception {
        Offer offer = index.getOffer(serviceCode.name());
        
        // See if we can use the one in the cache
        CachedItem<VersionIndex> vi = versionIndecies.get(offer.versionIndexUrl);
        if (vi != null && vi.isCurrent()) {
        	return vi.item;
        }
                
		DateTime readTime = DateTime.now();
        InputStream stream = new URL(domain + offer.versionIndexUrl).openStream();        
        VersionIndex versionIndex = new VersionIndex(stream);
        stream.close();
        versionIndecies.put(offer.versionIndexUrl, new CachedItem<VersionIndex>(versionIndex, readTime));
        return versionIndex;
	}
	
    public InstancePrices getPrices(DateTime start, ServiceCode serviceCode) throws Exception {
        VersionIndex versionIndex = getVersionIndex(getIndex(), serviceCode);
	       
        String id = versionIndex.getVersionId(start);
        Version version = versionIndex.getVersion(id);
        
        // Is the current version in our cache?
        InstancePrices prices = versionedPriceLists.get(serviceCode).get(id);
        if (prices == null) {        
	        // See if we've already processed the current version
	        prices = load(serviceCode, id, version);
        }
        
       	return prices;
    }
    
    private String getFilename(ServiceCode serviceCode, String versionId) {
    	return "prices_" + serviceCode + "_" + versionId;
    }
    
    protected InstancePrices load(ServiceCode serviceCode, String versionId, Version version) throws Exception {
        InstancePrices ip = null;
        
    	if (localDir != null) {    	
	    	String name = getFilename(serviceCode, versionId);
	        File file = new File(localDir, name + ".gz");
	        
	        if (workS3BucketName != null) {
		        logger.info("downloading " + file + "...");
		        AwsUtils.downloadFileIfNotExist(workS3BucketName, workS3BucketPrefix, file);
	        	logger.info("downloaded " + file);
	        }
	
	        if (file.exists()) {
	        	InputStream is = new FileInputStream(file);
	        	is = new GZIPInputStream(is);
	            DataInputStream in = new DataInputStream(is);
	            try {
	                ip = InstancePrices.Serializer.deserialize(in);
	            }
	            finally {
	                if (in != null)
	                    in.close();
	            }
	           	versionedPriceLists.get(serviceCode).put(versionId, ip);
	            return ip;
	        }
    	}

        ip = fetch(serviceCode, versionId, version);
       	versionedPriceLists.get(serviceCode).put(versionId, ip);
        return ip;
    }
    
    protected InstancePrices fetch(ServiceCode serviceCode, String versionId, Version version) throws Exception {
        logger.info("fetching price list for " + serviceCode + " from " + domain + version.offerVersionUrl + "...");
        InputStream stream = new URL(domain + version.offerVersionUrl).openStream();
        PriceList priceList = new PriceList(stream);
        
       	InstancePrices prices = new InstancePrices(serviceCode, versionId, version.getBeginDate(), version.getEndDate());
       	prices.importPriceList(priceList, tenancies);
       	
       	archive(prices, getFilename(serviceCode, versionId));
       	
       	return prices;
    }
    
    protected void archive(InstancePrices prices, String name) throws IOException {
    	if (localDir == null)
    		return;
    	
        logger.info("archiving price list " + name + "...");
        File file = new File(localDir, name + ".gz");
    	OutputStream os = new FileOutputStream(file);
		os = new GZIPOutputStream(os);
        DataOutputStream out = new DataOutputStream(os);
        try {
            InstancePrices.Serializer.serialize(out, prices);
        }
        finally {
            out.close();
        }
        
        if (workS3BucketName != null) {
	        logger.info(name + " uploading to s3...");
	        AwsUtils.upload(workS3BucketName, workS3BucketPrefix, localDir, name);
	        logger.info(name + " uploading done.");
        }
    }
    
	protected void loadInstanceMetrics() throws IOException {
		instanceMetrics = new InstanceMetrics();
		if (localDir == null)
			return;
		
		File file = new File(localDir, InstanceMetrics.dbName);
		
		if (workS3BucketName != null) {
			logger.info("downloading " + file + "...");
			AwsUtils.downloadFileIfNotExist(workS3BucketName, workS3BucketPrefix, file);
		}
	
		if (file.exists()) {
		    logger.info("trying to load " + file);
	        DataInputStream in = new DataInputStream(new FileInputStream(file));
	        try {
	            instanceMetrics = InstanceMetrics.Serializer.deserialize(in);
	            logger.info("done loading " + file);
	        }
	        finally {
	            in.close();
	        }
	    }
	}
	
    protected void archiveInstanceMetrics() throws IOException {
    	if (instanceMetrics == null || localDir == null)
    		return;
    	
       	logger.info("archiving instance metrics " + InstanceMetrics.dbName + "...");
        File file = new File(localDir, InstanceMetrics.dbName);
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));
        try {
        	InstanceMetrics.Serializer.serialize(out, instanceMetrics);
        }
        finally {
            out.close();
        }
        
        if (workS3BucketName != null) {
	        logger.info(InstanceMetrics.dbName + " uploading to s3...");
	        AwsUtils.upload(workS3BucketName, workS3BucketPrefix, localDir, InstanceMetrics.dbName);
	        logger.info(InstanceMetrics.dbName + " uploading done.");
        }
    }
    
}
