package com.netflix.ice.processor.pricelist;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.processor.pricelist.Index.Offer;
import com.netflix.ice.processor.pricelist.InstancePrices.Tenancy;
import com.netflix.ice.processor.pricelist.VersionIndex.Version;
import com.netflix.ice.reader.InstanceMetrics;

public class PriceListService {
    protected Logger logger = LoggerFactory.getLogger(getClass());
	private static final String domain = "https://pricing.us-east-1.amazonaws.com";
	private static final String priceListIndexUrl = "/offers/v1.0/aws/index.json";

	public enum ServiceCode {
		AmazonEC2,
		AmazonRDS,
		AmazonRedshift;
	}
	// Add other Tenancy values when needed - must also add to Key if more than one
	protected static Set<Tenancy> tenancies = Sets.newHashSet(Tenancy.Shared);
	
	private final String localDir;
	private final String workS3BucketName;
	private final String workS3BucketPrefix;
	
	private Map<ServiceCode, Map<String, InstancePrices>> versionedPriceLists; // Keyed by service code. Second key is version ID
	protected InstanceMetrics instanceMetrics;
	
	
	public PriceListService(String localDir, String workS3BucketName, String workS3BucketPrefix) throws Exception {
		this.localDir = localDir;
		this.workS3BucketName = workS3BucketName;
		this.workS3BucketPrefix = workS3BucketPrefix;
		
		versionedPriceLists = Maps.newHashMap();
		for (ServiceCode code: ServiceCode.values()) {
			Map<String, InstancePrices> versionedPrices = Maps.newHashMap();
			versionedPriceLists.put(code, versionedPrices);
		}
		instanceMetrics = new InstanceMetrics();
		loadInstanceMetrics();
	}
	
	public InstanceMetrics getInstanceMetrics() {
		return instanceMetrics;
	}
	
    public InstancePrices getPrices(DateTime month, ServiceCode serviceCode) throws Exception {
        
		InputStream stream = new URL(domain + priceListIndexUrl).openStream();
        Index index = new Index(stream);
        stream.close();
        
        Offer offer = index.getOffer(serviceCode.name());
        stream = new URL(domain + offer.versionIndexUrl).openStream();        
        VersionIndex versionIndex = new VersionIndex(stream);
        stream.close();
	       
        String id = versionIndex.getVersionId(month);
        Version version = versionIndex.getVersion(id);
        
        // Is the current version in our cache?
        InstancePrices prices = versionedPriceLists.get(serviceCode).get(id);
        if (prices == null) {        
	        // See if we've already processed the current version
	        prices = load(serviceCode, id, version);
	        if (prices == null) {
	            logger.info("fetching price list for " + serviceCode + "...");
	            stream = new URL(domain + version.offerVersionUrl).openStream();
	            PriceList priceList = new PriceList(stream);
	            
	           	prices = new InstancePrices(id, version.getBeginDate(), version.getEndDate());
	           	prices.importPriceList(priceList, tenancies, instanceMetrics);
	           	
	            logger.info("archiving price list for " + serviceCode + "...");
	           	archive(prices, getFilename(serviceCode, id));
	           	
	           	logger.info("archiving instance metrics...");
	           	archiveInstanceMetrics();
	        }
           	versionedPriceLists.get(serviceCode).put(id, prices);
        }
        
       	return prices;
    }
    
    private String getFilename(ServiceCode serviceCode, String versionId) {
    	return "prices_" + serviceCode + "_" + versionId;
    }
    
    protected InstancePrices load(ServiceCode serviceCode, String versionId, Version version) throws Exception {
    	String name = getFilename(serviceCode, versionId);
        File file = new File(localDir, name);
        logger.info("downloading " + file + "...");
        AwsUtils.downloadFileIfNotExist(workS3BucketName, workS3BucketPrefix, file);

        InstancePrices ip = null;
        
        if (file.exists()) {
        	logger.info("downloaded " + file);
        	
            DataInputStream in = new DataInputStream(new FileInputStream(file));
            try {
                ip = InstancePrices.Serializer.deserialize(in);
            }
            finally {
                if (in != null)
                    in.close();
            }
        }
        return ip;
    }
    
    protected void archive(InstancePrices prices, String name) throws IOException {
        File file = new File(localDir, name);
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));
        try {
            InstancePrices.Serializer.serialize(out, prices);
        }
        finally {
            out.close();
        }
        
        logger.info(name + " uploading to s3...");
        AwsUtils.upload(workS3BucketName, workS3BucketPrefix, localDir, name);
        logger.info(name + " uploading done.");
    }
    
	protected void loadInstanceMetrics() throws IOException {
		File file = new File(localDir, InstanceMetrics.dbName);
		logger.info("downloading " + file + "...");
		AwsUtils.downloadFileIfNotExist(workS3BucketName, workS3BucketPrefix, file);
	
		if (file.exists()) {
		    logger.info("trying to load " + file);
	        DataInputStream in = new DataInputStream(new FileInputStream(file));
	        try {
	            instanceMetrics.load(in);
	            logger.info("done loading " + file);
	        }
	        finally {
	            in.close();
	        }
	    }
	}
	
    protected void archiveInstanceMetrics() throws IOException {
        File file = new File(localDir, InstanceMetrics.dbName);
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));
        try {
        	instanceMetrics.archive(out);
        }
        finally {
            out.close();
        }
        
        logger.info(InstanceMetrics.dbName + " uploading to s3...");
        AwsUtils.upload(workS3BucketName, workS3BucketPrefix, localDir, InstanceMetrics.dbName);
        logger.info(InstanceMetrics.dbName + " uploading done.");
    }
    
}
