package com.netflix.ice.basic;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.zip.GZIPInputStream;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.PeriodType;

import com.amazonaws.AmazonServiceException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.ConsolidateType;
import com.netflix.ice.common.StalePoller;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.reader.ReadOnlyData;
import com.netflix.ice.reader.ReaderConfig;

/**
 * This class reads data from s3 bucket and feeds the data to UI
 */
public class DataFilePoller extends StalePoller {
    protected static final String compressExtension = ".gz";

    protected ReaderConfig config = ReaderConfig.getInstance();
    protected DateTime startDate;
    protected final String dbName;
    protected final boolean compress;
    protected ConsolidateType consolidateType;
    private AccountService accountService;
    private ProductService productService;

    // map of files we've loaded into the cache
    protected Map<DateTime, File> fileCache = Maps.newConcurrentMap();
    
    // data cache
    protected LoadingCache<DateTime, ReadOnlyData> data;

    public DataFilePoller(DateTime startDate, final String dbName, ConsolidateType consolidateType, boolean compress,
    		int monthlyCacheSize, AccountService accountService, ProductService productService) {
    	this.startDate = startDate;
        this.consolidateType = consolidateType;
        this.dbName = dbName;
        this.compress = compress;
        this.accountService = accountService;
        this.productService = productService;
        
        buildCache(monthlyCacheSize);
        start();
    }
    
    protected void buildCache(int monthlyCacheSize) {
        data = CacheBuilder.newBuilder()
     	       .maximumSize(monthlyCacheSize)
     	       .removalListener(new RemovalListener<DateTime, ReadOnlyData>() {
     	           public void onRemoval(RemovalNotification<DateTime, ReadOnlyData> objectRemovalNotification) {
     	               logger.info(dbName + " removing from file cache " + objectRemovalNotification.getKey());
     	               fileCache.remove(objectRemovalNotification.getKey());
     	           }
     	       })
     	       .build(
     	               new CacheLoader<DateTime, ReadOnlyData>() {
     	                   public ReadOnlyData load(DateTime monthDate) throws Exception {
     	                       return loadData(monthDate);
     	                   }
     	               });
    }
    
    /**
     * We check if new data is available periodically
     * @throws Exception
     */
    @Override
    protected boolean stalePoll() throws Exception {
        logger.info(dbName + " start polling...");
        for (DateTime key: Sets.newHashSet(fileCache.keySet())) {
            File file = fileCache.get(key);
            try {
                logger.info("trying to download " + file);
                boolean downloaded = downloadFile(file);
                if (downloaded) {
                    ReadOnlyData newData = loadDataFromFile(file);
                    data.put(key, newData);
                    fileCache.put(key, file);
                }
            }
            catch (Exception e) {
                logger.error("failed to download " + file, e);
                return true;
            }
        }
        return false;
    }

    @Override
    protected String getThreadName() {
        return this.dbName;
    }

    private ReadOnlyData loadData(DateTime monthDate) throws InterruptedException {
        while (true) {
            File file = getDownloadFile(monthDate);
            try {
                ReadOnlyData result = loadDataFromFile(file);
                fileCache.put(monthDate, file);
                return result;
            }
            catch (FileNotFoundException e) {
                logger.warn("no data for " + monthDate + " " + this.dbName);
                fileCache.put(monthDate, file);
                return new ReadOnlyData(new double[][]{}, Lists.<TagGroup>newArrayList());
            }
            catch (Exception e) {
                logger.error("error in loading data for " + monthDate + " " + this.dbName, e);
                if (file.delete())
                    logger.info("deleted corrupted file " + file);
                else
                    logger.error("not able to delete corrupted file " + file);
                Thread.sleep(2000L);
            }
        }
    }

    private synchronized File getDownloadFile(DateTime monthDate) {
        File file = getFile(monthDate);
        downloadFile(file);
        return file;
    }

    protected File getFile(DateTime monthDate) {
    	String filename = dbName;
        if (consolidateType == ConsolidateType.hourly)
        	filename += "_" + AwsUtils.monthDateFormat.print(monthDate);
        else if (consolidateType == ConsolidateType.daily)
            filename += "_" + monthDate.getYear();
        
        return new File(config.localDir, filename + (compress ? compressExtension : ""));
    }

    protected synchronized boolean downloadFile(File file) {
        try {
            return AwsUtils.downloadFileIfChanged(config.workS3BucketName, config.workS3BucketPrefix, file, 0);
        }
        catch (AmazonServiceException ase) {
        	if (ase.getStatusCode() == 404) {
            	logger.warn("file not found: " + file.getName());
            	if (file.exists()) {
                    logger.info("deleted stale file " + file);
            		file.delete();
            	}
        	}
        	else {
                logger.error("error downloading " + file.getName(), ase);
        	}
            return false;
        }
        catch (Exception e) {
            logger.error("error downloading " + file.getName(), e);
            return false;
        }
    }

    protected ReadOnlyData loadDataFromFile(File file) throws Exception {
        logger.info("trying to load data from " + file);
        InputStream is = new FileInputStream(file);
        if (compress)
        	is = new GZIPInputStream(is);
        DataInputStream in = new DataInputStream(is);
        try {
            ReadOnlyData result = ReadOnlyData.Serializer.deserialize(accountService, productService, in);
            logger.info("done loading data from " + file);
            return result;
        }
        finally {
            in.close();
        }
    }

    protected ReadOnlyData getReadOnlyData(DateTime key) throws ExecutionException {

        ReadOnlyData result = this.data.get(key);

        if (fileCache.get(key) == null) {
            logger.warn(dbName + " cannot find file in fileCache " + key);
            fileCache.put(key, getFile(key));
        }
        return result;
    }
    
    protected Interval getAdjustedInterval(Interval interval) {
    	// For hourly and daily consolidation, we need to start at first of month or year
        DateTime start = startDate;
        DateTime end = startDate;
        
        if (consolidateType == ConsolidateType.hourly) {
            start = interval.getStart().withDayOfMonth(1).withMillisOfDay(0);
            end = interval.getEnd();
        }
        else if (consolidateType == ConsolidateType.daily) {
            start = interval.getStart().withDayOfYear(1).withMillisOfDay(0);
            end = interval.getEnd();
        }

    	Interval adjusted = new Interval(start, end);
    	return adjusted;
    }
    
    protected int getSize(Interval interval) {
        int num = 0;
        if (consolidateType == ConsolidateType.hourly) {
            num = interval.toPeriod(PeriodType.hours()).getHours();
            if (interval.getStart().plusHours(num).isBefore(interval.getEnd()))
                num++;
        }
        else if (consolidateType == ConsolidateType.daily) {
            num = interval.toPeriod(PeriodType.days()).getDays();
            if (interval.getStart().plusDays(num).isBefore(interval.getEnd()))
                num++;
        }
        else if (consolidateType == ConsolidateType.weekly) {
            num = interval.toPeriod(PeriodType.weeks()).getWeeks();
            if (interval.getStart().plusWeeks(num).isBefore(interval.getEnd()))
                num++;
        }
        else if (consolidateType == ConsolidateType.monthly) {
            num = interval.toPeriod(PeriodType.months()).getMonths();
            if (interval.getStart().plusMonths(num).isBefore(interval.getEnd()))
                num++;
        }
        return num;
    }
    
    public int getDataLength(DateTime start) {
        try {
            ReadOnlyData data = getReadOnlyData(start);
            return data.getNum();
        }
        catch (ExecutionException e) {
            logger.error("error in getDataLength for " + start, e);
            return 0;
        }
    }    
}
