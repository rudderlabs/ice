package com.netflix.ice.processor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.Instance;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.Zone;

public class Instances {
    private final static Logger logger = LoggerFactory.getLogger(Instances.class);
    
	private final String localDir;
	private final String workS3BucketName;
	private final String workS3BucketPrefix;
	private ConcurrentMap<String, Instance> data;

	public Instances(String localDir, String workS3BucketName, String workS3BucketPrefix) {
    	this.localDir = localDir;
		this.workS3BucketName = workS3BucketName;
		this.workS3BucketPrefix = workS3BucketPrefix;
		data = Maps.newConcurrentMap();
	}
	
	public void add(String id, String type, Map<String, String> tags, Account account, Region region, Zone zone) {
		if (id.isEmpty()) {
			return;
		}
		data.put(id, new Instance(id, type, account, region, zone, tags));
	}
	
	public Instance get(String id) {
		return data.get(id);
	}
	
	private String getFilename(long timeMillis) {
        DateTime monthDateTime = new DateTime(timeMillis, DateTimeZone.UTC);
		return "instances_" + AwsUtils.monthDateFormat.print(monthDateTime) + ".csv.gz";
	}

    public void archive(long timeMillis) throws IOException {
        
        File file = new File(localDir, getFilename(timeMillis));
        
    	OutputStream os = new FileOutputStream(file);
		os = new GZIPOutputStream(os);        
		Writer out = new OutputStreamWriter(os);
        
        try {
        	// Write the header
        	out.write(Instance.header());
            for (Instance instance: data.values()) {
                out.write(instance.serialize());
            }
        	out.flush();
        }
        finally {
            out.close();
        }

        // archive to s3
        logger.info("uploading " + file + "...");
        AwsUtils.upload(workS3BucketName, workS3BucketPrefix, localDir, file.getName());
        logger.info("uploaded " + file);
    }
    
    public void retrieve(long timeMillis, AccountService accountService) {
        File file = new File(localDir, getFilename(timeMillis));
    	
        // read from s3 if not exists
        boolean downloaded = false;
        
        try {
            downloaded = AwsUtils.downloadFileIfChanged(workS3BucketName, workS3BucketPrefix, file, 0);
        }
        catch (Exception e) {
            logger.error("error downloading " + file, e);
            return;
        }
        if (downloaded || (data.size() == 0 && file.exists())) {
            BufferedReader reader = null;
            try {
            	InputStream is = new FileInputStream(file);
            	is = new GZIPInputStream(is);
                reader = new BufferedReader(new InputStreamReader(is));
                String line;
                
                // skip the header
                reader.readLine();

            	ConcurrentMap<String, Instance> dataMap = Maps.newConcurrentMap();
            	
                while ((line = reader.readLine()) != null) {
                	Instance instance = Instance.deserialize(line, accountService);
                	dataMap.put(instance.id, instance);
                }
                data = dataMap;
            }
            catch (Exception e) {
            	Logger logger = LoggerFactory.getLogger(ReservationService.class);
            	logger.error("error in reading " + file, e);
            }
            finally {
                if (reader != null)
                    try {reader.close();} catch (Exception e) {}
            }
        }        
    }
}
