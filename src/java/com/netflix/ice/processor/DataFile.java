package com.netflix.ice.processor;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.ice.common.AwsUtils;

public abstract class DataFile {
    private final static Logger logger = LoggerFactory.getLogger(DataWriter.class);

    private static final String compressExtension = ".gz";
    
    protected ProcessorConfig config = ProcessorConfig.getInstance();
    protected String dbName;
    protected File file;
    protected boolean compress;
    
    protected OutputStream os;

    DataFile(String name, boolean compress) throws Exception {
    	this.compress = compress;
        dbName = name;
        os = null;
        
        String filename = dbName + (compress ? compressExtension : "");
        file = new File(config.localDir, filename);
    }
    
    // For unit testing
    protected DataFile() {
    }
    
    public void open() throws IOException {
    	os = new FileOutputStream(file);
    	if (compress)
    		os = new GZIPOutputStream(os);
    }
    
    public void close() throws IOException {
    	os.close();
    	
        logger.info(this.dbName + " uploading to s3...");
        AwsUtils.upload(config.workS3BucketName, config.workS3BucketPrefix, config.localDir, dbName);
        logger.info(this.dbName + " uploading done.");    	
    }
    
    void archive() throws IOException {
    	open();
    	write();
    	close();
    }

    abstract protected void write() throws IOException;
}
