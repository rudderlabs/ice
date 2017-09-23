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

import com.netflix.ice.common.AwsUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class DataWriter {
    private final static Logger logger = LoggerFactory.getLogger(DataWriter.class);

    private static final String compressExtension = ".gz";
    
    private ProcessorConfig config = ProcessorConfig.getInstance();
    private String dbName;
    private File file;
    private boolean compress;
    private ReadWriteData data;

    DataWriter(String name, boolean loadData) throws Exception {
    	this.compress = false;
        dbName = name;
    	init(loadData);
    }
    
    DataWriter(String name, boolean loadData, boolean compress) throws Exception {
    	this.compress = compress;
        dbName = name;
    	init(loadData);
    }
    
    private void init(boolean loadData) throws Exception {
        String filename = dbName + (compress ? compressExtension : "");
        file = new File(config.localDir, filename);
        if (loadData) {
            AwsUtils.downloadFileIfNotExist(config.workS3BucketName, config.workS3BucketPrefix, file);
        }

        if (file.exists()) {
        	InputStream is = new FileInputStream(file);
        	if (compress)
        		is = new GZIPInputStream(is);
            DataInputStream in = new DataInputStream(is);
            try {
                data = ReadWriteData.Serializer.deserialize(config.accountService, config.productService, in);
            }
            catch (Exception e) {
                throw new RuntimeException("DataWriter: failed to load " + filename + ", " + e + ", " + e.getMessage());
            }
            finally {
                in.close();
            }
        }
        else {
            data = new ReadWriteData();
        }
    }

    ReadWriteData getData() {
        return data;
    }

    void archive() throws IOException {
        archive(data);
    }

    void archive(ReadWriteData data) throws IOException {
    	OutputStream os = new FileOutputStream(file);
    	if (compress)
    		os = new GZIPOutputStream(os);
        DataOutputStream out = new DataOutputStream(os);
        try {
            ReadWriteData.Serializer.serialize(out, data);
        	out.flush();
        }
        finally {
            out.close();
        }

        logger.info(this.dbName + " uploading to s3...");
        AwsUtils.upload(config.workS3BucketName, config.workS3BucketPrefix, config.localDir, dbName);
        logger.info(this.dbName + " uploading done.");
    }
}

