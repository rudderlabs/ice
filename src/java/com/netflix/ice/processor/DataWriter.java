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

import java.io.*;
import java.util.zip.GZIPInputStream;

public class DataWriter extends DataFile {
    private ReadWriteData data;

    DataWriter(String name, boolean compress) throws Exception {
    	super(name, compress);
        data = null;

        AwsUtils.downloadFileIfNotExist(config.workS3BucketName, config.workS3BucketPrefix, file);

        if (file.exists()) {
        	InputStream is = new FileInputStream(file);
        	if (compress)
        		is = new GZIPInputStream(is);
            DataInputStream in = new DataInputStream(is);
            try {
                data = ReadWriteData.Serializer.deserialize(config.accountService, config.productService, in);		
            }
            catch (Exception e) {
                throw new RuntimeException("DataWriter: failed to load " + file.getName() + ", " + e + ", " + e.getMessage());
            }
            finally {
                in.close();
            }
        }
        else {
        	data = new ReadWriteData();
        }
    }
    
    DataWriter(String name, ReadWriteData data, boolean compress) throws Exception {
    	super(name, compress);
    	this.data = data;
    }

	ReadWriteData getData() {
        return data;
    }

	@Override
	protected void write() throws IOException {
    	DataOutputStream out = new DataOutputStream(os);
        try {
        	ReadWriteData.Serializer.serialize(out, data);
    		out.flush();
        }
        finally {
        	out.close();
        }		
	}
}

