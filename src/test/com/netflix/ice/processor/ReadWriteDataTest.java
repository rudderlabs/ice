package com.netflix.ice.processor;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;

public class ReadWriteDataTest {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    private static final String resourcesDir = "src/test/resources";
    
	private Properties getProperties() throws IOException {
		Properties prop = new Properties();
		File file = new File(resourcesDir + "/ice.properties");
        InputStream is = new FileInputStream(file);
		prop.load(is);
	    is.close();
	    
		return prop;	
	}
	

	@Test
	public void testFileRead() throws IOException {
        String filename = "cost_hourly_ec2_instance_2017-06";

        
        File file = new File(resourcesDir, filename);
        
        if (!file.exists()) {
        	// Don't run the test if the file doesn't exist
        	return;
        }
        
        ReadWriteData data;
        
        Properties properties = getProperties();        
		AccountService as = new BasicAccountService(properties);
        ProductService ps = new BasicProductService(null);

        DataInputStream in;
		try {
			in = new DataInputStream(new FileInputStream(file));
		} catch (FileNotFoundException e) {
            throw new RuntimeException("testFileRead: failed to open " + filename + ", " + e.getMessage());
		}
        try {
            data = ReadWriteData.Serializer.deserialize(as, ps, in);
        }
        catch (Exception e) {
        	logger.error(e.toString());
            throw new RuntimeException("testFileRead: failed to load " + filename + ", " + e.getMessage());
        }
        finally {
            try {
				in.close();
			} catch (IOException e) {
			}
        }
        String outFilename = filename + ".csv";
        
        FileWriter out;
		try {
			out = new FileWriter(outFilename);
		} catch (FileNotFoundException e) {
            throw new RuntimeException("testFileRead: failed to create " + outFilename + ", " + e.getMessage());
		}
        try {
            // Output CSV file
        	try {
				ReadWriteDataTest.serialize(out, data);
			} catch (IOException e) {
                throw new RuntimeException("testFileRead: failed to write " + outFilename + ", " + e.getMessage());
			}
        }
        finally {
            try {
				out.close();
			} catch (IOException e) {
			}
        }
	}
    public static void serialize(OutputStreamWriter out, ReadWriteData data) throws IOException {
    	out.write("hour,data,account,region,zone,operation,usageType,usageUnits,resource\n");
        Collection<TagGroup> keys = data.getTagGroups();

        for (Integer i = 0; i < data.getNum(); /*i++*/) {
            Map<TagGroup, Double> map = data.getData(i);
            if (map.size() > 0) {
                for (TagGroup tagGroup: keys) {
                    Double v = map.get(tagGroup);
                    out.write(i.toString() + ",");
                    out.write(v == null ? "0," : (v.toString() + ","));
                    TagGroup.Serializer.serializeCsv(out, tagGroup);
                    out.write("\n");
                }
            }
            break; // only output hour 0 for now
        }
    }

}
