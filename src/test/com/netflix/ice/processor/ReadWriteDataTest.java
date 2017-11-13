package com.netflix.ice.processor;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.UsageType;

public class ReadWriteDataTest {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    private static final String resourcesDir = "src/test/resources";
    
    private static AccountService as;
    private static ProductService ps;
    
	private static Properties getProperties() throws IOException {
		Properties prop = new Properties();
		File file = new File(resourcesDir + "/ice.properties");
        InputStream is = new FileInputStream(file);
		prop.load(is);
	    is.close();
	    
		return prop;	
	}
	
	@BeforeClass
	public static void init() throws IOException {
		as = new BasicAccountService(getProperties());
        ps = new BasicProductService(null);
	}
	
	@Test
	public void testFileRead() throws IOException {
        String filename = "cost_daily_all_2017";
       
        File file = new File(resourcesDir, filename + ".gz");
        
        if (!file.exists()) {
        	// Don't run the test if the file doesn't exist
        	return;
        }
    	InputStream is = new FileInputStream(file);
    	is = new GZIPInputStream(is);
        DataInputStream in = new DataInputStream(is);
        ReadWriteData data;
        
		AccountService as = new BasicAccountService(getProperties());
        ProductService ps = new BasicProductService(null);
        try {
            data = ReadWriteData.Serializer.deserialize(as, ps, in);
        }
        finally {
            if (in != null)
                in.close();
        }

        String outFilename = resourcesDir + "/" + filename + ".csv";
        
        FileWriter out;
		out = new FileWriter(outFilename);
        // Output CSV file
		ReadWriteDataTest.serialize(out, data);
    	out.close();
	}
	
	@Test
	public void testSerializeDeserializeRDS() throws IOException {
		TagGroup tg = new TagGroup(as.getAccountByName("Account1"), Region.US_WEST_2, null, ps.getProductByName("RDS"), Operation.getOperation("CreateDBInstance"), UsageType.getUsageType("RDS:GP2-Storage", "GB"), null);
		testSerializeDeserialize(tg, 1.0);
		
		tg = new TagGroup(as.getAccountByName("Account1"), Region.US_WEST_2, null, ps.getProductByName("RDS Service"), Operation.getOperation("CreateDBInstance"), UsageType.getUsageType("RDS:GP2-Storage", "GB"), null);
		testSerializeDeserialize(tg, 1.0);

		tg = new TagGroup(as.getAccountByName("Account1"), Region.US_WEST_2, null, ps.getProductByName("Relational Database Service"), Operation.getOperation("CreateDBInstance"), UsageType.getUsageType("RDS:GP2-Storage", "GB"), null);
		testSerializeDeserialize(tg, 1.0);
	}
	
	private void testSerializeDeserialize(TagGroup tg, Double value) throws IOException {
		ReadWriteData data = new ReadWriteData();
		
        List<Map<TagGroup, Double>> list = Lists.newArrayList();
        Map<TagGroup, Double> map = ReadWriteData.getCreateData(list, 0);
        map.put(tg, value);
		data.setData(list, 0, false);
		
		ReadWriteData result = serializeDeserialize(as, ps, data);
		
		assertEquals("Length of data is wrong", 1, result.getNum());
		assertEquals("Length of first num is wrong", 1, result.getData(0).size());
		assertEquals("Value of first num is wrong", value, result.getData(0).get(tg), 0.001);
	}
	
	@Test
	public void testSerializeDeserializeTwice() throws IOException {
		ReadWriteData data = new ReadWriteData();
		
		TagGroup tg = new TagGroup(as.getAccountByName("Account1"), Region.US_WEST_2, null, ps.getProductByName("Simple Storage Service"), Operation.getOperation("StandardStorage"), UsageType.getUsageType("TimedStorage-ByteHrs", "GB"), null);
        List<Map<TagGroup, Double>> list = Lists.newArrayList();
        Map<TagGroup, Double> map = ReadWriteData.getCreateData(list, 0);
        map.put(tg, 1.0);
		data.setData(list, 0, false);
		
		data = serializeDeserialize(as, ps, data);
		
		TagGroup tg2 = new TagGroup(as.getAccountByName("Account1"), Region.US_WEST_2, null, ps.getProductByName("Simple Storage Service"), Operation.getOperation("StandardStorage"), UsageType.getUsageType("TimedStorage-ByteHrs", "GB"), null);

		list = Lists.newArrayList();
		map = ReadWriteData.getCreateData(list, 0);
		map.put(tg2, 2.0);
		data.setData(list, 1, false);
		
		ReadWriteData result = serializeDeserialize(as, ps, data);
		
		assertEquals("Length of data is wrong", 2, result.getNum());
		assertEquals("Length of first num is wrong", 1, result.getData(0).size());
		assertEquals("Value of first num is wrong", 1.0, result.getData(0).get(tg), 0.001);
		assertEquals("Length of second num is wrong", 1, result.getData(1).size());
		assertEquals("Value of second num is wrong", 2.0, result.getData(1).get(tg2), 0.001);
		assertEquals("Tags don't match", tg, tg2);
	}
	
	ReadWriteData serializeDeserialize(AccountService as, ProductService ps, ReadWriteData data) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(output);
		
		ReadWriteData.Serializer.serialize(out, data);
		ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
		DataInput in = new DataInputStream(input);
		return ReadWriteData.Serializer.deserialize(as, ps, in);
	}
	
    public static void serialize(OutputStreamWriter out, ReadWriteData data) throws IOException {
    	out.write("num,data,account,region,zone,product,operation,usageType,usageUnits,resource\n");
        Collection<TagGroup> keys = data.getTagGroups();

        for (Integer i = 0; i < data.getNum(); i++) {
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
        }
    }
    
}
