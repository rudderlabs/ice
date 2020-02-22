package com.netflix.ice.reader;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.zip.GZIPInputStream;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.tag.Zone.BadZone;

public class ReadOnlyDataTest {
    protected Logger logger = LoggerFactory.getLogger(getClass());
	private static final String dataDir = "src/test/data/";
    
    private static AccountService as;
    private static ProductService ps;
    
	@BeforeClass
	public static void init() throws IOException {
		as = new BasicAccountService();
        ps = new BasicProductService();
	}
	

	@Test
	public void testFileRead() throws IOException, BadZone {
        String filename = "cost_hourly_EC2Instance_2020-01";
        int numUserTags = 15;
       
        File file = new File(dataDir, filename + ".gz");
        
        if (!file.exists()) {
        	// Don't run the test if the file doesn't exist
        	return;
        }
    	InputStream is = new FileInputStream(file);
    	is = new GZIPInputStream(is);
        DataInputStream in = new DataInputStream(is);
        ReadOnlyData data = new ReadOnlyData(numUserTags);
        
        try {
            data.deserialize(as, ps, numUserTags, in, true);
        }
        finally {
            if (in != null)
                in.close();
        }

        String outFilename = dataDir + "/" + filename + "_ro.csv";
        
        FileWriter out;
		out = new FileWriter(outFilename);
        // Output CSV file
		serialize(out, data);
    	out.close();
	}
	
    private void serialize(OutputStreamWriter out, ReadOnlyData data) throws IOException {
    	out.write("num,data,account,region,zone,product,operation,usageType,usageUnits,resource\n");

        for (Integer i = 0; i < data.getNum(); i++) {
            Double[] values = data.getData(i);
        	for (int j = 0; j < values.length; j++) {
	            TagGroup tg = data.tagGroups.get(j);
	            
	            Double v = values[j];
	            if (v == null || v == 0.0)
	            	continue;
	            
	            out.write(i.toString() + ",");
	            out.write(v == null ? "0," : (v.toString() + ","));
	            TagGroup.Serializer.serializeCsv(out, tg);
	            out.write("\n");
        	}
        }
    }
    
}
