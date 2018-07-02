package com.netflix.ice.basic;

import java.io.File;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;

import org.joda.time.DateTime;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ConsolidateType;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.reader.ReadOnlyData;
import com.netflix.ice.tag.Product;

public class DataFilePollerTest {
    protected Logger logger = LoggerFactory.getLogger(getClass());
	private static final String dataDir = "src/test/data/";

	class TestDataFilePoller extends DataFilePoller {
		TestDataFilePoller(DateTime startDate, final String dbName, ConsolidateType consolidateType, boolean compress,
	    		int monthlyCacheSize, AccountService accountService, ProductService productService) {
			super(startDate, dbName, consolidateType, compress, monthlyCacheSize, accountService, productService);
		}
		
		@Override
		protected void buildCache(int monthlyCacheSize) {				
		}
		
		@Override
		public void start() {				
		}
	}

	@Test
	public void testLoadDataFromFile() throws Exception {
		AccountService as = new BasicAccountService(new Properties());
		ProductService ps = new BasicProductService(new Properties());
		
	    DataFilePoller data = new TestDataFilePoller(DateTime.now(), null, null, true, 0, as, ps);
	    
	    //File f = new File(dataDir + "coverage_hourly_Vertical_2018-06.gz");
	    File f = new File(dataDir + "cost_hourly_EC2_Instance_2018-06.gz");
	    
	    ReadOnlyData rod = data.loadDataFromFile(f);
	    
	    logger.info("File: " + f + " has " + rod.getTagGroups().size() + " tag groups and "+ rod.getNum() + " hours of data");
	    
	    SortedSet<Product> products = new TreeSet<Product>();
	    for (TagGroup tg: rod.getTagGroups())
	    	products.add(tg.product);
	
	    logger.info("Products:");
	    for (Product p: products)
	    	logger.info("  " + p.name);
	}

}
