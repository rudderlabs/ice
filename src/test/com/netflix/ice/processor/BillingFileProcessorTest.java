package com.netflix.ice.processor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicLineItemProcessorTest;
import com.netflix.ice.basic.BasicLineItemProcessorTest.PricingTerm;
import com.netflix.ice.basic.BasicLineItemProcessorTest.ProcessTest;
import com.netflix.ice.basic.BasicLineItemProcessorTest.Which;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.basic.BasicReservationService;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.IceOptions;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.common.LineItem.LineItemType;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.ReservationService.ReservationPeriod;
import com.netflix.ice.processor.ReservationService.ReservationUtilization;
import com.netflix.ice.processor.LineItemProcessor.Result;
import com.netflix.ice.processor.ReservationProcessorTest.Datum;
import com.netflix.ice.processor.ReservationService.ReservationKey;
import com.netflix.ice.processor.pricelist.InstancePrices;
import com.netflix.ice.processor.pricelist.InstancePrices.ServiceCode;
import com.netflix.ice.processor.pricelist.PriceListService;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.Zone;

public class BillingFileProcessorTest {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    
    private static final String resourcesDir = "src/test/resources/";
    private static final String resourcesReportDir = resourcesDir + "report/";
	private static PriceListService priceListService = null;
	private static Properties properties;
	private static AccountService accountService;
	private static ProductService productService;
	private static Zone ap_southeast_2a;
	

    @BeforeClass
    public static void init() throws Exception {
		ReservationProcessorTest.init();
		priceListService = new PriceListService(resourcesDir, null, null);
		priceListService.init();
        properties = getProperties();        
		accountService = new BasicAccountService(properties);
		productService = new BasicProductService(null);
		
		// Add all the zones we need for our test data		
		Region.AP_SOUTHEAST_2.addZone("ap-southeast-2a");
		
		
		
		ap_southeast_2a = Zone.getZone("ap-southeast-2a");
    }
    
    
	private static Properties getProperties() throws IOException {
		Properties prop = new Properties();
		File file = new File(resourcesReportDir, "ice.properties");
        InputStream is = new FileInputStream(file);
		prop.load(is);
	    is.close();
	    
		return prop;	
	}
	
	
	interface ReportTest {
		public long Process(ProcessorConfig config, DateTime start,
				CostAndUsageData costAndUsageData,
				Instances instances) throws Exception;
		
		public ReservationProcessor getReservationProcessor();
	}
	class CostAndUsageTest implements ReportTest {
		private ReservationProcessor reservationProcessor = null;
		
		public long Process(ProcessorConfig config, DateTime start,
				CostAndUsageData costAndUsageData,
				Instances instances) throws Exception {
			
			CostAndUsageReportProcessor cauProcessor = new CostAndUsageReportProcessor(config);
			reservationProcessor = cauProcessor.getReservationProcessor();
			File manifest = new File(resourcesReportDir, "hourly-cost-and-usage-Manifest.json");
			CostAndUsageReport report = new CostAndUsageReport(manifest, cauProcessor);
			
	    	List<File> files = Lists.newArrayList();
	    	for (String key: report.getReportKeys()) {
				String prefix = key.substring(0, key.lastIndexOf("/") + 1);
				String filename = key.substring(prefix.length());
	    		files.add(new File(resourcesReportDir, filename));
	    	}
	        Long startMilli = report.getStartTime().getMillis();
	        if (startMilli != start.getMillis()) {
	        	logger.error("Data file start time doesn't match config");
	        	return 0L;
	        }
	        return cauProcessor.processReport(report.getStartTime(), report, files,
	        		costAndUsageData, instances);
		}
		
		public ReservationProcessor getReservationProcessor() {
			return reservationProcessor;
		}
	}
	class DetailedBillingReportTest implements ReportTest {
		private ReservationProcessor reservationProcessor = null;

		public long Process(ProcessorConfig config, DateTime start,
				CostAndUsageData costAndUsageData,
				Instances instances) throws Exception {
			
			DetailedBillingReportProcessor dbrProcessor = new DetailedBillingReportProcessor(config);
			reservationProcessor = dbrProcessor.getReservationProcessor();
			File dbr = new File(resourcesReportDir, "aws-billing-detailed-line-items-with-resources-and-tags-2017-08.csv.zip");
			S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
			s3ObjectSummary.setKey("/aws-billing-detailed-line-items-with-resources-and-tags-2017-08.csv.zip");
			DetailedBillingReportProcessor.BillingFile report = dbrProcessor.new BillingFile(s3ObjectSummary, dbrProcessor);
			
	        return dbrProcessor.processReport(start, report, dbr,
	        		costAndUsageData, instances);
		}
		
		public ReservationProcessor getReservationProcessor() {
			return reservationProcessor;
		}
	}
	
	public void testFileData(ReportTest reportTest) throws Exception {
		class BasicTestReservationService extends BasicReservationService {
			BasicTestReservationService(ReservationPeriod term, ReservationUtilization defaultUtilization) {
				super(term, defaultUtilization, false);
			}
			
			@Override
			public void init() {
				// Overridden so that reservation services don't start up
			}
		}
        ReservationPeriod reservationPeriod = ReservationPeriod.valueOf(properties.getProperty(IceOptions.RESERVATION_PERIOD, "oneyear"));
        ReservationUtilization reservationUtilization = ReservationUtilization.valueOf(properties.getProperty(IceOptions.RESERVATION_UTILIZATION, "PARTIAL"));
		BasicReservationService reservationService = new BasicTestReservationService(reservationPeriod, reservationUtilization);
		
		@SuppressWarnings("deprecation")
		AWSCredentialsProvider credentialsProvider = new InstanceProfileCredentialsProvider();
		
		PriceListService priceListService = new PriceListService(null, null, null);
		priceListService.init();
		
		class TestProcessorConfig extends ProcessorConfig {
			public TestProcessorConfig(
		            Properties properties,
		            AWSCredentialsProvider credentialsProvider,
		            AccountService accountService,
		            ProductService productService,
		            ReservationService reservationService,
		            ResourceService resourceService,
		            PriceListService priceListService,
		            boolean compress) throws Exception {
				super(properties, credentialsProvider, accountService, productService, reservationService, resourceService, priceListService, compress);
			}
			
			@Override
			protected void initZones() {
				
			}
		}
		
		ProcessorConfig config = new TestProcessorConfig(
										properties,
										credentialsProvider,
										accountService,
										productService,
										reservationService,
										null,
										priceListService,
										false);
		BillingFileProcessor bfp = ProcessorConfig.billingFileProcessor;
		bfp.init();
		
		// Debug settings
		//bfp.reservationProcessor.setDebugHour(0);
		//bfp.reservationProcessor.setDebugFamily("c4");
    	
		CostAndUsageData costAndUsageData = new CostAndUsageData(null);
        Instances instances = new Instances(null, null, null);
        
		Long startMilli = config.startDate.getMillis();
		Map<ReservationKey, CanonicalReservedInstances> reservations = BasicReservationService.readReservations(new File(resourcesReportDir, "reservation_capacity.csv"));
		reservationService.updateReservations(reservations, accountService, startMilli, productService);
				
		Long endMilli = reportTest.Process(config, config.startDate, costAndUsageData, instances);
		    
        int hours = (int) ((endMilli - startMilli)/3600000L);
        logger.info("cut hours to " + hours);
        costAndUsageData.cutData(hours);
        		
		// Initialize the price lists
    	Map<Product, InstancePrices> prices = Maps.newHashMap();
    	prices.put(productService.getProductByName(Product.ec2Instance), priceListService.getPrices(config.startDate, ServiceCode.AmazonEC2));
    	if (reservationService.hasRdsReservations())
    		prices.put(productService.getProductByName(Product.rdsInstance), priceListService.getPrices(config.startDate, ServiceCode.AmazonRDS));
    	if (reservationService.hasRedshiftReservations())
    		prices.put(productService.getProductByName(Product.redshift), priceListService.getPrices(config.startDate, ServiceCode.AmazonRedshift));

        reportTest.getReservationProcessor().process(config.reservationService, costAndUsageData, null, config.startDate, prices);
        
        logger.info("Finished processing reports, ready to compare results on " + 
        		costAndUsageData.getUsage(null).getTagGroups().size() + " usage tags and " + 
        		costAndUsageData.getCost(null).getTagGroups().size() + " cost tags");
        
		// Read the file with tags to ignore if present
        File ignoreFile = new File(resourcesReportDir, "ignore.csv");
        Set<TagGroup> ignore = null;
        if (ignoreFile.exists()) {
    		try {
    			BufferedReader in = new BufferedReader(new FileReader(ignoreFile));
    			ignore = deserializeTagGroupsCsv(accountService, productService, in);
    			in.close();
    		} catch (Exception e) {
    			logger.error("Error reading ignore tags file " + e);
    		}
        }
                
        File expectedUsage = new File(resourcesReportDir, "usage.csv");
        if (!expectedUsage.exists()) {
        	// Comparison file doesn't exist yet, write out our current results
        	logger.info("Saving reference usage data...");
            writeData(costAndUsageData.getUsage(null), "Cost", expectedUsage);
        }
        else {
        	// Compare results against the expected data
        	logger.info("Comparing against reference usage data...");
        	compareData(costAndUsageData.getUsage(null), "Usage", expectedUsage, accountService, productService, ignore);
        }
        File expectedCost = new File(resourcesReportDir, "cost.csv");
        if (!expectedCost.exists()) {
        	// Comparison file doesn't exist yet, write out our current results
        	logger.info("Saving reference cost data...");
            writeData(costAndUsageData.getCost(null), "Cost", expectedCost);
        }
        else {
        	// Compare results against the expected data
        	logger.info("Comparing against reference cost data...");
        	compareData(costAndUsageData.getCost(null), "Cost", expectedCost, accountService, productService, ignore);
        }
	}
		
	private void writeData(ReadWriteData data, String dataType, File outputFile) {
		FileWriter out;
		try {
			out = new FileWriter(outputFile);
	        data.serializeCsv(out);
	        out.close();
		} catch (Exception e) {
			logger.error("Error writing " + dataType + " file " + e);
		}
	}
	
	private void compareData(ReadWriteData data, String dataType, File expectedFile, AccountService accountService, ProductService productService, Set<TagGroup> ignore) {
		// Read in the expected data
		ReadWriteData expectedData = new ReadWriteData();
		
		// Will print out tags that have the following usage type family. Set to null to disable.
		String debugFamily = null; // "t2";
		
		BufferedReader in;
		try {
			in = new BufferedReader(new FileReader(expectedFile));
			expectedData.deserializeCsv(accountService, productService, in);
			in.close();
		} catch (Exception e) {
			logger.error("Error reading " + dataType + " expected data file " + e);
		}
		
		
		// See that number of hours matches
		assertEquals(dataType+" number of hours doesn't match, expected " + expectedData.getNum() + ", got " + data.getNum(), expectedData.getNum(), data.getNum());
		// For each hour see that the length and entries match
		for (int i = 0; i < data.getNum(); i++) {
			Map<TagGroup, Double> expected = expectedData.getData(i);
			Map<TagGroup, Double> got = data.getData(i);
			int expectedLen = expected.keySet().size();
	        Set<TagGroup> keys = Sets.newTreeSet();
	        keys.addAll(got.keySet());
			int gotLen = keys.size();

	        if (expectedLen != gotLen)
	        	logger.info(dataType+" number of items for hour " + i + " doesn't match, expected " + expectedLen + ", got " + gotLen);
			
			// Count all the tags found vs. not found and output the error printouts in sorted order
			int numFound = 0;
			int numNotFound = 0;
			Set<TagGroup> notFound = Sets.newTreeSet();
			for (Entry<TagGroup, Double> entry: expected.entrySet()) {
				Double gotValue = got.get(entry.getKey());
				if (gotValue == null) {
					if (debugFamily == null || entry.getKey().usageType.name.contains(debugFamily))
						notFound.add(entry.getKey());
					numNotFound++;
				}
				else
					numFound++;
			}
			int numPrinted = 0;
			for (TagGroup tg: notFound) {
				logger.info("Tag not found: " + tg + ", value: " + expected.get(tg));
				if (numPrinted++ > 100)
					break;
			}
				
			// Scan for values in got but not in expected
			int numExtra = 0;
			Set<TagGroup> extras = Sets.newTreeSet();
			for (Entry<TagGroup, Double> entry: got.entrySet()) {
				Double expectedValue = expected.get(entry.getKey());
				if (expectedValue == null) {
					if (debugFamily == null || entry.getKey().usageType.name.contains(debugFamily))
						extras.add(entry.getKey());
					numExtra++;
				}
			}
			numPrinted = 0;
			for (TagGroup tg: extras) {
				logger.info("Extra tag found: " + tg + ", value: " + got.get(tg));
				if (numPrinted++ > 100)
					break;
			}
			if (numNotFound > 0 || numExtra > 0)
				logger.info("Hour "+i+" Tags not found: " + numNotFound + ", found " + numFound + ", extra " + numExtra);
			
			// Compare the values on found tags
			int numMatches = 0;
			int numMismatches = 0;
			if (numFound > 0) {
				for (Entry<TagGroup, Double> entry: got.entrySet()) {
					if (ignore != null && ignore.contains(entry.getKey()))
						continue;
					
					Double gotValue = entry.getValue();
					Double expectedValue = expected.get(entry.getKey());
					if (expectedValue != null) {
						if (Math.abs(expectedValue - gotValue) < 0.001)
							numMatches++;
						else {
							if (numMismatches < 100 && (debugFamily == null || entry.getKey().usageType.name.contains(debugFamily)))
								logger.info(dataType+" non-matching entry for hour " + i + " with tag " + entry.getKey() + ", expected " + expectedValue + ", got " + gotValue);
							numMismatches++;				
						}
					}
				}
				logger.info("Hour "+i+" has " + numMatches + " matches and " + numMismatches + " mismatches");
				assertEquals("Hour "+i+" has " + numMismatches + " incorrect data values", 0, numMismatches);
			}
			assertEquals("Hour "+i+" has " + numNotFound + " tags that were not found", 0, numNotFound);
			assertEquals(dataType+" number of items for hour " + i + " doesn't match, expected " + expectedLen + ", got " + gotLen, expectedLen, gotLen);			
		}
	}
	
	private Set<TagGroup> deserializeTagGroupsCsv(AccountService accountService, ProductService productService, BufferedReader in) throws IOException {
        Set<TagGroup> result = Sets.newTreeSet();

        String line;
        
        // skip the header
        in.readLine();

        while ((line = in.readLine()) != null) {
        	String[] items = line.split(",");        	
        	TagGroup tag = TagGroup.getTagGroup(items[0], items[1], items[2], items[3], items[4], items[5],
        			items.length > 6 ? items[6] : "", 
        			items.length > 7 ? items[7] : "", 
        			accountService, productService);
            result.add(tag);
        }

        return result;
    }


	
	@Test
	public void testCostAndUsageReport() throws Exception {
		testFileData(new CostAndUsageTest());
	}
	
	@Test
	public void testDetailedBillingReport() throws Exception {
		testFileData(new DetailedBillingReportTest());
	}
	
	@Test
	public void testAllUpfrontUsage() throws Exception {
		BasicLineItemProcessorTest.init(accountService);
		BasicLineItemProcessorTest lineItemTest = new BasicLineItemProcessorTest();
		BasicLineItemProcessorTest.accountService = ReservationProcessorTest.accountService;
		BasicLineItemProcessorTest.Line line = lineItemTest.new Line(LineItemType.DiscountedUsage, "111111111111", "ap-southeast-2", "ap-southeast-2a", "Amazon Elastic Compute Cloud", "APS2-BoxUsage:c4.2xlarge", "RunInstances", "USD 0.0 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1", "0", "All Upfront", "reserved-instances/2aaaaaaa-bbbb-cccc-ddddddddddddddddd");
		String[] tag = new String[] { "Account1", "ap-southeast-2", "ap-southeast-2a", "EC2 Instance", "Bonus RIs - All Upfront", "c4.2xlarge", null };
		ProcessTest test = lineItemTest.new ProcessTest(Which.cau, line, tag, 1.0, 0.0, Result.hourly, 30);
		
		BasicLineItemProcessorTest.cauLineItem.setItems(test.line.getCauLine(BasicLineItemProcessorTest.cauLineItem));
		long startMilli = new DateTime("2017-06-01T00:00:00Z", DateTimeZone.UTC).getMillis();
		CostAndUsageData costAndUsageData = test.runProcessTest(BasicLineItemProcessorTest.cauLineItem, "Cost and Usage", true, startMilli);

		long startMillis = 1491004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,ap-southeast-2,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,c4.2xlarge,Availability Zone,ap-southeast-2a,false,1464702209129,1496238208000,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		
		ReservationProcessorTest resTest = new ReservationProcessorTest();

		Datum[] expectedUsage = new Datum[]{
			resTest.new Datum(ReservationProcessorTest.accounts.get(0), Region.AP_SOUTHEAST_2, ap_southeast_2a, Operation.reservedInstancesFixed, "c4.2xlarge", 1.0),
		};
		
		Datum[] expectedCost = new Datum[]{
			resTest.new Datum(ReservationProcessorTest.accounts.get(0), Region.AP_SOUTHEAST_2, ap_southeast_2a, Operation.reservedInstancesFixed, "c4.2xlarge", 0.0),
			resTest.new Datum(ReservationProcessorTest.accounts.get(0), Region.AP_SOUTHEAST_2, ap_southeast_2a, Operation.upfrontAmortizedFixed, "c4.2xlarge", 0.095),
			resTest.new Datum(ReservationProcessorTest.accounts.get(0), Region.AP_SOUTHEAST_2, ap_southeast_2a, Operation.savingsFixed, "c4.2xlarge", 0.522 - 0.095),
		};

		Map<TagGroup, Double> hourUsageData = costAndUsageData.getUsage(null).getData(0);
		Map<TagGroup, Double> hourCostData = costAndUsageData.getCost(null).getData(0);

		List<Map<TagGroup, Double>> ud = new ArrayList<Map<TagGroup, Double>>();
		ud.add(hourUsageData);
		CostAndUsageData caud = new CostAndUsageData(null);
		caud.getUsage(null).setData(ud, 0, false);
		List<Map<TagGroup, Double>> cd = new ArrayList<Map<TagGroup, Double>>();
		cd.add(hourCostData);
		caud.getCost(null).setData(cd, 0, false);

		Set<Account> owners = Sets.newHashSet(ReservationProcessorTest.accounts.get(1));
		List<Account> linked = Lists.newArrayList();
		linked.add(ReservationProcessorTest.accounts.get(1));
		Map<Account, List<Account>> payerAccounts = Maps.newHashMap();
		
		payerAccounts.put(ReservationProcessorTest.accounts.get(0), linked);
		ReservationProcessor rp = new CostAndUsageReservationProcessor(payerAccounts, owners, new BasicProductService(null), priceListService, true);
		ReservationProcessorTest.runTest(startMillis, resCSV, caud, null, "c4", Region.AP_SOUTHEAST_2, rp);

		assertEquals("usage size wrong", expectedUsage.length, hourUsageData.size());
		for (Datum datum: expectedUsage) {
			assertNotNull("should have usage tag group " + datum.tagGroup, hourUsageData.get(datum.tagGroup));	
			assertEquals("wrong usage value for tag " + datum.tagGroup, datum.value, hourUsageData.get(datum.tagGroup), 0.001);
		}
		assertEquals("cost size wrong", expectedCost.length, hourCostData.size());
		for (Datum datum: expectedCost) {
			assertNotNull("should have cost tag group " + datum.tagGroup, hourCostData.get(datum.tagGroup));	
			assertEquals("wrong cost value for tag " + datum.tagGroup, datum.value, hourCostData.get(datum.tagGroup), 0.001);
		}
	}
	
}
