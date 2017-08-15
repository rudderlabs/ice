package com.netflix.ice.processor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.joda.time.DateTime;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicLineItemProcessor;
import com.netflix.ice.basic.BasicLineItemProcessorTest;
import com.netflix.ice.basic.BasicLineItemProcessorTest.PricingTerm;
import com.netflix.ice.basic.BasicLineItemProcessorTest.ProcessTest;
import com.netflix.ice.basic.BasicLineItemProcessorTest.Which;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.basic.BasicReservationService;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.IceOptions;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.Ec2InstanceReservationPrice.ReservationPeriod;
import com.netflix.ice.processor.Ec2InstanceReservationPrice.ReservationUtilization;
import com.netflix.ice.processor.LineItemProcessor.Result;
import com.netflix.ice.processor.ReservationProcessorTest.Datum;
import com.netflix.ice.processor.ReservationService.ReservationKey;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.Zone;

public class BillingFileProcessorTest {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    
    private static final String resourcesDir = "src/test/resources/report";

	private Properties getProperties() throws IOException {
		Properties prop = new Properties();
		File file = new File(resourcesDir + "/ice.properties");
        InputStream is = new FileInputStream(file);
		prop.load(is);
	    is.close();
	    
		return prop;	
	}
	
	
	interface ReportTest {
		public Long Process(ProcessorConfig config, DateTime start,
				Map<Product, ReadWriteData> usageDataByProduct,
				Map<Product, ReadWriteData> costDataByProduct,
				Instances instances) throws IOException;
	}
	class CostAndUsageTest implements ReportTest {
		public Long Process(ProcessorConfig config, DateTime start,
				Map<Product, ReadWriteData> usageDataByProduct,
				Map<Product, ReadWriteData> costDataByProduct,
				Instances instances) throws IOException {
			
			CostAndUsageReportProcessor cauProcessor = new CostAndUsageReportProcessor(config);
			File manifest = new File(resourcesDir + "/hourly-cost-and-usage-Manifest.json");
			CostAndUsageReport report = new CostAndUsageReport(manifest, cauProcessor);
			
	    	List<File> files = Lists.newArrayList();
	    	for (String key: report.getReportKeys()) {
				String prefix = key.substring(0, key.lastIndexOf("/") + 1);
				String filename = key.substring(prefix.length());
	    		files.add(new File(resourcesDir + "/" + filename));
	    	}
	        Long startMilli = report.getStartTime().getMillis();
	        if (startMilli != start.getMillis()) {
	        	logger.error("Data file start time doesn't match config");
	        	return 0L;
	        }
	        return cauProcessor.processReport(report.getStartTime(), report, files,
	        		usageDataByProduct, costDataByProduct, instances);
		}
	}
	class DetailedBillingReportTest implements ReportTest {
		public Long Process(ProcessorConfig config, DateTime start,
				Map<Product, ReadWriteData> usageDataByProduct,
				Map<Product, ReadWriteData> costDataByProduct,
				Instances instances) throws IOException {
			
			DetailedBillingReportProcessor dbrProcessor = new DetailedBillingReportProcessor(config);
			File dbr = new File(resourcesDir + "/aws-billing-detailed-line-items-with-resources-and-tags-2017-08.csv.zip");
			S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
			s3ObjectSummary.setKey("/aws-billing-detailed-line-items-with-resources-and-tags-2017-08.csv.zip");
			DetailedBillingReportProcessor.BillingFile report = dbrProcessor.new BillingFile(s3ObjectSummary, dbrProcessor);
			
            List<File> files = Lists.newArrayList();
            files.add(dbr);
			
	        return dbrProcessor.processReport(start, report, files,
	        		usageDataByProduct, costDataByProduct, instances);
		}
	}
	
	public void testFileData(ReportTest reportTest) throws IOException {
        Properties properties = getProperties();
                
		AccountService accountService = new BasicAccountService(properties);
		ProductService productService = new BasicProductService(null);
		class BasicTestReservationService extends BasicReservationService {
			BasicTestReservationService(ReservationPeriod term, ReservationUtilization defaultUtilization) {
				super(term, defaultUtilization);
			}
			
			@Override
			public void init() {
				// Overridden so that reservation services don't start up
			}
		}
        ReservationPeriod reservationPeriod = ReservationPeriod.valueOf(properties.getProperty(IceOptions.RESERVATION_PERIOD, "oneyear"));
        ReservationUtilization reservationUtilization = ReservationUtilization.valueOf(properties.getProperty(IceOptions.RESERVATION_UTILIZATION, "HEAVY_PARTIAL"));
		ReservationService reservationService = new BasicTestReservationService(reservationPeriod, reservationUtilization);
		
		@SuppressWarnings("deprecation")
		AWSCredentialsProvider credentialsProvider = new InstanceProfileCredentialsProvider();
		ProcessorConfig config = new ProcessorConfig(
										properties,
										credentialsProvider,
										accountService,
										productService,
										reservationService,
										null,
										new BasicLineItemProcessor(accountService, productService, reservationService, null, null),
										null);
		BillingFileProcessor bfp = new BillingFileProcessor(config, null, null, null, null);
		bfp.init();
    	
    	Map<Product, ReadWriteData> usageDataByProduct = new HashMap<Product, ReadWriteData>();
    	Map<Product, ReadWriteData> costDataByProduct = new HashMap<Product, ReadWriteData>();
        usageDataByProduct.put(null, new ReadWriteData());
        costDataByProduct.put(null, new ReadWriteData());
        Instances instances = new Instances();
        
		Long startMilli = config.startDate.getMillis();
		Map<ReservationKey, CanonicalReservedInstances> reservations = ReservationCapacityPoller.readReservations(new File(resourcesDir + "/reservation_capacity.csv"));
		reservationService.updateReservations(reservations, accountService, startMilli, productService);
		
		Long endMilli = reportTest.Process(config, config.startDate, usageDataByProduct, costDataByProduct, instances);
		    
        int hours = (int) ((endMilli - startMilli)/3600000L);
        logger.info("cut hours to " + hours);
        for (ReadWriteData data: usageDataByProduct.values()) {
            data.cutData(hours);
        }
        for (ReadWriteData data: costDataByProduct.values()) {
            data.cutData(hours);
        }
        
        // Support line items differ between the two reference reports, so purge all Support tags from both the cost and usage data
        purgeSupportTags(usageDataByProduct.get(null));
        purgeSupportTags(costDataByProduct.get(null));
		
        for (Ec2InstanceReservationPrice.ReservationUtilization utilization: Ec2InstanceReservationPrice.ReservationUtilization.values()) {
        	// We no longer support Light and Medium
        	if (utilization == Ec2InstanceReservationPrice.ReservationUtilization.LIGHT ||
        			utilization == Ec2InstanceReservationPrice.ReservationUtilization.MEDIUM)
        		continue;
        	
        	bfp.reservationProcessor.process(utilization, config.reservationService, usageDataByProduct.get(null), costDataByProduct.get(null), startMilli);
        }
        
        logger.info("Finished processing reports, ready to compare results on " + 
        		usageDataByProduct.get(null).getTagGroups().size() + " usage tags and " + 
        		costDataByProduct.get(null).getTagGroups().size() + " cost tags");
        
		// Read the file with tags to ignore if present
        File ignoreFile = new File(resourcesDir, "ignore.csv");
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
                
        File expectedUsage = new File(resourcesDir, "usage.csv");
        if (!expectedUsage.exists()) {
        	// Comparison file doesn't exist yet, write out our current results
        	logger.info("Saving reference usage data...");
            writeData(usageDataByProduct.get(null), "Cost", expectedUsage);
        }
        else {
        	// Compare results against the expected data
        	logger.info("Comparing against reference usage data...");
        	compareData(usageDataByProduct.get(null), "Usage", expectedUsage, accountService, productService, ignore);
        }
        File expectedCost = new File(resourcesDir, "cost.csv");
        if (!expectedCost.exists()) {
        	// Comparison file doesn't exist yet, write out our current results
        	logger.info("Saving reference cost data...");
            writeData(costDataByProduct.get(null), "Cost", expectedCost);
        }
        else {
        	// Compare results against the expected data
        	logger.info("Comparing against reference cost data...");
        	compareData(costDataByProduct.get(null), "Cost", expectedCost, accountService, productService, ignore);
        }
	}
	
	private void purgeSupportTags(ReadWriteData data) {
		for (int i = 0; i < data.getNum(); i++) {
			Map<TagGroup, Double> hourData = data.getData(i);
			Set<TagGroup> tagGroups = Sets.newTreeSet();
			tagGroups.addAll(hourData.keySet());
			for (TagGroup tg: tagGroups) {
				if (tg.product.isSupport())
					hourData.remove(tg);
			}
		}
	}
	
	private void writeData(ReadWriteData data, String dataType, File outputFile) {
		FileWriter out;
		try {
			out = new FileWriter(outputFile);
	        ReadWriteData.Serializer.serializeCsv(out, data);
	        out.close();
		} catch (Exception e) {
			logger.error("Error writing " + dataType + " file " + e);
		}
	}
	
	private void compareData(ReadWriteData data, String dataType, File expectedFile, AccountService accountService, ProductService productService, Set<TagGroup> ignore) {
		// Read in the expected data
		ReadWriteData expectedData = null;
		
		// Will print out tags that have the following usage type family. Set to null to disable.
		String debugFamily = null; // "t2";
		
		BufferedReader in;
		try {
			in = new BufferedReader(new FileReader(expectedFile));
			expectedData = ReadWriteData.Serializer.deserializeCsv(accountService, productService, in);
			in.close();
		} catch (Exception e) {
			logger.error("Error reading " + dataType + " expected data file " + e);
		}
		
		
		// See that number of hours matches
		assertTrue(dataType+" number of hours doesn't match, expected " + expectedData.getNum() + ", got " + data.getNum(), expectedData.getNum() == data.getNum());
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
			
			// Count all the tags found vs. not found
			int numFound = 0;
			int numNotFound = 0;
			for (Entry<TagGroup, Double> entry: expected.entrySet()) {
				Double gotValue = got.get(entry.getKey());
				if (gotValue == null) {
					if (numNotFound < 100 && (debugFamily == null || entry.getKey().usageType.name.contains(debugFamily)))
						logger.info("Tag not found: " + entry.getKey() + ", value: " + entry.getValue());
					numNotFound++;
				}
				else
					numFound++;
			}
			// Scan for values in got but not in expected
			int numExtra = 0;
			for (Entry<TagGroup, Double> entry: got.entrySet()) {
				Double expectedValue = expected.get(entry.getKey());
				if (expectedValue == null) {
					if (numExtra < 100 && (debugFamily == null || entry.getKey().usageType.name.contains(debugFamily)))
						logger.info("Extra tag found: " + entry.getKey() + ", value: " + entry.getValue());
					numExtra++;
				}
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
				assertTrue("Hour "+i+" has " + numMismatches + " incorrect data values", numMismatches == 0);
			}
			assertTrue("Hour "+i+" has " + numNotFound + " tags that were not found", numNotFound == 0);
			assertTrue(dataType+" number of items for hour " + i + " doesn't match, expected " + expectedLen + ", got " + gotLen, expectedLen == gotLen);			
		}
	}
	
	private Set<TagGroup> deserializeTagGroupsCsv(AccountService accountService, ProductService productService, BufferedReader in) throws IOException {
        Set<TagGroup> result = Sets.newTreeSet();

        String line;
        
        // skip the header
        in.readLine();

        while ((line = in.readLine()) != null) {
        	String[] items = line.split(",");        	
        	TagGroup tag = new TagGroup(items[0], items[1], items[2], items[3], items[4], items[5],
        			items.length > 6 ? items[6] : "", 
        			items.length > 7 ? items[7] : "", 
        			accountService, productService);
            result.add(tag);
        }

        return result;
    }


	
	@Test
	public void testCostAndUsageReport() throws IOException {
		testFileData(new CostAndUsageTest());
	}
	
	@Test
	public void testDetailedBillingReport() throws IOException {
		testFileData(new DetailedBillingReportTest());
	}
	
	@Test
	public void testAllUpfrontUsage() {
		BasicLineItemProcessorTest.processSetup();
		BasicLineItemProcessorTest lineItemTest = new BasicLineItemProcessorTest();
		BasicLineItemProcessorTest.accountService = ReservationProcessorTest.accountService;
		lineItemTest.newBasicLineItemProcessor();
		BasicLineItemProcessorTest.Line line = lineItemTest.new Line(BasicLineItemProcessorTest.LineItemType.DiscountedUsage, "111111111111", "ap-southeast-2a", "Amazon Elastic Compute Cloud", "APS2-BoxUsage:c4.2xlarge", "RunInstances", "USD 0.0 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1", "0", "All Upfront");
		String[] tag = new String[] { "Account1", "ap-southeast-2", "ap-southeast-2a", "EC2 Instance", "Bonus RIs - All Upfront", "c4.2xlarge", null };
		ProcessTest test = lineItemTest.new ProcessTest(Which.both, line, tag, 1, 0.0, Result.hourly);
		
		BasicLineItemProcessorTest.cauLineItem.setItems(test.cauItems);
		lineItemTest.runProcessTest(test, BasicLineItemProcessorTest.cauLineItem, "Cost and Usage", true);

		long startMillis = 1491004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,ap-southeast-2,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,c4.2xlarge,Availability Zone,ap-southeast-2a,false,1464702209129,1496238208000,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		
		ReservationProcessorTest resTest = new ReservationProcessorTest();
		Datum[] expectedUsage = new Datum[]{
			resTest.new Datum(ReservationProcessorTest.accounts.get(0), Region.AP_SOUTHEAST_2, Zone.AP_SOUTHEAST_2A, Operation.reservedInstancesFixed, "c4.2xlarge", 1.0),
		};
		
		Datum[] expectedCost = new Datum[]{
			resTest.new Datum(ReservationProcessorTest.accounts.get(0), Region.AP_SOUTHEAST_2, Zone.AP_SOUTHEAST_2A, Operation.reservedInstancesFixed, "c4.2xlarge", 0.0),
			resTest.new Datum(ReservationProcessorTest.accounts.get(0), Region.AP_SOUTHEAST_2, Zone.AP_SOUTHEAST_2A, Operation.upfrontAmortizedFixed, "c4.2xlarge", 0.095),
		};

		Map<TagGroup, Double> hourUsageData = BasicLineItemProcessorTest.usageDataByProduct.get(null).getData(0);
		Map<TagGroup, Double> hourCostData = BasicLineItemProcessorTest.costDataByProduct.get(null).getData(0);

		List<Map<TagGroup, Double>> ud = new ArrayList<Map<TagGroup, Double>>();
		ud.add(hourUsageData);
		ReadWriteData usage = new ReadWriteData();
		usage.setData(ud, 0, false);
		List<Map<TagGroup, Double>> cd = new ArrayList<Map<TagGroup, Double>>();
		cd.add(hourCostData);
		ReadWriteData cost = new ReadWriteData();
		cost.setData(cd, 0, false);

		Set<Account> owners = Sets.newHashSet(ReservationProcessorTest.accounts.get(1));
		ReservationProcessorTest.runTest(startMillis, resCSV, usage, cost, "c4", owners);

		assertTrue("usage size should be " + expectedUsage.length + ", got " + hourUsageData.size(), hourUsageData.size() == expectedUsage.length);
		for (Datum datum: expectedUsage) {
			assertNotNull("should have usage tag group " + datum.tagGroup, hourUsageData.get(datum.tagGroup));	
			assertTrue("should have usage value " + datum.value + " for tag " + datum.tagGroup + ", got " + hourUsageData.get(datum.tagGroup), hourUsageData.get(datum.tagGroup) == datum.value);
		}
		assertTrue("cost size should be " + expectedCost.length + ", got " + hourCostData.size(), hourCostData.size() == expectedCost.length);
		for (Datum datum: expectedCost) {
			assertNotNull("should have cost tag group " + datum.tagGroup, hourCostData.get(datum.tagGroup));	
			assertEquals("should have cost value for tag " + datum.tagGroup, datum.value, hourCostData.get(datum.tagGroup), 0.001);
		}

	}

}
