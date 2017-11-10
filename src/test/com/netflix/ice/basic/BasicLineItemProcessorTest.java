package com.netflix.ice.basic;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.netflix.ice.basic.BasicLineItemProcessor.ReformedMetaData;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.LineItem;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.common.LineItem.LineItemType;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.common.TagGroupRI;
import com.netflix.ice.processor.CostAndUsageData;
import com.netflix.ice.processor.CostAndUsageReportLineItem;
import com.netflix.ice.processor.CostAndUsageReportProcessor;
import com.netflix.ice.processor.CostAndUsageReport;
import com.netflix.ice.processor.DetailedBillingReportLineItem;
import com.netflix.ice.processor.ReservationService.ReservationPeriod;
import com.netflix.ice.processor.ReservationService.ReservationUtilization;
import com.netflix.ice.processor.pricelist.InstancePrices;
import com.netflix.ice.processor.pricelist.PriceListService;
import com.netflix.ice.processor.pricelist.InstancePrices.ServiceCode;
import com.netflix.ice.processor.Instances;
import com.netflix.ice.processor.LineItemProcessor.Result;
import com.netflix.ice.processor.ReservationService;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Tag;

public class BasicLineItemProcessorTest {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    
    private static final String resourcesDir = "src/test/resources";

    static final String[] dbrHeader = {
		"InvoiceID","PayerAccountId","LinkedAccountId","RecordType","RecordId","ProductName","RateId","SubscriptionId","PricingPlanId","UsageType","Operation","AvailabilityZone","ReservedInstance","ItemDescription","UsageStartDate","UsageEndDate","UsageQuantity","BlendedRate","BlendedCost","UnBlendedRate","UnBlendedCost"
    };


    public static AccountService accountService = new BasicAccountService(null, null, null, null, null);
    private static ProductService productService = new BasicProductService(null);
    private BasicLineItemProcessor lineItemProcessor;
	private static PriceListService priceListService = null;
    public static CostAndUsageReportProcessor cauProc;
    public static CostAndUsageReportLineItem cauLineItem;
    public static ResourceService resourceService;

    @BeforeClass
    public static void newPriceListService() throws Exception {
		priceListService = new PriceListService(resourcesDir, null, null);
		priceListService.init();
    }
    
    @Before
    public void newBasicLineItemProcessor() {
		ReservationService reservationService = new BasicReservationService(ReservationPeriod.oneyear, ReservationUtilization.PARTIAL, false);
    	
    	lineItemProcessor = new BasicLineItemProcessor(accountService, productService, reservationService, resourceService);    	
    }
    
	@Test
	public void testReformEC2Spot() {
	    ReformedMetaData rmd = lineItemProcessor.reform(ReservationUtilization.HEAVY, productService.getProductByName(Product.ec2), false, "RunInstances:SV001", "USW2-SpotUsage:c4.large", "c4.large Linux/UNIX Spot Instance-hour in US West (Oregon) in VPC Zone #1", 0.02410000, null, "");
	    assertTrue("Operation should be spot instance but got " + rmd.operation, rmd.operation == Operation.spotInstances);
	}

	@Test
	public void testReformEC2ReservedPartialUpfront() {
	    ReformedMetaData rmd = lineItemProcessor.reform(ReservationUtilization.PARTIAL, productService.getProductByName(Product.ec2), true, "RunInstances:0002", "APS2-HeavyUsage:c4.2xlarge", "USD 0.34 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", 0.34, null, "");
	    assertTrue("Operation should be Partial instance but got " + rmd.operation, rmd.operation == Operation.bonusReservedInstancesPartial);
	}

	@Test
	public void testReformEC2ReservedPartialUpfrontWithPurchaseOption() {
	    ReformedMetaData rmd = lineItemProcessor.reform(ReservationUtilization.HEAVY, productService.getProductByName(Product.ec2), true, "RunInstances:0002", "APS2-HeavyUsage:c4.2xlarge", "USD 0.34 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", 0.34, "Partial Upfront", "");
	    assertTrue("Operation should be Partial instance but got " + rmd.operation, rmd.operation == Operation.bonusReservedInstancesPartial);
	}

	@Test
	public void testReformRDSReservedAllUpfront() {
	    ReformedMetaData rmd = lineItemProcessor.reform(ReservationUtilization.FIXED, productService.getProductByName(Product.rds), true, "CreateDBInstance:0002", "APS2-InstanceUsage:db.t2.small", "MySQL, db.t2.small reserved instance applied", 0.0, null, "");
	    assertTrue("Operation should be Fixed instance but got " + rmd.operation, rmd.operation == Operation.bonusReservedInstancesFixed);
	}

	@Test
	public void testReformRDSReservedAllUpfrontWithPurchaseOption() {
	    ReformedMetaData rmd = lineItemProcessor.reform(ReservationUtilization.HEAVY, productService.getProductByName(Product.rds), true, "CreateDBInstance:0002", "APS2-InstanceUsage:db.t2.small", "MySQL, db.t2.small reserved instance applied", 0.0, "All Upfront", "");
	    assertTrue("Operation should be Fixed instance but got " + rmd.operation, rmd.operation == Operation.bonusReservedInstancesFixed);
	}

	@Test
	public void testReformRDSReservedPartialUpfront() {
	    ReformedMetaData rmd = lineItemProcessor.reform(ReservationUtilization.PARTIAL, productService.getProductByName(Product.rds), true, "CreateDBInstance:0002", "APS2-HeavyUsage:db.t2.small", "USD 0.021 hourly fee per MySQL, db.t2.small instance", 0.021, null, "");
	    assertTrue("Operation should be Partial instance but got " + rmd.operation, rmd.operation == Operation.bonusReservedInstancesPartial);
	    assertTrue("Usage type should be db.t2.small.mysql but got " + rmd.usageType, rmd.usageType.name.equals("db.t2.small.mysql"));

	    rmd = lineItemProcessor.reform(ReservationUtilization.PARTIAL, productService.getProductByName(Product.rds), true, "CreateDBInstance:0002", "APS2-InstanceUsage:db.t2.small", "MySQL, db.t2.small reserved instance applied", 0.012, null, "");	    
	    assertTrue("Operation should be Partial instance but got " + rmd.operation, rmd.operation == Operation.bonusReservedInstancesPartial);
	    assertTrue("Usage type should be db.t2.small.mysql but got " + rmd.usageType, rmd.usageType.name.equals("db.t2.small.mysql"));
	}
	
	@Test
	public void testReformRDSReservedPartialUpfrontWithPurchaseOption() {
	    ReformedMetaData rmd = lineItemProcessor.reform(ReservationUtilization.HEAVY, productService.getProductByName(Product.rds), true, "CreateDBInstance:0002", "APS2-HeavyUsage:db.t2.small", "USD 0.021 hourly fee per MySQL, db.t2.small instance", 0.021, "Partial Upfront", "");
	    assertTrue("Operation should be Partial instance but got " + rmd.operation, rmd.operation == Operation.bonusReservedInstancesPartial);
	    assertTrue("Usage type should be db.t2.small.mysql but got " + rmd.usageType, rmd.usageType.name.equals("db.t2.small.mysql"));

	    rmd = lineItemProcessor.reform(ReservationUtilization.HEAVY, productService.getProductByName(Product.rds), true, "CreateDBInstance:0002", "APS2-InstanceUsage:db.t2.small", "MySQL, db.t2.small reserved instance applied", 0.012, "Partial Upfront", "");	    
	    assertTrue("Operation should be Partial instance but got " + rmd.operation, rmd.operation == Operation.bonusReservedInstancesPartial);
	    assertTrue("Usage type should be db.t2.small.mysql but got " + rmd.usageType, rmd.usageType.name.equals("db.t2.small.mysql"));
	}
	
	private boolean tagMatches(Tag tag, String expect) {
		if (tag == null) {
			return expect == null;
		}
		return tag.name.equals(expect);
	}
	private String checkTag(TagGroup tagGroup, String[] tags) {
		StringBuilder errors = new StringBuilder();
		if (!tagMatches(tagGroup.account, tags[0]))
			errors.append("Account mismatch: " + tagGroup.account + "/" + tags[0] + ", ");
		if (!tagMatches(tagGroup.region, tags[1]))
			errors.append("Region mismatch: " + tagGroup.region + "/" + tags[1] + ", ");
		if (!tagMatches(tagGroup.zone, tags[2]))
			errors.append("Zone mismatch: " + tagGroup.zone + "/" + tags[2] + ", ");
		if (!tagMatches(tagGroup.product, tags[3]))
			errors.append("Product mismatch: " + tagGroup.product + "/" + tags[3] + ", ");
		if (!tagMatches(tagGroup.operation, tags[4]))
			errors.append("Operation mismatch: " + tagGroup.operation + "/" + tags[4] + ", ");
		if (!tagMatches(tagGroup.usageType, tags[5]))
			errors.append("UsageType mismatch: " + tagGroup.usageType + "/" + tags[5] + ", ");
		if (!tagMatches(tagGroup.resourceGroup, tags[6]))
			errors.append("ResourceGroup mismatch: " + tagGroup.resourceGroup + "/" + tags[6] + ", ");
		
		String ret = errors.toString();
		if (!ret.isEmpty()) // strip final ", "
			ret = ret.substring(0, ret.length() - 2);
		return ret;
	}
	
	public static enum PricingTerm {
		onDemand,
		reserved,
		spot,
		none
	}
	
	public class Line {
		static final int numDbrItems = 21;
		static final int numCauItems = 141;
		
		public LineItemType lineItemType;
		public String account;
		public String region;
		public String zone;
		public String product;
		public String operation;
		public String type;
		public String description;
		public PricingTerm term;
		public String start;	// Start date in ISO format
		public String end;	// End date in ISO format
		public String quantity;
		public String cost;
		public String purchaseOption;
		public String reservationARN;
		public String resource = "";
		public String environment = "";
		public String email = "";
		
		// For basic testing
		public Line(LineItemType lineItemType, String account, String zone, String product, String type, String operation, 
				String description, PricingTerm term, String start, String end, String quantity, String cost, String purchaseOption,
				String reservationARN) {
			init(lineItemType, account, zone, product, type, operation, 
				description, term, start, end, quantity, cost, purchaseOption, reservationARN);
		}
		
		// For resource testing
		public Line(LineItemType lineItemType, String account, String zone, String product, String type, String operation, 
				String description, PricingTerm term, String start, String end, String quantity, String cost, String purchaseOption,
				String reservationARN, String resource, String environment, String email) {
			init(lineItemType, account, zone, product, type, operation, 
				description, term, start, end, quantity, cost, purchaseOption, reservationARN);
			this.resource = resource;
			this.environment = environment;
			this.email = email;
		}
		
		private void init(LineItemType lineItemType, String account, String zone, String product, String type, String operation, 
				String description, PricingTerm term, String start, String end, String quantity, String cost, String purchaseOption,
				String reservationARN) {
			this.lineItemType = lineItemType;
			this.account = account;
			this.zone = zone;
			this.product = product;
			this.operation = operation;
			this.type = type;
			this.description = description;
			this.term = term;
			this.start = start;
			this.end = end;
			this.quantity = quantity;
			this.cost = cost;
			this.purchaseOption = purchaseOption;
			this.reservationARN = reservationARN;
		}
		
		String[] getDbrLine() {
			String[] items = new String[Line.numDbrItems];
			for (int i = 0; i < items.length; i++)
				items[i] = "";
			items[2] = account;
			items[11] = zone;
			items[5] = product;
			items[10] = operation;
			items[9] = type;
			items[13] = description;
			items[12] = term == PricingTerm.reserved ? "Y" : "";
			items[14] = LineItem.amazonBillingDateFormat.print(LineItem.amazonBillingDateFormatISO.parseDateTime(start));
			items[15] = LineItem.amazonBillingDateFormat.print(LineItem.amazonBillingDateFormatISO.parseDateTime(end));
			items[16] = quantity;
			items[20] = cost;
			return items;
		}
		
		String[] getCauLine() {
	        String[] items = new String[cauLineItem.size()];
			for (int i = 0; i < items.length; i++)
				items[i] = "";
			items[cauLineItem.getAccountIdIndex()] = account;
			items[cauLineItem.getZoneIndex()] = zone;
			items[cauLineItem.getProductIndex()] = product;
			items[cauLineItem.getOperationIndex()] = operation;
			items[cauLineItem.getUsageTypeIndex()] = type;
			items[cauLineItem.getProductUsageTypeIndex()] = type;
			items[cauLineItem.getDescriptionIndex()] = description;
			items[cauLineItem.getReservedIndex()] = term == PricingTerm.reserved ? "Reserved" : term == PricingTerm.spot ? "" : term == PricingTerm.onDemand ? "OnDemand" : "";
			items[cauLineItem.getStartTimeIndex()] = start;
			items[cauLineItem.getEndTimeIndex()] = end;
			items[cauLineItem.getUsageQuantityIndex()] = quantity;
			items[cauLineItem.getLineItemTypeIndex()] = lineItemType.name();
			if (lineItemType == LineItemType.DiscountedUsage)
				items[cauLineItem.getCostIndex()] = "0"; // Discounted usage doesn't carry cost
			else
				items[cauLineItem.getCostIndex()] = cost;
			items[cauLineItem.getPurchaseOptionIndex()] = purchaseOption;
			items[cauLineItem.getReservationArnIndex()] = reservationARN;
			items[cauLineItem.getResourceIndex()] = resource;
			items[cauLineItem.getResourceTagStartIndex() + 1] = environment;
			items[cauLineItem.getResourceTagStartIndex() + 2] = email;			
			return items;
		}
	}
	
	public static enum Which {
		dbr,
		cau,
		both
	}

	public class ProcessTest {
		private String[] dbrItems;
		public String[] cauItems;
		private String[] expectedTag;
		private Double usage; // non-null if we expect a usageTag
		private Double cost; // non-null if we expect a costTag
		private Result result;
		private int daysInMonth;
		private String[] expectedResourceTag = null;
		private Product product = null;
		private Double resourceCost = null;
		
				
		ProcessTest(String[] dbrItems, String[] cauItems, String[] expectedTag, Double usage, Double cost, Result result, int daysInMonth) {
			this.dbrItems = dbrItems;
			this.cauItems = cauItems;
			this.expectedTag = expectedTag;
			this.usage = usage;
			this.cost = cost;
			this.result = result;
			this.daysInMonth = daysInMonth;
		}
		
		public ProcessTest(Which which, Line line, String[] expectedTag, Double usage, Double cost, Result result, int daysInMonth) {
			if (which == Which.dbr || which == Which.both)
				this.dbrItems = line.getDbrLine();
			if (which == Which.cau || which == Which.both)
				this.cauItems = line.getCauLine();
			this.expectedTag = expectedTag;
			this.usage = usage;
			this.cost = cost;
			this.result = result;
			this.daysInMonth = daysInMonth;
		}
		
		public ProcessTest(Which which, Line line, String[] expectedTag, Double usage, Double cost, Result result, int daysInMonth, String[] expectedResourceTag, Product product, Double resourceCost) {
			if (which == Which.dbr || which == Which.both)
				this.dbrItems = line.getDbrLine();
			if (which == Which.cau || which == Which.both)
				this.cauItems = line.getCauLine();
			this.expectedTag = expectedTag;
			this.usage = usage;
			this.cost = cost;
			this.result = result;
			this.daysInMonth = daysInMonth;
			this.expectedResourceTag = expectedResourceTag;
			this.product = product;
			this.resourceCost = resourceCost;
		}		
	}
	
	public void run(ProcessTest t) throws Exception {
        LineItem dbrLineItem = new DetailedBillingReportLineItem(false, true, dbrHeader);
        
		
		if (t.dbrItems != null) {
	        dbrLineItem.setItems(t.dbrItems);
			runProcessTest(t, dbrLineItem, "Detailed Billing", false, priceListService);
		}
		
		if (t.cauItems != null) {
			cauLineItem.setItems(t.cauItems);
			runProcessTest(t, cauLineItem, "Cost and Usage", true, priceListService);
		}
	}
	
	@BeforeClass
	public static void processSetup() throws IOException {
        cauProc = new CostAndUsageReportProcessor(null);
		File manifest = new File(resourcesDir, "manifestTest.json");
        CostAndUsageReport cauReport = new CostAndUsageReport(manifest, cauProc);
        cauLineItem = new CostAndUsageReportLineItem(false, cauReport);
        
		Map<String, List<String>> tagKeys = Maps.newHashMap();
		Map<String, List<String>> tagValues = Maps.newHashMap();
		resourceService = new BasicResourceService(productService, tagKeys, tagValues);
		String[] customTags = new String[]{ "Environment", "Email" };
		resourceService.init(customTags);
    	resourceService.initHeader(cauLineItem.getResourceTagsHeader());
	}
	
	public CostAndUsageData runProcessTest(ProcessTest t, LineItem lineItem, String reportName, boolean isCostAndUsageReport, PriceListService priceListService) throws Exception {
		long startMilli = DateTime.parse("2017-06-01T00:00:00Z").getMillis();
        Map<String, Double> ondemandRate = Maps.newHashMap();
		Instances instances = null;
		CostAndUsageData costAndUsageData = new CostAndUsageData();
		InstancePrices ec2Prices = priceListService.getPrices(new DateTime(startMilli),  ServiceCode.AmazonEC2);
        
		Result result = lineItemProcessor.process(startMilli, false, isCostAndUsageReport, lineItem, costAndUsageData, ec2Prices, ondemandRate, instances);
		assertEquals(reportName + " Incorrect result", t.result, result);
		
		if (result == Result.delay) {
			// Expand the data by number of hours in month
			costAndUsageData.getUsage(null).getData(t.daysInMonth * 24 - 1);
			costAndUsageData.getCost(null).getData(t.daysInMonth * 24 - 1);
			result = lineItemProcessor.process(startMilli, true, isCostAndUsageReport, lineItem, costAndUsageData, ec2Prices, ondemandRate, instances);
		}
		
		// Check usage data
		int gotLen = costAndUsageData.getUsage(null).getTagGroups().size();
		int expectLen = t.expectedTag == null || t.usage == null ? 0 : 1;
		assertEquals(reportName + " Incorrect number of usage tags", expectLen, gotLen);
		if (gotLen > 0) {
			TagGroup got = (TagGroup) costAndUsageData.getUsage(null).getTagGroups().toArray()[0];
			//logger.info("Got Tag: " + got);
			String errors = checkTag(got, t.expectedTag);
			assertTrue(reportName + " Tag is not correct: " + errors, errors.length() == 0);
			double usage = costAndUsageData.getUsage(null).getData(0).get(got);
			assertEquals(reportName + " Usage is incorrect", t.usage, usage, 0.001);
		}
		// Check cost data
		gotLen = costAndUsageData.getCost(null).getTagGroups().size();
		expectLen = t.expectedTag == null || t.cost == null ? 0 : 1;
		assertEquals(reportName + " Incorrect number of cost tags", (t.expectedTag == null ? 0 : 1), gotLen);
		if (gotLen > 0) {
			TagGroup got = (TagGroup) costAndUsageData.getCost(null).getTagGroups().toArray()[0];
			String errors = checkTag(got, t.expectedTag);
			assertTrue(reportName + " Tag is not correct: " + errors, errors.length() == 0);
			double cost = costAndUsageData.getCost(null).getData(0).get(got);
			assertEquals(reportName + " Cost is incorrect", t.cost, cost, 0.001);				
		}
		
		// Check resource cost data
		if (t.product != null && t.resourceCost != null) {
			gotLen = costAndUsageData.getCost(t.product).getTagGroups().size();
			expectLen = t.expectedTag == null || t.cost == null ? 0 : 1;
			assertEquals(reportName + " Incorrect number of resource cost tags", (t.expectedTag == null ? 0 : 1), gotLen);
			if (gotLen > 0) {
				TagGroup got = (TagGroup) costAndUsageData.getCost(t.product).getTagGroups().toArray()[0];
				String errors = checkTag(got, t.expectedResourceTag);
				assertTrue(reportName + " Tag is not correct: " + errors, errors.length() == 0);
				double cost = costAndUsageData.getCost(t.product).getData(0).get(got);
				assertEquals(reportName + " Resource cost is incorrect", t.cost, cost, 0.001);				
				
				if (t.product.isEc2Instance() && isCostAndUsageReport) {
					assertTrue("Tag group is wrong type", got instanceof TagGroupRI);
				}
			}
		}
		
		return costAndUsageData;
	}
	
	@Test
	public void testReservedAllUpfrontUsage() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "234567890123", "ap-southeast-2a", "Amazon Elastic Compute Cloud", "APS2-BoxUsage:c4.2xlarge", "RunInstances:0002", "USD 0.0 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1", "0", "All Upfront", "arn");
		String[] tag = new String[] { "234567890123", "ap-southeast-2", "ap-southeast-2a", "EC2 Instance", "Bonus RIs - All Upfront", "c4.2xlarge.windows", null };
		ProcessTest test = new ProcessTest(Which.both, line, tag, 1.0, 0.0, Result.hourly, 30);
		run(test);
	}
	
	@Test
	public void testReservedPartialUpfrontUsage() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "234567890123", "ap-southeast-2a", "Amazon Elastic Compute Cloud", "APS2-HeavyUsage:c4.2xlarge", "RunInstances:0002", "USD 0.34 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1", "0.34", "Partial Upfront", "arn");
		String[] tag = new String[] { "234567890123", "ap-southeast-2", "ap-southeast-2a", "EC2 Instance", "Bonus RIs - Partial Upfront", "c4.2xlarge.windows", null };
		ProcessTest test = new ProcessTest(Which.dbr, line, tag, 1.0, 0.34, Result.hourly, 30);
		run(test);
	}
	
	@Test
	public void testReservedPartialUpfrontUsageFamily() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "234567890123", "ap-southeast-2a", "Amazon Elastic Compute Cloud", "APS2-HeavyUsage:c4.2xlarge", "RunInstances:0002", "USD 0.34 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "0.25", "0.085", "Partial Upfront", "arn");
		String[] tag = new String[] { "234567890123", "ap-southeast-2", "ap-southeast-2a", "EC2 Instance", "Bonus RIs - Partial Upfront", "c4.2xlarge.windows", null };
		ProcessTest test = new ProcessTest(Which.dbr, line, tag, 0.25, 0.085, Result.hourly, 30);
		run(test);
	}
	
	@Test
	public void testReservedPartialUpfrontDiscountedUsageWithResourceTags() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "234567890123", "ap-southeast-2a", "Amazon Elastic Compute Cloud", "APS2-BoxUsage:c4.2xlarge", "RunInstances:0002", "USD 0.34 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1", "0.34", "Partial Upfront", "arn", "i-0184b2c6d0325157b", "Prod", "john.doe@foobar.com");
		String[] tag = new String[] { "234567890123", "ap-southeast-2", "ap-southeast-2a", "EC2 Instance", "Bonus RIs - Partial Upfront", "c4.2xlarge.windows", null };
		String[] resourceTag = new String[] { "234567890123", "ap-southeast-2", "ap-southeast-2a", "EC2 Instance", "Bonus RIs - Partial Upfront", "c4.2xlarge.windows", "Prod_john.doe@foobar.com" };
		ProcessTest test = new ProcessTest(Which.cau, line, tag, 1.0, 0.0, Result.hourly, 30, resourceTag, productService.getProductByName(Product.ec2Instance), 0.0);
		run(test);
	}
	
	@Test
	public void testReservedPartialUpfrontDiscountedUsageFamily() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "234567890123", "ap-southeast-2a", "Amazon Elastic Compute Cloud", "APS2-BoxUsage:c4.large", "RunInstances:0002", "Linux/UNIX (Amazon VPC), c4.2xlarge reserved instance applied", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1", "0.34", "Partial Upfront", "arn");
		String[] tag = new String[] { "234567890123", "ap-southeast-2", "ap-southeast-2a", "EC2 Instance", "Bonus RIs - Partial Upfront", "c4.large.windows", null };
		ProcessTest test = new ProcessTest(Which.cau, line, tag, 1.0, 0.0, Result.hourly, 30);
		run(test);
	}
	
	@Test
	public void testReservedPartialUpfrontMonthlyFeeDBR() throws Exception {
		Line line = new Line(LineItemType.RIFee, "234567890123", "", "Amazon Elastic Compute Cloud", "APS2-HeavyUsage:c4.2xlarge", "RunInstances:0002", "USD 0.34 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2017-06-30T23:59:59Z", "720", "244.8", "", "arn");
		ProcessTest test = new ProcessTest(Which.dbr, line, null, 1.0, 0.0, Result.ignore, 30);
		run(test);
	}
	
	@Test
	public void testReservedPartialUpfrontMonthlyFee() throws Exception {
		Line line = new Line(LineItemType.RIFee, "234567890123", "", "Amazon Elastic Compute Cloud", "APS2-HeavyUsage:c4.2xlarge", "RunInstances:0002", "USD 0.34 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.none, "2017-06-01T00:00:00Z", "2017-06-30T23:59:59Z", "720", "244.8", "", "arn");
		ProcessTest test = new ProcessTest(Which.cau, line, null, 0.0, 0.0, Result.ignore, 30);
		run(test);
	}
	
	@Test
	public void testReservedPartialUpfrontMonthlyFeeRDS() throws Exception {
		Line line = new Line(LineItemType.RIFee, "234567890123", "", "Amazon Relational Database Service", "APS2-HeavyUsage:db.t2.micro", "CreateDBInstance:0014", "USD 0.012 hourly fee per PostgreSQL, db.t2.micro instance", null, "2017-06-01T00:00:00Z", "2017-06-30T23:59:59Z", "1440", "17.28", "", "arn");
		String[] tag = new String[] { "234567890123", "ap-southeast-2", null, "RDS Instance", "Bonus RIs - Partial Upfront", "db.t2.micro.postgresql", null };
		ProcessTest test = new ProcessTest(Which.cau, line, tag, null, 0.024, Result.delay, 30);
		run(test);
	}
	
	@Test
	public void testReservedPartialUpfrontHourlyUsageRDS() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "234567890123", "", "Amazon Relational Database Service", "APS2-InstanceUsage:db.t2.micro", "CreateDBInstance:0014", "PostgreSQL, db.t2.micro reserved instance applied", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1", "0", "Partial Upfront", "arn");
		String[] tag = new String[] { "234567890123", "ap-southeast-2", null, "RDS Instance", "Bonus RIs - Partial Upfront", "db.t2.micro.postgresql", null };
		ProcessTest test = new ProcessTest(Which.cau, line, tag, 1.0, 0.0, Result.hourly, 30);
		run(test);
	}
	
	@Test
	public void testRIPurchase() throws Exception {
		Line line = new Line(LineItemType.Fee, "234567890123", "", "Amazon Elastic Compute Cloud", "", "", "Sign up charge for subscription: 647735683, planId: 2195643", PricingTerm.reserved, "2017-06-09T21:21:37Z", "2018-06-09T21:21:36Z", "150.0", "9832.500000", "", "arn");
		ProcessTest test = new ProcessTest(Which.both, line, null, 0.0, 0.0, Result.ignore, 30);
		run(test);
	}
	
	@Test
	public void testSpot() throws Exception {
		Line line = new Line(LineItemType.Usage, "234567890123", "", "Amazon Elastic Compute Cloud", "APN2-SpotUsage:c4.xlarge", "RunInstances:SV052", "c4.xlarge Linux/UNIX Spot Instance-hour in Asia Pacific (Seoul) in VPC Zone #52", PricingTerm.spot, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1.00000000", "0.3490000000000", "", "arn");
		String[] tag = new String[] { "234567890123", "ap-northeast-2", null, "EC2 Instance", "Spot Instances", "c4.xlarge", null };
		ProcessTest test = new ProcessTest(Which.both, line, tag, 1.0, 0.349, Result.hourly, 30);
		run(test);
	}
	@Test
	public void testSpotWithResourceTags() throws Exception {
		Line line = new Line(LineItemType.Usage, "234567890123", "", "Amazon Elastic Compute Cloud", "APN2-SpotUsage:c4.xlarge", "RunInstances:SV052", "c4.xlarge Linux/UNIX Spot Instance-hour in Asia Pacific (Seoul) in VPC Zone #52", PricingTerm.spot, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1.00000000", "0.3490000000000", "", "arn", "i-0184b2c6d0325157b", "Prod", "john.doe@foobar.com");
		String[] tag = new String[] { "234567890123", "ap-northeast-2", null, "EC2 Instance", "Spot Instances", "c4.xlarge", null };
		String[] resourceTag = new String[] { "234567890123", "ap-northeast-2", null, "EC2 Instance", "Spot Instances", "c4.xlarge", "Prod_john.doe@foobar.com" };
		ProcessTest test = new ProcessTest(Which.cau, line, tag, 1.0, 0.349, Result.hourly, 30, resourceTag, productService.getProductByName(Product.ec2Instance), 0.0);
		run(test);
	}
}
