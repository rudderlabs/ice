package com.netflix.ice.basic;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.netflix.ice.basic.BasicLineItemProcessor.ReformedMetaData;
import com.netflix.ice.basic.BasicReservationService.Reservation;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.LineItem;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.common.LineItem.LineItemType;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.common.TagGroupRI;
import com.netflix.ice.processor.CostAndUsageData;
import com.netflix.ice.processor.CostAndUsageReportLineItemProcessor;
import com.netflix.ice.processor.CostAndUsageReportLineItem;
import com.netflix.ice.processor.CostAndUsageReportProcessor;
import com.netflix.ice.processor.CostAndUsageReport;
import com.netflix.ice.processor.DetailedBillingReportLineItem;
import com.netflix.ice.processor.ReadWriteData;
import com.netflix.ice.processor.ReservationService.ReservationPeriod;
import com.netflix.ice.processor.ReservationService.ReservationUtilization;
import com.netflix.ice.processor.Instances;
import com.netflix.ice.processor.LineItemProcessor.Result;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Operation.ReservationOperation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.Tag;

public class BasicLineItemProcessorTest {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    
    private static final String resourcesDir = "src/test/resources";
    private static final String manifest2017 = "manifestTest.json";
    private static final String manifest2018 = "manifest-2018-01.json";
    private static final String manifest2019 = "manifest-2019-01.json";

    static final String[] dbrHeader = {
		"InvoiceID","PayerAccountId","LinkedAccountId","RecordType","RecordId","ProductName","RateId","SubscriptionId","PricingPlanId","UsageType","Operation","AvailabilityZone","ReservedInstance","ItemDescription","UsageStartDate","UsageEndDate","UsageQuantity","BlendedRate","BlendedCost","UnBlendedRate","UnBlendedCost"
    };


    public static AccountService accountService = null;
    private static ProductService productService = null;
    public static CostAndUsageReportLineItem cauLineItem;
    public static ResourceService resourceService;

    @BeforeClass
	public static void beforeClass() throws Exception {
    	init(new BasicAccountService(null, null, null, null));
    }
    
    public static void init(AccountService as) throws Exception {
    	accountService = as;
    	
		Properties props = new Properties();
		props.setProperty("RDS", "Relational Database Service");
		props.setProperty("EC2", "Elastic Compute Cloud");
		productService = new BasicProductService(props);

		cauLineItem = newCurLineItem(manifest2017, null);
        
		Map<String, List<String>> tagKeys = Maps.newHashMap();
		Map<String, List<String>> tagValues = Maps.newHashMap();
		String[] customTags = new String[]{ "Environment", "Email" };
		resourceService = new BasicResourceService(productService, customTags, new String[]{}, tagKeys, tagValues, null);
	}
    
    private static CostAndUsageReportLineItem newCurLineItem(String manifestFilename, DateTime costAndUsageNetUnblendedStartDate) throws IOException {
		CostAndUsageReportProcessor cauProc = new CostAndUsageReportProcessor(null);
		File manifest = new File(resourcesDir, manifestFilename);
        CostAndUsageReport cauReport = new CostAndUsageReport(manifest, cauProc);
        return new CostAndUsageReportLineItem(false, costAndUsageNetUnblendedStartDate, cauReport);
    }
    
    public BasicLineItemProcessor newBasicLineItemProcessor() {
    	return newBasicLineItemProcessor(cauLineItem, null);
    }
	
    public BasicLineItemProcessor newBasicLineItemProcessor(LineItem lineItem, Reservation reservation) {
		BasicReservationService reservationService = new BasicReservationService(ReservationPeriod.oneyear, ReservationUtilization.PARTIAL, false);
		if (reservation != null)
			reservationService.injectReservation(reservation);
    	
    	resourceService.initHeader(lineItem.getResourceTagsHeader());
    	if (lineItem instanceof CostAndUsageReportLineItem)
    		return new CostAndUsageReportLineItemProcessor(accountService, productService, reservationService, resourceService);
    	else
    		return new BasicLineItemProcessor(accountService, productService, reservationService, resourceService);    	
    }
    
	@Test
	public void testReformEC2Spot() {
	    ReformedMetaData rmd = newBasicLineItemProcessor().reform(ReservationUtilization.HEAVY, productService.getProductByName(Product.ec2), false, "RunInstances:SV001", "USW2-SpotUsage:c4.large", "c4.large Linux/UNIX Spot Instance-hour in US West (Oregon) in VPC Zone #1", 0.02410000, null, "");
	    assertTrue("Operation should be spot instance but got " + rmd.operation, rmd.operation == Operation.spotInstances);
	}

	@Test
	public void testReformEC2ReservedPartialUpfront() {
	    ReformedMetaData rmd = newBasicLineItemProcessor().reform(ReservationUtilization.PARTIAL, productService.getProductByName(Product.ec2), true, "RunInstances:0002", "APS2-HeavyUsage:c4.2xlarge", "USD 0.34 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", 0.34, null, "");
	    assertTrue("Operation should be Partial instance but got " + rmd.operation, rmd.operation == Operation.bonusReservedInstancesPartial);
	}

	@Test
	public void testReformEC2ReservedPartialUpfrontWithPurchaseOption() {
	    ReformedMetaData rmd = newBasicLineItemProcessor().reform(ReservationUtilization.HEAVY, productService.getProductByName(Product.ec2), true, "RunInstances:0002", "APS2-HeavyUsage:c4.2xlarge", "USD 0.34 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", 0.34, "Partial Upfront", "");
	    assertTrue("Operation should be Partial instance but got " + rmd.operation, rmd.operation == Operation.bonusReservedInstancesPartial);
	}

	@Test
	public void testReformRDSReservedAllUpfront() {
	    ReformedMetaData rmd = newBasicLineItemProcessor().reform(ReservationUtilization.FIXED, productService.getProductByName(Product.rds), true, "CreateDBInstance:0002", "APS2-InstanceUsage:db.t2.small", "MySQL, db.t2.small reserved instance applied", 0.0, null, "");
	    assertTrue("Operation should be Fixed instance but got " + rmd.operation, rmd.operation == Operation.bonusReservedInstancesFixed);
	}

	@Test
	public void testReformRDSReservedAllUpfrontWithPurchaseOption() {
	    ReformedMetaData rmd = newBasicLineItemProcessor().reform(ReservationUtilization.HEAVY, productService.getProductByName(Product.rds), true, "CreateDBInstance:0002", "APS2-InstanceUsage:db.t2.small", "MySQL, db.t2.small reserved instance applied", 0.0, "All Upfront", "");
	    assertTrue("Operation should be Fixed instance but got " + rmd.operation, rmd.operation == Operation.bonusReservedInstancesFixed);
	}

	@Test
	public void testReformRDSReservedPartialUpfront() {
	    ReformedMetaData rmd = newBasicLineItemProcessor().reform(ReservationUtilization.PARTIAL, productService.getProductByName(Product.rds), true, "CreateDBInstance:0002", "APS2-HeavyUsage:db.t2.small", "USD 0.021 hourly fee per MySQL, db.t2.small instance", 0.021, null, "");
	    assertTrue("Operation should be Partial instance but got " + rmd.operation, rmd.operation == Operation.bonusReservedInstancesPartial);
	    assertTrue("Usage type should be db.t2.small.mysql but got " + rmd.usageType, rmd.usageType.name.equals("db.t2.small.mysql"));

	    rmd = newBasicLineItemProcessor().reform(ReservationUtilization.PARTIAL, productService.getProductByName(Product.rds), true, "CreateDBInstance:0002", "APS2-InstanceUsage:db.t2.small", "MySQL, db.t2.small reserved instance applied", 0.012, null, "");	    
	    assertTrue("Operation should be Partial instance but got " + rmd.operation, rmd.operation == Operation.bonusReservedInstancesPartial);
	    assertTrue("Usage type should be db.t2.small.mysql but got " + rmd.usageType, rmd.usageType.name.equals("db.t2.small.mysql"));
	}
	
	@Test
	public void testReformRDSReservedPartialUpfrontWithPurchaseOption() {
	    ReformedMetaData rmd = newBasicLineItemProcessor().reform(ReservationUtilization.HEAVY, productService.getProductByName(Product.rds), true, "CreateDBInstance:0002", "APS2-HeavyUsage:db.t2.small", "USD 0.021 hourly fee per MySQL, db.t2.small instance", 0.021, "Partial Upfront", "");
	    assertTrue("Operation should be Partial instance but got " + rmd.operation, rmd.operation == Operation.bonusReservedInstancesPartial);
	    assertTrue("Usage type should be db.t2.small.mysql but got " + rmd.usageType, rmd.usageType.name.equals("db.t2.small.mysql"));

	    rmd = newBasicLineItemProcessor().reform(ReservationUtilization.HEAVY, productService.getProductByName(Product.rds), true, "CreateDBInstance:0002", "APS2-InstanceUsage:db.t2.small", "MySQL, db.t2.small reserved instance applied", 0.012, "Partial Upfront", "");	    
	    assertTrue("Operation should be Partial instance but got " + rmd.operation, rmd.operation == Operation.bonusReservedInstancesPartial);
	    assertTrue("Usage type should be db.t2.small.mysql but got " + rmd.usageType, rmd.usageType.name.equals("db.t2.small.mysql"));
	}
	
	private boolean tagMatches(Tag tag, String expect) {
		if (tag == null) {
			return expect == null;
		}
		return tag.name.equals(expect);
	}
	
	private final int accountIndex = 0;
	private final int regionIndex = 1;
	private final int zoneIndex = 2;
	private final int productIndex = 3;
	private final int operationIndex = 4;
	private final int usgaeTypeIndex = 5;
	private final int resourceGroupIndex = 6;
		
	public static enum PricingTerm {
		onDemand,
		reserved,
		spot,
		none
	}
	
	public class Line {
		static final int numDbrItems = 21;
		
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
		public String amortization = "";
		public String recurring = "";
		public String publicOnDemandCost = "";
		public String unusedQuantity = "";
		public String unusedAmortizedUpfrontFeeForBillingPeriod = "";
		public String unusedRecurringFee = "";
		
		// For basic testing
		public Line(LineItemType lineItemType, String account, String region, String zone, String product, String type, String operation, 
				String description, PricingTerm term, String start, String end, String quantity, String cost, String purchaseOption,
				String reservationARN) {
			init(lineItemType, account, region, zone, product, type, operation, 
				description, term, start, end, quantity, cost, purchaseOption, reservationARN);
		}
		
		// For testing fields added to CUR in 2018 (amortization and recurring reservation fees) and in 2019 (EDP Net pricing for same)
		public Line(LineItemType lineItemType, String account, String region, String zone, String product, String type, String operation, 
				String description, PricingTerm term, String start, String end, String quantity, String cost, String purchaseOption,
				String reservationARN, String amortization, String recurring, String publicOnDemandCost) {
			init(lineItemType, account, region, zone, product, type, operation, 
				description, term, start, end, quantity, cost, purchaseOption, reservationARN);
			this.amortization = amortization;
			this.recurring = recurring;
			this.publicOnDemandCost = publicOnDemandCost;
		}
				
		private void init(LineItemType lineItemType, String account, String region, String zone, String product, String type, String operation, 
				String description, PricingTerm term, String start, String end, String quantity, String cost, String purchaseOption,
				String reservationARN) {
			this.lineItemType = lineItemType;
			this.account = account;
			this.region = region;
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
		
		// For resource testing
		public void setResources(String resource, String environment, String email) {
			this.resource = resource;
			this.environment = environment;
			this.email = email;
		}
		
		// For RIFee unused testing
		public void setUnused(String unusedQuantity, String unusedAmortizedUpfrontFeeForBillingPeriod, String unusedRecurringFee) {
			this.unusedQuantity = unusedQuantity;
			this.unusedAmortizedUpfrontFeeForBillingPeriod = unusedAmortizedUpfrontFeeForBillingPeriod;
			this.unusedRecurringFee = unusedRecurringFee;
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
		
		public String[] getCauLine(CostAndUsageReportLineItem lineItem) {
	        String[] items = new String[lineItem.size()];
			for (int i = 0; i < items.length; i++)
				items[i] = "";
			items[lineItem.getAccountIdIndex()] = account;
			items[lineItem.getZoneIndex()] = zone;
			items[lineItem.getProductRegionIndex()] = region;
			items[lineItem.getProductIndex()] = product;
			items[lineItem.getOperationIndex()] = operation;
			items[lineItem.getUsageTypeIndex()] = type;
			items[lineItem.getProductUsageTypeIndex()] = type;
			items[lineItem.getDescriptionIndex()] = description;
			items[lineItem.getReservedIndex()] = term == PricingTerm.reserved ? "Reserved" : term == PricingTerm.spot ? "" : term == PricingTerm.onDemand ? "OnDemand" : "";
			items[lineItem.getStartTimeIndex()] = start;
			items[lineItem.getEndTimeIndex()] = end;
			items[lineItem.getUsageQuantityIndex()] = quantity;
			items[lineItem.getLineItemTypeIndex()] = lineItemType.name();
			if (lineItemType == LineItemType.DiscountedUsage)
				items[lineItem.getCostIndex()] = "0"; // Discounted usage doesn't carry cost
			else
				items[lineItem.getCostIndex()] = cost;
			items[lineItem.getPurchaseOptionIndex()] = purchaseOption;
			items[lineItem.getReservationArnIndex()] = reservationARN;
			items[lineItem.getResourceIndex()] = resource;
			if (lineItem.getResourceTagStartIndex() + 2 < items.length) {
				items[lineItem.getResourceTagStartIndex() + 1] = environment;
				items[lineItem.getResourceTagStartIndex() + 2] = email;
			}
			
			int amortIndex = lineItemType == LineItemType.DiscountedUsage ? lineItem.getAmortizedUpfrontCostForUsageIndex() : lineItem.getUnusedAmortizedUpfrontFeeForBillingPeriodIndex();
			if (amortIndex >= 0)
				items[amortIndex] = amortization;
			int recurringIndex = lineItemType == LineItemType.DiscountedUsage ? lineItem.getRecurringFeeForUsageIndex() : lineItem.getUnusedRecurringFeeIndex();
			if (recurringIndex >= 0)
				items[recurringIndex] = recurring;
			if (lineItemType == LineItemType.RIFee) {
				int unusedIndex = lineItem.getUnusedQuantityIndex();
				if (unusedIndex >= 0) {
					items[unusedIndex] = unusedQuantity;
					items[lineItem.getUnusedRecurringFeeIndex()] = unusedRecurringFee;
					items[lineItem.getUnusedAmortizedUpfrontFeeForBillingPeriodIndex()] = unusedAmortizedUpfrontFeeForBillingPeriod;
				}
			}
			if (lineItemType == LineItemType.DiscountedUsage) {
				int publicOnDemandCostIndex = lineItem.getPublicOnDemandCostIndex();
				if (publicOnDemandCostIndex >= 0)
					items[publicOnDemandCostIndex] = this.publicOnDemandCost;
			}
			return items;
		}
	}
	
	public static enum Which {
		dbr,
		cau,
		both
	}

	public class ProcessTest {
		private Which which;
		public Line line;
		private String[] expectedTag;
		private Double usage; // non-null if we expect a usageTag
		private Double cost; // non-null if we expect a costTag
		private Result result;
		private int daysInMonth;
		private String[] expectedResourceTag = null;
		private Product product = null;
		private Double resourceCost = null;
		private Double amortization = null;
		private Double savings = null;
		private boolean delayed = false;
		private int numExpectedUsageTags;
		private int numExpectedCostTags;
		private int numExpectedResourceCostTags;
		private Reservation reservation = null;
				
		public ProcessTest(Which which, Line line, String[] expectedTag, Double usage, Double cost, Result result, int daysInMonth) {
			this.which = which;
			this.line = line;
			this.expectedTag = expectedTag;
			this.usage = usage;
			this.cost = cost;
			this.result = result;
			this.daysInMonth = daysInMonth;
			this.numExpectedUsageTags = this.expectedTag == null || this.usage == null ? 0 : 1;
			this.numExpectedCostTags = this.expectedTag == null || this.cost == null ? 0 : 1;
		}
		
		// Constructor for testing RIFee line item types
		public ProcessTest(Which which, Line line, String[] expectedTag, Double usage, Double cost, Result result, int daysInMonth, Double amortization, boolean delayed, int numExpectedUsageTags, int numExpectedCostTags) {
			this.which = which;
			this.line = line;
			this.expectedTag = expectedTag;
			this.usage = usage;
			this.cost = cost;
			this.result = result;
			this.daysInMonth = daysInMonth;
			this.amortization = amortization;
			this.delayed = delayed;
			this.numExpectedUsageTags = numExpectedUsageTags;
			this.numExpectedCostTags = numExpectedCostTags;
		}
		
		public ProcessTest(Which which, Line line, String[] expectedTag, Double usage, Double cost, Result result, int daysInMonth, Double amortization, Double savings) {
			this.which = which;
			this.line = line;
			this.expectedTag = expectedTag;
			this.usage = usage;
			this.cost = cost;
			this.result = result;
			this.daysInMonth = daysInMonth;
			this.amortization = amortization;
			this.savings = savings;
			this.numExpectedUsageTags = this.expectedTag == null || this.usage == null ? 0 : 1;
			this.numExpectedCostTags = this.expectedTag == null || this.cost == null ? 0 : 1;
			if (this.amortization != null)
				this.numExpectedCostTags++;
			if (this.savings != null)
				this.numExpectedCostTags++;
		}
		
		public void setResources(String[] expectedResourceTag, Product product, Double resourceCost, int numExpectedResourceCostTags) {
			this.expectedResourceTag = expectedResourceTag;
			this.product = product;
			this.resourceCost = resourceCost;
			this.numExpectedResourceCostTags = numExpectedResourceCostTags;
		}
		
		public void addReservation(Reservation res) {
			this.reservation = res;
		}
	
		public void run() throws Exception {        
			long startMilli = new DateTime("2017-06-01T00:00:00Z", DateTimeZone.UTC).getMillis();
			
			if (which == Which.dbr || which == Which.both) {
		        LineItem dbrLineItem = new DetailedBillingReportLineItem(false, true, dbrHeader);
		        dbrLineItem.setItems(line.getDbrLine());
				runProcessTest(dbrLineItem, "Detailed Billing", false, startMilli);
			}
			
			if (which == Which.cau || which == Which.both) {
				CostAndUsageReportLineItem lineItem = newCurLineItem(manifest2017, null);
				lineItem.setItems(line.getCauLine(lineItem));
				runProcessTest(lineItem, "Cost and Usage", true, startMilli);
			}
		}
	
		// Version of run() that allows us to test columns added in 2018 and 2019 for CURs
		public void run(String start, String netUnblendedStart) throws Exception {
			run(start, netUnblendedStart, null);
		}
		
		public void run(String start, String netUnblendedStart, String[] rawLineItem) throws Exception {
			DateTime dt = new DateTime(start, DateTimeZone.UTC);
			String manifest = "";
			switch(dt.getYear()) {
			case 2018:
				manifest = manifest2018;
				break;
			case 2019:
				manifest = manifest2019;
				break;
			default:
				manifest = manifest2017;
				break;
			}
			CostAndUsageReportLineItem lineItem = newCurLineItem(manifest, new DateTime(netUnblendedStart, DateTimeZone.UTC));
			lineItem.setItems(line == null ? rawLineItem : line.getCauLine(lineItem));
			runProcessTest(lineItem, "Cost and Usage", true, dt.getMillis());
		}
	
		public CostAndUsageData runProcessTest(LineItem lineItem, String reportName, boolean isCostAndUsageReport, long startMilli) throws Exception {
			Instances instances = null;
			CostAndUsageData costAndUsageData = new CostAndUsageData(null);
			
			BasicLineItemProcessor lineItemProc = newBasicLineItemProcessor(lineItem, reservation);
	        
			Result result = lineItemProc.process(startMilli, delayed, isCostAndUsageReport, lineItem, costAndUsageData, instances, 0.0);
			assertEquals(reportName + " Incorrect result", this.result, result);
			
			if (result == Result.delay) {
				// Expand the data by number of hours in month
				costAndUsageData.getUsage(null).getData(daysInMonth * 24 - 1);
				costAndUsageData.getCost(null).getData(daysInMonth * 24 - 1);
				result = lineItemProc.process(startMilli, true, isCostAndUsageReport, lineItem, costAndUsageData, instances, 0.0);
			}
			
			// Check usage data
			int gotLen = costAndUsageData.getUsage(null).getTagGroups().size();
			assertEquals(reportName + " Incorrect number of usage tags", numExpectedUsageTags, gotLen);
			if (gotLen > 0) {
				TagGroup got = (TagGroup) costAndUsageData.getUsage(null).getTagGroups().toArray()[0];
				//logger.info("Got Tag: " + got);
				String errors = checkTag(got, expectedTag);
				assertTrue(reportName + " Tag is not correct: " + errors, errors.length() == 0);
				double usage = costAndUsageData.getUsage(null).getData(0).get(got);
				assertEquals(reportName + " Usage is incorrect", usage, usage, 0.001);
			}
			// Check cost data
			gotLen = costAndUsageData.getCost(null).getTagGroups().size();
			assertEquals(reportName + " Incorrect number of cost tags", numExpectedCostTags, gotLen);
			if (gotLen > 0) {
				checkCostAndUsage(costAndUsageData, reportName, null, isCostAndUsageReport, expectedTag);
			}
			
			// Check resource cost data
			if (product != null && resourceCost != null) {
				gotLen = costAndUsageData.getCost(product).getTagGroups().size();
				assertEquals(reportName + " Incorrect number of resource cost tags", numExpectedResourceCostTags, gotLen);
				if (gotLen > 0) {
					checkCostAndUsage(costAndUsageData, reportName, product, isCostAndUsageReport, expectedResourceTag);
				}
			}
			
			return costAndUsageData;
		}
		
		private void checkCostAndUsage(CostAndUsageData costAndUsageData, String reportName, Product p, boolean isCostAndUsageReport, String[] expectedTag) {
			ReadWriteData costData = costAndUsageData.getCost(p);
			for (TagGroup tg: costData.getTagGroups()) {
				// check for matching operation
				if (tg.operation.isAmortized() && amortization != null) {
					String[] amortizedTag = expectedTag.clone();
					amortizedTag[operationIndex] = ReservationOperation.getUpfrontAmortized(((ReservationOperation) tg.operation).getUtilization()).name;
					String errors = checkTag(tg, amortizedTag);
					assertTrue(reportName + " Amortization Tag is not correct: " + errors, errors.length() == 0);
					double cost = costData.getData(0).get(tg);
					assertEquals(reportName + " Cost is incorrect", amortization, cost, 0.001);				
				}
				else if (tg.operation.isSavings() && savings != null) {
					String[] savingsTag = expectedTag.clone();
					savingsTag[operationIndex] = ReservationOperation.getSavings(((ReservationOperation) tg.operation).getUtilization()).name;
					String errors = checkTag(tg, savingsTag);
					assertTrue(reportName + " Savings Tag is not correct: " + errors, errors.length() == 0);
					double cost = costData.getData(0).get(tg);
					assertEquals(reportName + " Cost is incorrect", savings, cost, 0.001);				
				}
				else {
					String errors = checkTag(tg, expectedTag);
					assertTrue(reportName + " Tag is not correct: " + errors, errors.length() == 0);					
					double cost = costData.getData(0).get(tg);
					assertEquals(reportName + " Cost is incorrect", cost, cost, 0.001);				
				}
				if (product != null && product.isEc2Instance() && isCostAndUsageReport) {
					assertTrue("Tag group is wrong type", tg instanceof TagGroupRI);
				}
			}
		}
		
		private String checkTag(TagGroup tagGroup, String[] tags) {
			StringBuilder errors = new StringBuilder();
			if (!tagMatches(tagGroup.account, tags[accountIndex]))
				errors.append("Account mismatch: " + tagGroup.account + "/" + tags[accountIndex] + ", ");
			if (!tagMatches(tagGroup.region, tags[regionIndex]))
				errors.append("Region mismatch: " + tagGroup.region + "/" + tags[regionIndex] + ", ");
			if (!tagMatches(tagGroup.zone, tags[zoneIndex]))
				errors.append("Zone mismatch: " + tagGroup.zone + "/" + tags[zoneIndex] + ", ");
			if (!tagMatches(tagGroup.product, tags[productIndex]))
				errors.append("Product mismatch: " + tagGroup.product + "/" + tags[productIndex] + ", ");
			if (!tagMatches(tagGroup.operation, tags[operationIndex]))
				errors.append("Operation mismatch: " + tagGroup.operation + "/" + tags[operationIndex] + ", ");
			if (!tagMatches(tagGroup.usageType, tags[usgaeTypeIndex]))
				errors.append("UsageType mismatch: " + tagGroup.usageType + "/" + tags[usgaeTypeIndex] + ", ");
			if (!tagMatches(tagGroup.resourceGroup, tags[resourceGroupIndex]))
				errors.append("ResourceGroup mismatch: " + tagGroup.resourceGroup + "/" + tags[resourceGroupIndex] + ", ");
			
			String ret = errors.toString();
			if (!ret.isEmpty()) // strip final ", "
				ret = ret.substring(0, ret.length() - 2);
			return ret;
		}		
	}
	
	@Test
	public void testReservedAllUpfrontUsage() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "234567890123", "ap-southeast-2", "ap-southeast-2a", "Amazon Elastic Compute Cloud", "APS2-BoxUsage:c4.2xlarge", "RunInstances:0002", "USD 0.0 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1", "0", "All Upfront", "arn");
		String[] tag = new String[] { "234567890123", "ap-southeast-2", "ap-southeast-2a", "EC2 Instance", "Bonus RIs - All Upfront", "c4.2xlarge.windows", null };
		ProcessTest test = new ProcessTest(Which.both, line, tag, 1.0, 0.0, Result.hourly, 30);
		test.run();
	}
	
	@Test
	public void testReservedPartialUpfrontUsage() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "234567890123", "ap-southeast-2", "ap-southeast-2a", "Amazon Elastic Compute Cloud", "APS2-HeavyUsage:c4.2xlarge", "RunInstances:0002", "USD 0.34 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1", "0.34", "Partial Upfront", "arn");
		String[] tag = new String[] { "234567890123", "ap-southeast-2", "ap-southeast-2a", "EC2 Instance", "Bonus RIs - Partial Upfront", "c4.2xlarge.windows", null };
		ProcessTest test = new ProcessTest(Which.dbr, line, tag, 1.0, 0.34, Result.hourly, 30);
		test.run();
	}
	
	@Test
	public void testReservedNoUpfrontUsage() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "234567890123", "ap-southeast-2", "ap-southeast-2a", "Amazon Elastic Compute Cloud", "APS2-HeavyUsage:c4.2xlarge", "RunInstances:0002", "USD 0.45 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "2019-01-01T00:00:00Z", "2019-01-01T01:00:00Z", "1", "0.45", "No Upfront", "arn", "0", "0.45", "0.60");
		String[] tag = new String[] { "234567890123", "ap-southeast-2", "ap-southeast-2a", "EC2 Instance", "Bonus RIs - No Upfront", "c4.2xlarge.windows", null };
		ProcessTest test = new ProcessTest(Which.cau, line, tag, 1.0, 0.45, Result.hourly, 31, null, 0.15);
		test.run("2019-01-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
		// Test with resource tags
		line.setResources("i-0184b2c6d0325157b", "Prod", "john.doe@foobar.com");
		String[] resourceTag = new String[] { "234567890123", "ap-southeast-2", "ap-southeast-2a", "EC2 Instance", "Bonus RIs - No Upfront", "c4.2xlarge.windows", "Prod" + ResourceGroup.separator + "john.doe@foobar.com" };
		test = new ProcessTest(Which.cau, line, tag, 1.0, 0.45, Result.hourly, 31, null, 0.15);
		test.setResources(resourceTag, productService.getProductByName(Product.ec2Instance), 0.0, 1);
		test.run("2019-01-01T00:00:00Z", "2019-01-01T00:00:00Z");
	}
	
	@Test
	public void testReservedPartialUpfrontUsageFamily() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "234567890123", "ap-southeast-2", "ap-southeast-2a", "Amazon Elastic Compute Cloud", "APS2-HeavyUsage:c4.2xlarge", "RunInstances:0002", "USD 0.34 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "0.25", "0.085", "Partial Upfront", "arn");
		String[] tag = new String[] { "234567890123", "ap-southeast-2", "ap-southeast-2a", "EC2 Instance", "Bonus RIs - Partial Upfront", "c4.2xlarge.windows", null };
		ProcessTest test = new ProcessTest(Which.dbr, line, tag, 0.25, 0.085, Result.hourly, 30);
		test.run();
	}
	
	@Test
	public void testReservedPartialUpfrontUsageFamilyRDS() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "234567890123", "ap-southeast-2", "ap-southeast-2a", "Amazon Relational Database Service", "APS2-InstanceUsage:db.t2.micro", "CreateDBInstance:0002", "MySQL, db.t2.micro reserved instance applied", PricingTerm.reserved, "2019-01-01T00:00:00Z", "2019-01-01T01:00:00Z", "0.5", "0.0", "Partial Upfront", "arn", "0.00739", "0.00902", "0.026");
		String[] tag = new String[] { "234567890123", "ap-southeast-2", "ap-southeast-2a", "RDS Instance", "Bonus RIs - Partial Upfront", "db.t2.micro.mysql", null };
		ProcessTest test = new ProcessTest(Which.cau, line, tag, 0.25, 0.00902, Result.hourly, 31, 0.00739, 0.00959);
		test.run("2019-01-01T00:00:00Z", "2019-01-01T00:00:00Z");
	}
	
	@Test
	public void testReservedPartialUpfrontDiscountedUsageWithResourceTags() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "234567890123", "ap-southeast-2", "ap-southeast-2a", "Amazon Elastic Compute Cloud", "APS2-BoxUsage:c4.2xlarge", "RunInstances:0002", "USD 0.34 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1", "0.34", "Partial Upfront", "arn");
		line.setResources("i-0184b2c6d0325157b", "Prod", "john.doe@foobar.com");
		String[] tag = new String[] { "234567890123", "ap-southeast-2", "ap-southeast-2a", "EC2 Instance", "Bonus RIs - Partial Upfront", "c4.2xlarge.windows", null };
		String[] resourceTag = new String[] { "234567890123", "ap-southeast-2", "ap-southeast-2a", "EC2 Instance", "Bonus RIs - Partial Upfront", "c4.2xlarge.windows", "Prod" + ResourceGroup.separator + "john.doe@foobar.com" };
		ProcessTest test = new ProcessTest(Which.cau, line, tag, 1.0, 0.0, Result.hourly, 30);
		test.setResources(resourceTag, productService.getProductByName(Product.ec2Instance), 0.0, 1);
		test.run();
	}
	
	@Test
	public void testReservedPartialUpfrontDiscountedUsageFamily() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "234567890123", "ap-southeast-2", "ap-southeast-2a", "Amazon Elastic Compute Cloud", "APS2-BoxUsage:c4.large", "RunInstances:0002", "Linux/UNIX (Amazon VPC), c4.2xlarge reserved instance applied", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1", "0.34", "Partial Upfront", "arn");
		String[] tag = new String[] { "234567890123", "ap-southeast-2", "ap-southeast-2a", "EC2 Instance", "Bonus RIs - Partial Upfront", "c4.large.windows", null };
		ProcessTest test = new ProcessTest(Which.cau, line, tag, 1.0, 0.0, Result.hourly, 30);
		test.run();
		
		line = new Line(LineItemType.DiscountedUsage, "234567890123", "ap-southeast-2", "ap-southeast-2a", "Amazon Elastic Compute Cloud", "APS2-BoxUsage:c4.large", "RunInstances:0002", "Linux/UNIX (Amazon VPC), c4.2xlarge reserved instance applied", PricingTerm.reserved, "2018-01-01T00:00:00Z", "2018-01-01T01:00:00Z", "1", "0.34", "Partial Upfront", "arn", "0.32", "0.36", "1.02");
		tag = new String[] { "234567890123", "ap-southeast-2", "ap-southeast-2a", "EC2 Instance", "Bonus RIs - Partial Upfront", "c4.large.windows", null };
		test = new ProcessTest(Which.cau, line, tag, 1.0, 0.36, Result.hourly, 31, 0.32, 0.34);
		test.run("2018-01-01T00:00:00Z", "2019-01-01T00:00:00Z");

		line = new Line(LineItemType.DiscountedUsage, "234567890123", "ap-southeast-2", "ap-southeast-2a", "Amazon Elastic Compute Cloud", "APS2-BoxUsage:c4.large", "RunInstances:0002", "Linux/UNIX (Amazon VPC), c4.2xlarge reserved instance applied", PricingTerm.reserved, "2019-01-01T00:00:00Z", "2019-01-01T01:00:00Z", "1", "0.34", "Partial Upfront", "arn", "0.32", "0.36", "1.02");
		tag = new String[] { "234567890123", "ap-southeast-2", "ap-southeast-2a", "EC2 Instance", "Bonus RIs - Partial Upfront", "c4.large.windows", null };
		test = new ProcessTest(Which.cau, line, tag, 1.0, 0.36, Result.hourly, 31, 0.32, 0.34);
		test.run("2019-01-01T00:00:00Z", "2019-01-01T00:00:00Z");

	}
	
	@Test
	public void testReservedPartialUpfrontMonthlyFeeDBR() throws Exception {
		Line line = new Line(LineItemType.RIFee, "234567890123", "ap-southeast-2", "", "Amazon Elastic Compute Cloud", "APS2-HeavyUsage:c4.2xlarge", "RunInstances:0002", "USD 0.34 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2017-06-30T23:59:59Z", "720", "244.8", "", "arn");
		ProcessTest test = new ProcessTest(Which.dbr, line, null, 1.0, 0.0, Result.delay, 30);
		test.run();
		
		line = new Line(LineItemType.RIFee, "234567890123", "ap-southeast-2", "", "Amazon Elastic Compute Cloud", "APS2-HeavyUsage:c4.2xlarge", "RunInstances:0002", "USD 0.34 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2017-06-30T23:59:59Z", "720", "244.8", "", "arn");
		test = new ProcessTest(Which.dbr, line, null, 1.0, 0.0, Result.ignore, 30, 0.0, true, 0, 0);
		test.run();
	}
	
	@Test
	public void testReservedNoUpfrontUnusedMonthlyFee() throws Exception {
		Line line = new Line(LineItemType.RIFee, "234567890123", "eu-west-1", "", "Amazon Elastic Compute Cloud", "EU-HeavyUsage:t2.small", "RunInstances", "USD 0.0146 hourly fee per Linux/UNIX (Amazon VPC), t2.small instance", PricingTerm.none, "2019-01-01T00:00:00Z", "2019-02-01T00:00:00Z", "18600", "271.56", "", "arn", "", "", "");
		line.setUnused("95", "0", "1.4");
		String[] tag = new String[] { "234567890123", "eu-west-1", null, "EC2 Instance", "Unused RIs - No Upfront", "t2.small", null };
		ProcessTest test = new ProcessTest(Which.cau, line, tag, 0.0, 32.45, Result.ignore, 31, 0.0, true, 0, 1);
		TagGroup tg = TagGroup.getTagGroup(tag[0], tag[1], tag[2], tag[3], tag[4], tag[5], "hours", null, accountService, productService);
		Reservation r = new Reservation("arn", tg, 25, 0, 0, ReservationUtilization.HEAVY, 0.0, 0.028);
		test.addReservation(r);
		test.run("2019-01-01T00:00:00Z", "2019-02-01T00:00:00Z");
	}
	
	@Test
	public void testReservedPartialUpfrontMonthlyFee() throws Exception {
		Line line = new Line(LineItemType.RIFee, "234567890123", "ap-southeast-2", "", "Amazon Elastic Compute Cloud", "APS2-HeavyUsage:c4.2xlarge", "RunInstances:0002", "USD 0.34 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.none, "2017-06-01T00:00:00Z", "2017-06-30T23:59:59Z", "720", "244.8", "", "arn");
		String[] tag = new String[] { "234567890123", "ap-southeast-2", null, "EC2 Instance", "Bonus RIs - Partial Upfront", "c4.2xlarge.windows", null };
		ProcessTest test = new ProcessTest(Which.cau, line, tag, 0.0, 0.0, Result.monthly, 30, 0.0, true, 0, 1);
		test.run();

		// Test reservation/unused rates
		line = new Line(LineItemType.RIFee, "234567890123", "ap-southeast-2", "", "Amazon Elastic Compute Cloud", "APS2-HeavyUsage:c4.large", "RunInstances", "USD 0.0 hourly fee per Linux/UNIX (Amazon VPC), c4.large instance", PricingTerm.none, "2019-01-01T00:00:00Z", "2019-01-31T23:59:59Z", "744", "0", "", "arn", "34.36895", "32.45", "");
		line.setUnused("1", "34.36895", "0.01");
		tag = new String[] { "234567890123", "ap-southeast-2", null, "EC2 Instance", "Unused RIs - Partial Upfront", "c4.large", null };
		test = new ProcessTest(Which.cau, line, tag, 0.0, 32.45, Result.ignore, 31, 34.36895, true, 0, 2);
		test.run("2019-01-01T00:00:00Z", "2019-02-01T00:00:00Z");
	}
	
	@Test
	public void testReservedPartialUpfrontMonthlyFeeRDS() throws Exception {
		Line line = new Line(LineItemType.RIFee, "234567890123", "ap-southeast-2", "", "Amazon Relational Database Service", "APS2-HeavyUsage:db.t2.micro", "CreateDBInstance:0014", "USD 0.012 hourly fee per PostgreSQL, db.t2.micro instance", null, "2017-06-01T00:00:00Z", "2017-06-30T23:59:59Z", "1440", "17.28", "", "arn");
		String[] tag = new String[] { "234567890123", "ap-southeast-2", null, "RDS Instance", "Bonus RIs - Partial Upfront", "db.t2.micro.postgres", null };
		ProcessTest test = new ProcessTest(Which.cau, line, tag, null, 0.024, Result.delay, 30);
		test.run();
	}
	
	@Test
	public void testReservedPartialUpfrontHourlyUsageRDS() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "234567890123", "ap-southeast-2", "", "Amazon Relational Database Service", "APS2-InstanceUsage:db.t2.micro", "CreateDBInstance:0014", "PostgreSQL, db.t2.micro reserved instance applied", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1", "0", "Partial Upfront", "arn");
		String[] tag = new String[] { "234567890123", "ap-southeast-2", null, "RDS Instance", "Bonus RIs - Partial Upfront", "db.t2.micro.postgres", null };
		ProcessTest test = new ProcessTest(Which.cau, line, tag, 1.0, 0.0, Result.hourly, 30);
		test.run();
	}
	
	@Test
	public void testRIPurchase() throws Exception {
		Line line = new Line(LineItemType.Fee, "234567890123", "ap-southeast-2", "", "Amazon Elastic Compute Cloud", "", "", "Sign up charge for subscription: 647735683, planId: 2195643", PricingTerm.reserved, "2017-06-09T21:21:37Z", "2018-06-09T21:21:36Z", "150.0", "9832.500000", "", "arn");
		ProcessTest test = new ProcessTest(Which.both, line, null, 0.0, 0.0, Result.ignore, 30);
		test.run();
	}
	
	@Test
	public void testSpot() throws Exception {
		Line line = new Line(LineItemType.Usage, "234567890123", "ap-northeast-2", "", "Amazon Elastic Compute Cloud", "APN2-SpotUsage:c4.xlarge", "RunInstances:SV052", "c4.xlarge Linux/UNIX Spot Instance-hour in Asia Pacific (Seoul) in VPC Zone #52", PricingTerm.spot, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1.00000000", "0.3490000000000", "", "arn");
		String[] tag = new String[] { "234567890123", "ap-northeast-2", null, "EC2 Instance", "Spot Instances", "c4.xlarge", null };
		ProcessTest test = new ProcessTest(Which.both, line, tag, 1.0, 0.349, Result.hourly, 30);
		test.run();
	}
	@Test
	public void testSpotWithResourceTags() throws Exception {
		Line line = new Line(LineItemType.Usage, "234567890123", "ap-northeast-2", "", "Amazon Elastic Compute Cloud", "APN2-SpotUsage:c4.xlarge", "RunInstances:SV052", "c4.xlarge Linux/UNIX Spot Instance-hour in Asia Pacific (Seoul) in VPC Zone #52", PricingTerm.spot, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1.00000000", "0.3490000000000", "", "arn");
		line.setResources("i-0184b2c6d0325157b", "Prod", "john.doe@foobar.com");
		String[] tag = new String[] { "234567890123", "ap-northeast-2", null, "EC2 Instance", "Spot Instances", "c4.xlarge", null };
		String[] resourceTag = new String[] { "234567890123", "ap-northeast-2", null, "EC2 Instance", "Spot Instances", "c4.xlarge", "Prod" + ResourceGroup.separator + "john.doe@foobar.com" };
		ProcessTest test = new ProcessTest(Which.cau, line, tag, 1.0, 0.349, Result.hourly, 30);
		test.setResources(resourceTag, productService.getProductByName(Product.ec2Instance), 0.0, 1);
		test.run();
	}
// TODO: add support for credits
//	@Test
//	public void testLambdaCredit() throws Exception {
//		String rawLineItem = "3s5n7gjxiw5rbappdpqwtq5yx2fiwznedfq5qhw2f3jdiecjlx6q,2019-01-01T00:00:00Z/2019-02-01T00:00:00Z,123456789,AWS,Anniversary,123456789012,2019-01-01T00:00:00Z,2019-02-01T00:00:00Z,234567890123,Credit,2019-01-01T00:00:00Z,2019-02-01T00:00:00Z,AWSDataTransfer,USW2-DataTransfer-Regional-Bytes,,,,0,,,USD,,-35.576771,,-35.576771,AWS Lambda Data Transfer Pricing Adjustment,,,-35.576771,Amazon Web Services. Inc.,AWS Data Transfer,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,US West (Oregon),AWS Region,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,Data Transfer,,,,,,us-west-2,,,,,,,,,AWSDataTransfer,AWS Data Transfer,62WNQUZDQQ6GRJC5,,,,,,,,,,,,US West (Oregon),AWS Region,,,IntraRegion,,USW2-DataTransfer-Regional-Bytes,,,,,,,,,,,,331970144,0.0000000000,0.0100000000,OnDemand,GB,,,,,,,,,,,,,,,,,,1387008443,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,";
//		String[] tag = new String[] { "234567890123", "us-west-2", null, "DataTransfer", "", "USW2-DataTransfer-Regional-Bytes", null };
//		ProcessTest test = new ProcessTest(Which.cau, null, tag, 0.0, -35.576771 / 31, Result.monthly, 31);
//		test.run("2019-01-01T00:00:00Z", "2019-02-01T00:00:00Z", rawLineItem.split(","));
//	}
}
