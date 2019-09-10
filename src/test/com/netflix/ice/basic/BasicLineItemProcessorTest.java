package com.netflix.ice.basic;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.ice.basic.BasicLineItemProcessor.ReformedMetaData;
import com.netflix.ice.basic.BasicReservationService.Reservation;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.LineItem;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.common.Config.TagCoverage;
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
    
    private static final String ec2 = "Amazon Elastic Compute Cloud";
    private static final String rds = "Amazon Relational Database Service";
    private static final String es = "Amazon Elasticsearch Service";
    private static final String ec = "Amazon ElastiCache";

    static final String[] dbrHeader = {
		"InvoiceID","PayerAccountId","LinkedAccountId","RecordType","RecordId","ProductName","RateId","SubscriptionId","PricingPlanId","UsageType","Operation","AvailabilityZone","ReservedInstance","ItemDescription","UsageStartDate","UsageEndDate","UsageQuantity","BlendedRate","BlendedCost","UnBlendedRate","UnBlendedCost"
    };


    public static AccountService accountService = null;
    private static ProductService productService = null;
    public static CostAndUsageReportLineItem cauLineItem;
    public static ResourceService resourceService;

    @BeforeClass
	public static void beforeClass() throws Exception {
    	init(new BasicAccountService());
    }
    
    public static void init(AccountService as) throws Exception {
    	accountService = as;
    	
		productService = new BasicProductService();

		cauLineItem = newCurLineItem(manifest2017, null);
        
		String[] customTags = new String[]{ "Environment", "Email" };
		resourceService = new BasicResourceService(productService, customTags, new String[]{});
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
		BasicReservationService reservationService = new BasicReservationService(ReservationPeriod.oneyear, ReservationUtilization.PARTIAL);
		if (reservation != null)
			reservationService.injectReservation(reservation);
    	
    	resourceService.initHeader(lineItem.getResourceTagsHeader(), "123456789012");
    	if (lineItem instanceof CostAndUsageReportLineItem)
    		return new CostAndUsageReportLineItemProcessor(accountService, productService, reservationService, resourceService);
    	else
    		return new BasicLineItemProcessor(accountService, productService, reservationService, resourceService);    	
    }
    
    private ReformedMetaData testReform(Line line, ReservationUtilization utilization) throws IOException {
		CostAndUsageReportLineItem lineItem = newCurLineItem(manifest2017, null);
		lineItem.setItems(line.getCauLine(lineItem));
		return newBasicLineItemProcessor().reform(0L, lineItem, utilization);
    }
    
	@Test
	public void testReformEC2Spot() throws IOException {
		Line line = new Line(ec2, "RunInstances:SV001", "USW2-SpotUsage:c4.large", "c4.large Linux/UNIX Spot Instance-hour in US West (Oregon) in VPC Zone #1", null, "0.02410000", null);
	    ReformedMetaData rmd = testReform(line, ReservationUtilization.NO);
	    assertTrue("Operation should be spot instance but got " + rmd.operation, rmd.operation == Operation.spotInstances);
	}

	@Test
	public void testReformEC2ReservedPartialUpfront() throws IOException {
		Line line = new Line(ec2, "RunInstances:0002", "APS2-HeavyUsage:c4.2xlarge", "USD 0.34 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "0.34", null);
	    ReformedMetaData rmd = testReform(line, ReservationUtilization.PARTIAL);
	    assertTrue("Operation should be Partial instance but got " + rmd.operation, rmd.operation == Operation.bonusReservedInstancesPartialUpfront);
	}

	@Test
	public void testReformEC2ReservedPartialUpfrontWithPurchaseOption() throws IOException {
		Line line = new Line(ec2, "RunInstances:0002", "APS2-HeavyUsage:c4.2xlarge", "USD 0.34 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "0.34", "Partial Upfront");
	    ReformedMetaData rmd = testReform(line, ReservationUtilization.NO);
	    assertTrue("Operation should be Partial instance but got " + rmd.operation, rmd.operation == Operation.bonusReservedInstancesPartialUpfront);
	}

	@Test
	public void testReformRDSReservedAllUpfront() throws IOException {
		Line line = new Line(rds, "CreateDBInstance:0002", "APS2-InstanceUsage:db.t2.small", "MySQL, db.t2.small reserved instance applied", PricingTerm.reserved, "0.0", null);
	    ReformedMetaData rmd = testReform(line, ReservationUtilization.ALL);
	    assertTrue("Operation should be All instance but got " + rmd.operation, rmd.operation == Operation.bonusReservedInstancesAllUpfront);
	}

	@Test
	public void testReformRDSReservedAllUpfrontWithPurchaseOption() throws IOException {
		Line line = new Line(rds, "CreateDBInstance:0002", "APS2-InstanceUsage:db.t2.small", "MySQL, db.t2.small reserved instance applied", PricingTerm.reserved, "0.0", "All Upfront");
	    ReformedMetaData rmd = testReform(line, ReservationUtilization.NO);
	    assertTrue("Operation should be All instance but got " + rmd.operation, rmd.operation == Operation.bonusReservedInstancesAllUpfront);
	}

	@Test
	public void testReformRDSReservedPartialUpfront() throws IOException {
		Line line = new Line(rds, "CreateDBInstance:0002", "APS2-HeavyUsage:db.t2.small", "USD 0.021 hourly fee per MySQL, db.t2.small instance", PricingTerm.reserved, "0.021", null);
	    ReformedMetaData rmd = testReform(line, ReservationUtilization.PARTIAL);
	    assertTrue("Operation should be Partial instance but got " + rmd.operation, rmd.operation == Operation.bonusReservedInstancesPartialUpfront);
	    assertTrue("Usage type should be db.t2.small.mysql but got " + rmd.usageType, rmd.usageType.name.equals("db.t2.small.mysql"));

	    line = new Line(rds, "CreateDBInstance:0002", "APS2-InstanceUsage:db.t2.small", "MySQL, db.t2.small reserved instance applied", PricingTerm.reserved, "0.012", null);	    
	    rmd = testReform(line, ReservationUtilization.PARTIAL);
	    assertTrue("Operation should be Partial instance but got " + rmd.operation, rmd.operation == Operation.bonusReservedInstancesPartialUpfront);
	    assertTrue("Usage type should be db.t2.small.mysql but got " + rmd.usageType, rmd.usageType.name.equals("db.t2.small.mysql"));
	}
	
	@Test
	public void testReformRDSReservedPartialUpfrontWithPurchaseOption() throws IOException {
		Line line = new Line(rds, "CreateDBInstance:0002", "APS2-HeavyUsage:db.t2.small", "USD 0.021 hourly fee per MySQL, db.t2.small instance", PricingTerm.reserved, "0.021", "Partial Upfront");
	    ReformedMetaData rmd = testReform(line, ReservationUtilization.NO);
	    assertTrue("Operation should be Partial instance but got " + rmd.operation, rmd.operation == Operation.bonusReservedInstancesPartialUpfront);
	    assertTrue("Usage type should be db.t2.small.mysql but got " + rmd.usageType, rmd.usageType.name.equals("db.t2.small.mysql"));

	    line = new Line(rds, "CreateDBInstance:0002", "APS2-InstanceUsage:db.t2.small", "MySQL, db.t2.small reserved instance applied", PricingTerm.reserved, "0.012", "Partial Upfront");	    
	    rmd = testReform(line, ReservationUtilization.NO);
	    assertTrue("Operation should be Partial instance but got " + rmd.operation, rmd.operation == Operation.bonusReservedInstancesPartialUpfront);
	    assertTrue("Usage type should be db.t2.small.mysql but got " + rmd.usageType, rmd.usageType.name.equals("db.t2.small.mysql"));
	}
	
	private boolean tagMatches(Tag tag, String expect) {
		if (tag == null) {
			return expect == null;
		}
		return tag.name.equals(expect);
	}
	
	private final int regionIndex = 0;
	private final int zoneIndex = 1;
	private final int productIndex = 2;
	private final int operationIndex = 3;
	private final int usgaeTypeIndex = 4;
	private final int resourceGroupIndex = 5;
		
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
		public String amortizedUpfrontFeeForBillingPeriod = "";
		public String unusedQuantity = "";
		public String unusedAmortizedUpfrontFeeForBillingPeriod = "";
		public String unusedRecurringFee = "";
		public String numberOfReservations = "";
		public String reservationStartTime = "";
		public String reservationEndTime = "";
		public String normalizationFactor = "";
		
		// For basic testing
		public Line(LineItemType lineItemType, String region, String zone, String product, String type, String operation, 
				String description, PricingTerm term, String start, String end, String quantity, String cost, String purchaseOption) {
			init(lineItemType, "234567890123", region, zone, product, type, operation, 
				description, term, start, end, quantity, cost, purchaseOption, "arn");
		}
		
		// For testing reform method in BasicLineItemProcessor
		public Line(String product, String operation, String type, String description, PricingTerm term, String cost, String purchaseOption) {
			init(null, null, null, null, product, type, operation, 
					description, term, null, null, null, cost, purchaseOption, null);
			lineItemType = LineItemType.Usage;
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
		
		// For DiscountedUsage testing
		public void setDiscountedUsageFields(String amortization, String recurring, String publicOnDemandCost) {
			this.amortization = amortization;
			this.recurring = recurring;
			this.publicOnDemandCost = publicOnDemandCost;
		}
		
		// For RIFee testing
		public void setRIFeeFields(String amortizedUpfrontFeeForBillingPeriod, String unusedQuantity, String unusedAmortizedUpfrontFeeForBillingPeriod, String unusedRecurringFee,
				String numberOfReservations, String reservationStartTime, String reservationEndTime) {
			this.amortizedUpfrontFeeForBillingPeriod = amortizedUpfrontFeeForBillingPeriod;
			this.unusedQuantity = unusedQuantity;
			this.unusedAmortizedUpfrontFeeForBillingPeriod = unusedAmortizedUpfrontFeeForBillingPeriod;
			this.unusedRecurringFee = unusedRecurringFee;
			this.numberOfReservations = numberOfReservations;
			this.reservationStartTime = reservationStartTime;
			this.reservationEndTime = reservationEndTime;
			if (this.purchaseOption != null && !this.purchaseOption.isEmpty()) {
				logger.error("Don't set purchase option in RIFee records - AWS has these blank");
				this.purchaseOption = "";
			}
		}
		public void setNormalizationFactor(String normalizationFactor) {
			this.normalizationFactor = normalizationFactor;
		}
		
		String[] getDbrLine() {
			String[] items = new String[Line.numDbrItems];
			for (int i = 0; i < items.length; i++)
				items[i] = "";
			items[1] = account; // payer account
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
			items[lineItem.getPayerAccountIdIndex()] = account;
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
			items[lineItem.getLineItemNormalizationFactorIndex()] = normalizationFactor;
			items[lineItem.getLineItemTypeIndex()] = lineItemType.name();
			items[lineItem.getCostIndex()] = cost;
			items[lineItem.getPurchaseOptionIndex()] = purchaseOption;
			items[lineItem.getReservationArnIndex()] = reservationARN;
			items[lineItem.getResourceIndex()] = resource;
			if (lineItem.getResourceTagStartIndex() + 2 < items.length) {
				items[lineItem.getResourceTagStartIndex() + 1] = environment;
				items[lineItem.getResourceTagStartIndex() + 2] = email;
			}
			
			switch (lineItemType) {
			case RIFee:
				// RIFee is used to get recurring and amortization fees for unused reservations
				set(lineItem.getAmortizedUpfrontFeeForBillingPeriodIndex(), items, amortizedUpfrontFeeForBillingPeriod);
				int unusedIndex = lineItem.getUnusedQuantityIndex();
				if (unusedIndex >= 0) {
					items[unusedIndex] = unusedQuantity;
					items[lineItem.getUnusedRecurringFeeIndex()] = unusedRecurringFee;
					items[lineItem.getUnusedAmortizedUpfrontFeeForBillingPeriodIndex()] = unusedAmortizedUpfrontFeeForBillingPeriod;
				}
				if (lineItem.getReservationNumberOfReservationsIndex() >= 0)
					items[lineItem.getReservationNumberOfReservationsIndex()] = numberOfReservations;
				if (lineItem.getReservationStartTimeIndex() >= 0) {
					items[lineItem.getReservationStartTimeIndex()] = reservationStartTime;
					items[lineItem.getReservationEndTimeIndex()] = reservationEndTime;
				}
				break;
			
			case DiscountedUsage:
				set(lineItem.getAmortizedUpfrontCostForUsageIndex(), items, amortization);
				set(lineItem.getRecurringFeeForUsageIndex(), items, recurring);
				int publicOnDemandCostIndex = lineItem.getPublicOnDemandCostIndex();
				if (publicOnDemandCostIndex >= 0)
					items[publicOnDemandCostIndex] = this.publicOnDemandCost;
				//items[lineItem.getCostIndex()] = "0"; // Discounted usage doesn't carry cost
				break;
				
			default:
				break;
			}
			
			return items;
		}
		
		private void set(int index, String[] items, String value) {
			if (index >= 0)
				items[index] = value;
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
		private Double unusedCost = null;
				
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
		public ProcessTest(Which which, Line line, String[] expectedTag, Double usage, Result result, int daysInMonth, Double amortization, boolean delayed, int numExpectedUsageTags, int numExpectedCostTags, Double cost, Double unusedCost) {
			this.which = which;
			this.line = line;
			this.expectedTag = expectedTag;
			this.usage = usage;
			this.cost = cost; // used if before Jan 1, 2018
			this.result = result;
			this.daysInMonth = daysInMonth;
			this.amortization = amortization;
			this.delayed = delayed;
			this.numExpectedUsageTags = numExpectedUsageTags;
			this.numExpectedCostTags = numExpectedCostTags;
			this.unusedCost = unusedCost; // used if after Jan 1, 2018
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
			
			DateTime dt = new DateTime(line.start, DateTimeZone.UTC);

			if (this.amortization != null && dt.getYear() >= 2018 && this.amortization > 0.0)
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
			run("2017-06-01T00:00:00Z", null);
		}
		
		public void run(String start, String netUnblendedStart) throws Exception {        
			DateTime dt = new DateTime(start, DateTimeZone.UTC);
			long startMilli = dt.withDayOfMonth(1).getMillis();
			
			if (which == Which.dbr || which == Which.both) {
		        LineItem dbrLineItem = new DetailedBillingReportLineItem(false, true, dbrHeader);
		        dbrLineItem.setItems(line.getDbrLine());
				runProcessTest(dbrLineItem, "Detailed Billing", false, startMilli);
			}
			
			if (which == Which.cau || which == Which.both) {
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

				CostAndUsageReportLineItem lineItem = newCurLineItem(manifest, netUnblendedStart == null ? null : new DateTime(netUnblendedStart, DateTimeZone.UTC));
				lineItem.setItems(line.getCauLine(lineItem));
				runProcessTest(lineItem, "Cost and Usage", true, startMilli);
			}
		}
		
		public void runProcessTest(LineItem lineItem, String reportName, boolean isCostAndUsageReport, long startMilli) throws Exception {
			Instances instances = null;
			CostAndUsageData costAndUsageData = new CostAndUsageData(null, TagCoverage.none);
			
			BasicLineItemProcessor lineItemProc = newBasicLineItemProcessor(lineItem, reservation);
			
			if (delayed) {
				// Make sure we have one hour of cost and usage data so monthly fees get tallied properly
				costAndUsageData.getCost(null).getData(0);
				costAndUsageData.getUsage(null).getData(0);
			}
	        
			Result result = lineItemProc.process(startMilli, delayed, isCostAndUsageReport, lineItem, costAndUsageData, instances, 0.0);
			assertEquals(reportName + " Incorrect result", this.result, result);
			
			if (result == Result.delay) {
				// Expand the data by number of hours in month
				costAndUsageData.getUsage(null).getData(daysInMonth * 24 - 1);
				costAndUsageData.getCost(null).getData(daysInMonth * 24 - 1);
				result = lineItemProc.process(startMilli, true, isCostAndUsageReport, lineItem, costAndUsageData, instances, 0.0);
			}
			
			// Check usage data
			logger.info("Test:");
			int gotLen = costAndUsageData.getUsage(null).getTagGroups().size();
			for (TagGroup tg: costAndUsageData.getUsage(null).getTagGroups())
				logger.info(" - usage: " + costAndUsageData.getUsage(null).getData(0).get(tg) + ", " + tg);
			assertEquals(reportName + " Incorrect number of usage tags", numExpectedUsageTags, gotLen);
			if (gotLen > 0) {
				TagGroup got = (TagGroup) costAndUsageData.getUsage(null).getTagGroups().toArray()[0];
				//logger.info("Got Tag: " + got);
				String errors = checkTag(got, expectedTag);
				assertTrue(reportName + " Tag is not correct: " + errors, errors.isEmpty());
				double usage = costAndUsageData.getUsage(null).getData(0).get(got);
				assertEquals(reportName + " Usage is incorrect", usage, usage, 0.001);
			}
			// Check cost data
			gotLen = costAndUsageData.getCost(null).getTagGroups().size();
			for (TagGroup tg: costAndUsageData.getCost(null).getTagGroups())
				logger.info(" - cost: " + costAndUsageData.getCost(null).getData(0).get(tg) + ", " + tg);
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
			
			// Check reservations if any
			if (lineItem.getLineItemType() == LineItemType.RIFee && lineItem.getStartMillis() >= CostAndUsageReportLineItemProcessor.jan1_2018) {
				// We should have a Reservation record for the RIFee line item
				assertEquals("Wrong number of reservations", 1, costAndUsageData.getReservations().size());
				Reservation r = costAndUsageData.getReservations().values().iterator().next();
				String errors = checkTag(r.tagGroup, expectedTag);
				assertTrue(reportName + " Tag is not correct: " + errors, errors.isEmpty());
				assertEquals("wrong reservation amortization", amortization, r.hourlyFixedPrice * r.count, 0.001);
				assertEquals("wrong reservation recurring fee", cost, r.usagePrice * r.count, 0.001);
			}
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
				else if (tg.operation.isUnused() && unusedCost != null) {
					String errors = checkTag(tg, expectedTag);
					assertTrue(reportName + " Tag is not correct: " + errors, errors.length() == 0);					
					double cost = costData.getData(0).get(tg);
					assertEquals(reportName + " Cost is incorrect", unusedCost, cost, 0.001);				
				}
				else {
					String errors = checkTag(tg, expectedTag);
					assertTrue(reportName + " Tag is not correct: " + errors, errors.length() == 0);					
					double cost = costData.getData(0).get(tg);
					assertEquals(reportName + " Cost is incorrect", this.cost, cost, 0.001);				
				}
				if (product != null && product.isEc2Instance() && isCostAndUsageReport) {
					assertTrue("Tag group is wrong type", tg instanceof TagGroupRI);
				}
			}
		}
		
		private String checkTag(TagGroup tagGroup, String[] tags) {
			StringBuilder errors = new StringBuilder();
			if (!tagMatches(tagGroup.account, "234567890123"))
				errors.append("Account mismatch: " + tagGroup.account + "/" + "234567890123" + ", ");
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
		Line line = new Line(LineItemType.DiscountedUsage, "ap-southeast-2", "ap-southeast-2a", ec2, "APS2-BoxUsage:c4.2xlarge", "RunInstances:0002", "USD 0.0 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1", "0", "All Upfront");
		line.setDiscountedUsageFields("1.5", "0.0", "");
		String[] tag = new String[] { "ap-southeast-2", "ap-southeast-2a", "EC2 Instance", "Bonus RIs - All Upfront", "c4.2xlarge.windows", null };
		ProcessTest test = new ProcessTest(Which.dbr, line, tag, 1.0, 0.0, Result.hourly, 30);
		test.run();
		test = new ProcessTest(Which.cau, line, tag, 1.0, 0.0, Result.hourly, 30);
		test.run();
		
		// Test 2017 manifest which doesn't support amortization. Savings will come back as full price
		line = new Line(LineItemType.DiscountedUsage, "ap-southeast-2", "ap-southeast-2a", ec2, "APS2-BoxUsage:c4.2xlarge", "RunInstances:0002", "USD 0.0 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1", "0", "All Upfront");
		line.setDiscountedUsageFields("1.5", "0.0", "3.0");
		test = new ProcessTest(Which.cau, line, tag, 1.0, 0.0, Result.hourly, 30);
		test.run();
		
		// Test 2018 which does support amortization
		line = new Line(LineItemType.DiscountedUsage, "ap-southeast-2", "ap-southeast-2a", ec2, "APS2-BoxUsage:c4.2xlarge", "RunInstances:0002", "USD 0.0 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "2018-06-01T00:00:00Z", "2018-06-01T01:00:00Z", "1", "0", "All Upfront");
		line.setDiscountedUsageFields("1.5", "0.0", "3.0");
		test = new ProcessTest(Which.cau, line, tag, 1.0, 0.0, Result.hourly, 30, 1.5, 1.5);
		test.run("2018-06-01T00:00:00Z", null);
	}
	
	@Test
	public void testReservedPartialUpfrontUsage() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "ap-southeast-2", "ap-southeast-2a", ec2, "APS2-HeavyUsage:c4.2xlarge", "RunInstances:0002", "USD 0.34 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1", "0.34", "Partial Upfront");
		line.setDiscountedUsageFields("1.0", "1.0", "3.0");
		String[] tag = new String[] { "ap-southeast-2", "ap-southeast-2a", "EC2 Instance", "Bonus RIs - Partial Upfront", "c4.2xlarge.windows", null };
		ProcessTest test = new ProcessTest(Which.dbr, line, tag, 1.0, 0.34, Result.hourly, 30);
		test.run();
	}
	
	@Test
	public void testReservedNoUpfrontUsage() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "ap-southeast-2", "ap-southeast-2a", ec2, "APS2-HeavyUsage:c4.2xlarge", "RunInstances:0002", "USD 0.45 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "2019-01-01T00:00:00Z", "2019-01-01T01:00:00Z", "1", "0.45", "No Upfront");
		line.setDiscountedUsageFields("0", "0.45", "0.60");
		String[] tag = new String[] { "ap-southeast-2", "ap-southeast-2a", "EC2 Instance", "Bonus RIs - No Upfront", "c4.2xlarge.windows", null };
		ProcessTest test = new ProcessTest(Which.cau, line, tag, 1.0, 0.45, Result.hourly, 31, null, 0.15);
		test.run("2019-01-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
		// Test with resource tags
		line.setResources("i-0184b2c6d0325157b", "Prod", "john.doe@foobar.com");
		String[] resourceTag = new String[] { "ap-southeast-2", "ap-southeast-2a", "EC2 Instance", "Bonus RIs - No Upfront", "c4.2xlarge.windows", "Prod" + ResourceGroup.separator + "john.doe@foobar.com" };
		test = new ProcessTest(Which.cau, line, tag, 1.0, 0.45, Result.hourly, 31, null, 0.15);
		test.setResources(resourceTag, productService.getProductByName(Product.ec2Instance), 0.0, 2);
		test.run("2019-01-01T00:00:00Z", "2019-01-01T00:00:00Z");
	}
	
	@Test
	public void testReservedPartialUpfrontUsageFamily() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "ap-southeast-2", "ap-southeast-2a", ec2, "APS2-HeavyUsage:c4.2xlarge", "RunInstances:0002", "USD 0.34 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "0.25", "0.085", "Partial Upfront");
		String[] tag = new String[] { "ap-southeast-2", "ap-southeast-2a", "EC2 Instance", "Bonus RIs - Partial Upfront", "c4.2xlarge.windows", null };
		ProcessTest test = new ProcessTest(Which.dbr, line, tag, 0.25, 0.085, Result.hourly, 30);
		test.run();
	}
	
	@Test
	public void testReservedPartialUpfrontUsageFamilyRDS() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "ap-southeast-2", "ap-southeast-2a", rds, "APS2-InstanceUsage:db.t2.micro", "CreateDBInstance:0002", "MySQL, db.t2.micro reserved instance applied", PricingTerm.reserved, "2019-01-01T00:00:00Z", "2019-01-01T01:00:00Z", "0.5", "0.0", "Partial Upfront");
		line.setDiscountedUsageFields("0.00739", "0.00902", "0.026");
		String[] tag = new String[] { "ap-southeast-2", "ap-southeast-2a", "RDS Instance", "Bonus RIs - Partial Upfront", "db.t2.micro.mysql", null };
		ProcessTest test = new ProcessTest(Which.cau, line, tag, 0.25, 0.00902, Result.hourly, 31, 0.00739, 0.00959);
		test.run("2019-01-01T00:00:00Z", "2019-01-01T00:00:00Z");
	}
	
	@Test
	public void testReservedPartialUpfrontDiscountedUsageWithResourceTags() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "ap-southeast-2", "ap-southeast-2a", ec2, "APS2-BoxUsage:c4.2xlarge", "RunInstances:0002", "USD 0.34 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1", "0.34", "Partial Upfront");
		line.setResources("i-0184b2c6d0325157b", "Prod", "john.doe@foobar.com");
		String[] tag = new String[] { "ap-southeast-2", "ap-southeast-2a", "EC2 Instance", "Bonus RIs - Partial Upfront", "c4.2xlarge.windows", null };
		String[] resourceTag = new String[] { "ap-southeast-2", "ap-southeast-2a", "EC2 Instance", "Bonus RIs - Partial Upfront", "c4.2xlarge.windows", "Prod" + ResourceGroup.separator + "john.doe@foobar.com" };
		ProcessTest test = new ProcessTest(Which.cau, line, tag, 1.0, 0.34, Result.hourly, 30);
		test.setResources(resourceTag, productService.getProductByName(Product.ec2Instance), 0.0, 1);
		test.run();
	}
	
	@Test
	public void testReservedPartialUpfrontDiscountedUsageFamily() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "ap-southeast-2", "ap-southeast-2a", ec2, "APS2-BoxUsage:c4.large", "RunInstances:0002", "Linux/UNIX (Amazon VPC), c4.2xlarge reserved instance applied", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1", "0.34", "Partial Upfront");
		String[] tag = new String[] { "ap-southeast-2", "ap-southeast-2a", "EC2 Instance", "Bonus RIs - Partial Upfront", "c4.large.windows", null };
		ProcessTest test = new ProcessTest(Which.cau, line, tag, 1.0, 0.34, Result.hourly, 30);
		test.run();
		
		line = new Line(LineItemType.DiscountedUsage, "ap-southeast-2", "ap-southeast-2a", ec2, "APS2-BoxUsage:c4.large", "RunInstances:0002", "Linux/UNIX (Amazon VPC), c4.2xlarge reserved instance applied", PricingTerm.reserved, "2018-01-01T00:00:00Z", "2018-01-01T01:00:00Z", "1", "0.34", "Partial Upfront");
		line.setDiscountedUsageFields("0.32", "0.36", "1.02");
		tag = new String[] { "ap-southeast-2", "ap-southeast-2a", "EC2 Instance", "Bonus RIs - Partial Upfront", "c4.large.windows", null };
		test = new ProcessTest(Which.cau, line, tag, 1.0, 0.36, Result.hourly, 31, 0.32, 0.34);
		test.run("2018-01-01T00:00:00Z", "2019-01-01T00:00:00Z");

		line = new Line(LineItemType.DiscountedUsage, "ap-southeast-2", "ap-southeast-2a", ec2, "APS2-BoxUsage:c4.large", "RunInstances:0002", "Linux/UNIX (Amazon VPC), c4.2xlarge reserved instance applied", PricingTerm.reserved, "2019-01-01T00:00:00Z", "2019-01-01T01:00:00Z", "1", "0.34", "Partial Upfront");
		line.setDiscountedUsageFields("0.32", "0.36", "1.02");
		tag = new String[] { "ap-southeast-2", "ap-southeast-2a", "EC2 Instance", "Bonus RIs - Partial Upfront", "c4.large.windows", null };
		test = new ProcessTest(Which.cau, line, tag, 1.0, 0.36, Result.hourly, 31, 0.32, 0.34);
		test.run("2019-01-01T00:00:00Z", "2019-01-01T00:00:00Z");

	}
	
	@Test
	public void testRIFeeNoUpfront() throws Exception {
		// Test Cost and Usage Prior to Jan 1, 2018 (Uses Reservations pulled by Capacity Poller, so we inject one)
		Line line = new Line(LineItemType.RIFee, "eu-west-1", "", ec2, "EU-HeavyUsage:t2.small", "RunInstances", "USD 0.0146 hourly fee per Linux/UNIX (Amazon VPC), t2.small instance", PricingTerm.none, "2017-11-01T00:00:00Z", "2017-12-01T00:00:00Z", "18600", "271.56", "");
		line.setRIFeeFields("", "", "", "", "25", "", "");
		String[] tag = new String[] { "eu-west-1", null, "EC2 Instance", "Bonus RIs - No Upfront", "t2.small", null };
		ProcessTest test = new ProcessTest(Which.cau, line, tag, null, Result.monthly, 31, 0.0, true, 0, 1, 0.377, null);
		TagGroup tg = TagGroup.getTagGroup("234567890123", tag[0], tag[1], tag[2], tag[3], tag[4], "hours", null, accountService, productService);
		Reservation r = new Reservation("arn", tg, 25, 0, 0, ReservationUtilization.NO, 0.0, 0.028);
		test.addReservation(r);
		test.run("2017-11-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
		// Test Cost and Usage After Jan 1, 2018 (Uses RIFee data in CUR)
		// 25 RIs at 0.0146/hr recurring
		line = new Line(LineItemType.RIFee, "eu-west-1", "", ec2, "EU-HeavyUsage:t2.small", "RunInstances", "USD 0.0146 hourly fee per Linux/UNIX (Amazon VPC), t2.small instance", PricingTerm.none, "2019-01-01T00:00:00Z", "2019-02-01T00:00:00Z", "18600", "271.56", "");
		line.setRIFeeFields("0", "0", "0", "0", "25", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		tag = new String[] { "eu-west-1", null, "EC2 Instance", "Used RIs - No Upfront", "t2.small", null };
		test = new ProcessTest(Which.cau, line, tag, null, Result.ignore, 31, 0.0, true, 0, 0, 0.365, null);
		test.run("2019-01-01T00:00:00Z", "2019-01-01T00:00:00Z");		
	}
	
	@Test
	public void testRIFeePartialUpfront() throws Exception {
		// Test case before we had support for Amortization
		// 1 RI with 1.0/hr recurring and 2.0/hr upfront
		Line line = new Line(LineItemType.RIFee, "ap-southeast-2", "", ec2, "APS2-HeavyUsage:c4.2xlarge", "RunInstances:0002", "USD 0.34 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.none, "2017-06-01T00:00:00Z", "2017-06-30T23:59:59Z", "720", "720.0", "");
		line.setRIFeeFields("1440", "", "", "", "1", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		String[] tag = new String[] { "ap-southeast-2", null, "EC2 Instance", "Bonus RIs - Partial Upfront", "c4.2xlarge.windows", null };
		ProcessTest test = new ProcessTest(Which.cau, line, tag, null, Result.monthly, 30, 2.0, true, 0, 1, 1.0, null);
		test.run();

		// Test reservation/unused rates
		// 1 RI with 1.0/hr recurring and 2.0/hr upfront
		line = new Line(LineItemType.RIFee, "ap-southeast-2", "", ec2, "APS2-HeavyUsage:c4.2xlarge", "RunInstances:0002", "USD 0.34 hourly fee per Windows (Amazon VPC), c4.2xlarge instance", PricingTerm.none, "2019-06-01T00:00:00Z", "2019-06-30T23:59:59Z", "720", "720.0", "");
		line.setRIFeeFields("1440", "0", "0", "0", "1", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		tag = new String[] { "ap-southeast-2", null, "EC2 Instance", "Used RIs - Partial Upfront", "c4.2xlarge.windows", null };
		test = new ProcessTest(Which.cau, line, tag, null, Result.ignore, 31, 2.0, true, 0, 0, 1.0, null);
		test.run("2019-06-01T00:00:00Z", "2019-02-01T00:00:00Z");
	}
	
	@Test
	public void testReservedMonthlyFeeRDS() throws Exception {
		// Partial Upfront - 2 RIs at 1.0/hr recurring and 0.5/hr upfront
		Line line = new Line(LineItemType.RIFee, "ap-southeast-2", "", rds, "APS2-HeavyUsage:db.t2.micro", "CreateDBInstance:0014", "USD 0.012 hourly fee per PostgreSQL, db.t2.micro instance", null, "2019-06-01T00:00:00Z", "2019-06-30T23:59:59Z", "1440", "1440.0", "");
		line.setRIFeeFields("720.0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		line.setNormalizationFactor("0.5");
		String[] tag = new String[] { "ap-southeast-2", null, "RDS Instance", "Used RIs - Partial Upfront", "db.t2.micro.postgres", null };
		ProcessTest test = new ProcessTest(Which.cau, line, tag, null, Result.ignore, 30, 1.0, true, 0, 0, 2.0, null);
		test.run("2019-07-01T00:00:00Z", "2019-01-01T00:00:00Z");

		// All Upfront - 2 RIs at 2.0/hr upfront
		line = new Line(LineItemType.RIFee, "ap-southeast-2", "", rds, "APS2-HeavyUsage:db.t2.micro", "CreateDBInstance:0014", "USD 0.012 hourly fee per PostgreSQL, db.t2.micro instance", null, "2019-06-01T00:00:00Z", "2019-06-30T23:59:59Z", "1440", "0", "");
		line.setRIFeeFields("2880.0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		line.setNormalizationFactor("0.5");
		tag = new String[] { "ap-southeast-2", null, "RDS Instance", "Used RIs - All Upfront", "db.t2.micro.postgres", null };
		test = new ProcessTest(Which.cau, line, tag, null, Result.ignore, 30, 4.0, true, 0, 0, 0.0, null);
		test.run("2019-07-01T00:00:00Z", "2019-01-01T00:00:00Z");

		// No Upfront - 2 RIs at 1.5/hr recurring
		line = new Line(LineItemType.RIFee, "ap-southeast-2", "", rds, "APS2-HeavyUsage:db.t2.micro", "CreateDBInstance:0014", "USD 0.012 hourly fee per PostgreSQL, db.t2.micro instance", null, "2019-06-01T00:00:00Z", "2019-06-30T23:59:59Z", "1440", "2160", "");
		line.setRIFeeFields("0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		line.setNormalizationFactor("0.5");
		tag = new String[] { "ap-southeast-2", null, "RDS Instance", "Used RIs - No Upfront", "db.t2.micro.postgres", null };
		test = new ProcessTest(Which.cau, line, tag, null, Result.ignore, 30, 0.0, true, 0, 0, 3.0, null);
		test.run("2019-07-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
		// Partial Upfront Multi-AZ 1 RI at 2.0/hr recurring and 1.0/hr upfront
		line = new Line(LineItemType.RIFee, "ap-southeast-2", "", rds, "APS2-HeavyUsage:db.t2.micro", "CreateDBInstance:0014", "USD 0.012 hourly fee per PostgreSQL, db.t2.micro instance", null, "2019-06-01T00:00:00Z", "2019-06-30T23:59:59Z", "720", "1440.0", "");
		line.setRIFeeFields("720.0", "0", "0", "0", "1", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		line.setNormalizationFactor("1");
		tag = new String[] { "ap-southeast-2", null, "RDS Instance", "Used RIs - Partial Upfront", "db.t2.micro.multiaz.postgres", null };
		test = new ProcessTest(Which.cau, line, tag, null, Result.ignore, 30, 1.0, true, 0, 0, 2.0, null);
		test.run("2019-07-01T00:00:00Z", "2019-01-01T00:00:00Z");
	}
	
	@Test
	public void testReservedPartialUpfrontHourlyUsageRDS() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "ap-southeast-2", "", rds, "APS2-InstanceUsage:db.t2.micro", "CreateDBInstance:0014", "PostgreSQL, db.t2.micro reserved instance applied", PricingTerm.reserved, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1", "0", "Partial Upfront");
		String[] tag = new String[] { "ap-southeast-2", null, "RDS Instance", "Bonus RIs - Partial Upfront", "db.t2.micro.postgres", null };
		ProcessTest test = new ProcessTest(Which.cau, line, tag, 1.0, 0.0, Result.hourly, 30);
		test.run();
	}
	
	@Test
	public void testRIPurchase() throws Exception {
		Line line = new Line(LineItemType.Fee, "ap-southeast-2", "", ec2, "", "", "Sign up charge for subscription: 647735683, planId: 2195643", PricingTerm.reserved, "2017-06-09T21:21:37Z", "2018-06-09T21:21:36Z", "150.0", "9832.500000", "");
		ProcessTest test = new ProcessTest(Which.both, line, null, 0.0, 0.0, Result.ignore, 30);
		test.run();
	}
	
	@Test
	public void testSpot() throws Exception {
		Line line = new Line(LineItemType.Usage, "ap-northeast-2", "", ec2, "APN2-SpotUsage:c4.xlarge", "RunInstances:SV052", "c4.xlarge Linux/UNIX Spot Instance-hour in Asia Pacific (Seoul) in VPC Zone #52", PricingTerm.spot, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1.00000000", "0.3490000000000", "");
		String[] tag = new String[] { "ap-northeast-2", null, "EC2 Instance", "Spot Instances", "c4.xlarge", null };
		ProcessTest test = new ProcessTest(Which.both, line, tag, 1.0, 0.349, Result.hourly, 30);
		test.run();
	}
	@Test
	public void testSpotWithResourceTags() throws Exception {
		Line line = new Line(LineItemType.Usage, "ap-northeast-2", "", ec2, "APN2-SpotUsage:c4.xlarge", "RunInstances:SV052", "c4.xlarge Linux/UNIX Spot Instance-hour in Asia Pacific (Seoul) in VPC Zone #52", PricingTerm.spot, "2017-06-01T00:00:00Z", "2017-06-01T01:00:00Z", "1.00000000", "0.3490000000000", "");
		line.setResources("i-0184b2c6d0325157b", "Prod", "john.doe@foobar.com");
		String[] tag = new String[] { "ap-northeast-2", null, "EC2 Instance", "Spot Instances", "c4.xlarge", null };
		String[] resourceTag = new String[] { "ap-northeast-2", null, "EC2 Instance", "Spot Instances", "c4.xlarge", "Prod" + ResourceGroup.separator + "john.doe@foobar.com" };
		ProcessTest test = new ProcessTest(Which.cau, line, tag, 1.0, 0.349, Result.hourly, 30);
		test.setResources(resourceTag, productService.getProductByName(Product.ec2Instance), 0.0, 1);
		test.run();
	}
	
	@Test
	public void testReservedMonthlyFeeElasticsearch() throws Exception {
		// Partial Upfront - 2 RIs at 1.0/hr recurring and 0.5/hr upfront
		Line line = new Line(LineItemType.RIFee, "us-east-1", "", es, "HeavyUsage:r4.xlarge.elasticsearch", "ESDomain", "USD 0.0 hourly fee per Elasticsearch, r4.xlarge.elasticsearch instance", null, "2019-07-01T00:00:00Z", "2019-07-31T23:59:59Z", "1488", "1488.0", "");
		line.setRIFeeFields("744.0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		String[] tag = new String[] { "us-east-1", null, "Elasticsearch Service", "Used RIs - Partial Upfront", "r4.xlarge.elasticsearch", null };
		ProcessTest test = new ProcessTest(Which.cau, line, tag, null, Result.ignore, 31, 1.0, true, 0, 0, 2.0, null);
		test.run("2019-07-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
		// All Upfront - 2 RIs at 2.0/hr upfront
		line = new Line(LineItemType.RIFee, "us-east-1", "", es, "HeavyUsage:r4.xlarge.elasticsearch", "ESDomain", "USD 0.0 hourly fee per Elasticsearch, r4.xlarge.elasticsearch instance", null, "2019-07-01T00:00:00Z", "2019-07-31T23:59:59Z", "1488", "0", "");
		line.setRIFeeFields("2976.0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		tag = new String[] { "us-east-1", null, "Elasticsearch Service", "Used RIs - All Upfront", "r4.xlarge.elasticsearch", null };
		test = new ProcessTest(Which.cau, line, tag, null, Result.ignore, 31, 4.0, true, 0, 0, 0.0, null);
		test.run("2019-07-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
		// No Upfront - 2 RIs at 1.5/hr recurring
		line = new Line(LineItemType.RIFee, "us-east-1", "", es, "HeavyUsage:r4.xlarge.elasticsearch", "ESDomain", "USD 0.0 hourly fee per Elasticsearch, r4.xlarge.elasticsearch instance", null, "2019-07-01T00:00:00Z", "2019-07-31T23:59:59Z", "1488", "2232.0", "");
		line.setRIFeeFields("0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		tag = new String[] { "us-east-1", null, "Elasticsearch Service", "Used RIs - No Upfront", "r4.xlarge.elasticsearch", null };
		test = new ProcessTest(Which.cau, line, tag, null, Result.ignore, 31, 0.0, true, 0, 0, 3.0, null);
		test.run("2019-07-01T00:00:00Z", "2019-01-01T00:00:00Z");
	}
	
	@Test
	public void testReservedElasticsearch() throws Exception {
		// Partial Upfront
		Line line = new Line(LineItemType.DiscountedUsage, "us-east-1", "", es, "ESInstance:r4.xlarge", "ESDomain", "Elasticsearch, r4.xlarge.elasticsearch reserved instance applied", PricingTerm.reserved, "2018-01-01T00:00:00Z", "2018-01-01T01:00:00Z", "1", "0", "Partial Upfront");
		line.setDiscountedUsageFields("0.25", "0.32", "0.66");
		String[] tag = new String[] { "us-east-1", null, "Elasticsearch Service", "Bonus RIs - Partial Upfront", "r4.xlarge.elasticsearch", null };
		ProcessTest test = new ProcessTest(Which.cau, line, tag, 1.0, 0.32, Result.hourly, 31, 0.25, 0.09);
		test.run("2018-01-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
		// All Upfront
		line = new Line(LineItemType.DiscountedUsage, "us-east-1", "", es, "ESInstance:r4.xlarge", "ESDomain", "Elasticsearch, r4.xlarge.elasticsearch reserved instance applied", PricingTerm.reserved, "2018-01-01T00:00:00Z", "2018-01-01T01:00:00Z", "1", "0", "All Upfront");
		line.setDiscountedUsageFields("0.30", "0", "0.66");
		tag = new String[] { "us-east-1", null, "Elasticsearch Service", "Bonus RIs - All Upfront", "r4.xlarge.elasticsearch", null };
		test = new ProcessTest(Which.cau, line, tag, 1.0, 0.0, Result.hourly, 31, 0.30, 0.36);
		test.run("2018-01-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
		// No Upfront
		line = new Line(LineItemType.DiscountedUsage, "us-east-1", "", es, "ESInstance:r4.xlarge", "ESDomain", "Elasticsearch, r4.xlarge.elasticsearch reserved instance applied", PricingTerm.reserved, "2018-01-01T00:00:00Z", "2018-01-01T01:00:00Z", "1", "0", "No Upfront");
		line.setDiscountedUsageFields("0", "0.34", "0.66");
		tag = new String[] { "us-east-1", null, "Elasticsearch Service", "Bonus RIs - No Upfront", "r4.xlarge.elasticsearch", null };
		test = new ProcessTest(Which.cau, line, tag, 1.0, 0.34, Result.hourly, 31, 0.0, 0.32);
		test.run("2018-01-01T00:00:00Z", "2019-01-01T00:00:00Z");
	}
	
	@Test
	public void testReservedDynamoDB() throws Exception {
		Line line = new Line(LineItemType.DiscountedUsage, "us-east-1", "", "Amazon DynamoDB", "WriteCapacityUnit-Hrs", "CommittedThroughput", "DynamoDB, Reserved Write Capacity used this month", PricingTerm.reserved, "2018-01-01T00:00:00Z", "2018-01-01T01:00:00Z", "1", "0", "Heavy Utilization");
		line.setDiscountedUsageFields("0.00028082", "0.00020992", "0.0013");
		String[] tag = new String[] { "us-east-1", null, "DynamoDB", "CommittedThroughput", "WriteCapacityUnit-Hrs", null };
		// TODO: support DynamoDB amortization
		//test = new ProcessTest(Which.cau, line, tag, 1.0, 0.0, Result.hourly, 31, 0.32, 0.34);
		ProcessTest test = new ProcessTest(Which.cau, line, tag, 1.0, 0.0, Result.hourly, 31);
		test.run("2018-01-01T00:00:00Z", "2019-01-01T00:00:00Z");
	}
		
	@Test
	public void testReservedMonthlyFeeElastiCache() throws Exception {
		// Partial Upfront - 2 RIs at 1.0/hr recurring and 0.5/hr upfront
		Line line = new Line(LineItemType.RIFee, "us-east-1", "", ec, "HeavyUsage:cache.m5.medium", "CreateCacheCluster:0002", "USD 0.03 hourly fee per Redis, cache.m3.medium instance", null, "2019-07-01T00:00:00Z", "2019-07-31T23:59:59Z", "1488", "1488.0", "");
		line.setRIFeeFields("744.0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		String[] tag = new String[] { "us-east-1", null, "ElastiCache", "Used RIs - Partial Upfront", "cache.m5.medium.redis", null };
		ProcessTest test = new ProcessTest(Which.cau, line, tag, null, Result.ignore, 31, 1.0, true, 0, 0, 2.0, null);
		test.run("2019-07-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
		// All Upfront - 2 RIs at 2.0/hr upfront
		line = new Line(LineItemType.RIFee, "us-east-1", "", ec, "HeavyUsage:cache.m5.medium", "CreateCacheCluster:0002", "USD 0.03 hourly fee per Redis, cache.m3.medium instance", null, "2019-07-01T00:00:00Z", "2019-07-31T23:59:59Z", "1488", "0", "");
		line.setRIFeeFields("2976.0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		tag = new String[] { "us-east-1", null, "ElastiCache", "Used RIs - All Upfront", "cache.m5.medium.redis", null };
		test = new ProcessTest(Which.cau, line, tag, null, Result.ignore, 31, 4.0, true, 0, 0, 0.0, null);
		test.run("2019-07-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
		// No Upfront - 2 RIs at 1.5/hr recurring
		line = new Line(LineItemType.RIFee, "us-east-1", "", ec, "HeavyUsage:cache.m5.medium", "CreateCacheCluster:0002", "USD 0.03 hourly fee per Redis, cache.m3.medium instance", null, "2019-07-01T00:00:00Z", "2019-07-31T23:59:59Z", "1488", "2232.0", "");
		line.setRIFeeFields("0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		tag = new String[] { "us-east-1", null, "ElastiCache", "Used RIs - No Upfront", "cache.m5.medium.redis", null };
		test = new ProcessTest(Which.cau, line, tag, null, Result.ignore, 31, 0.0, true, 0, 0, 3.0, null);
		test.run("2019-07-01T00:00:00Z", "2019-01-01T00:00:00Z");

		// Heavy Utilization - 2 RIs at 1.0/hr recurring and 0.5/hr upfront
		line = new Line(LineItemType.RIFee, "us-east-1", "", ec, "HeavyUsage:cache.m3.medium", "CreateCacheCluster:0002", "USD 0.03 hourly fee per Redis, cache.m3.medium instance", null, "2019-07-01T00:00:00Z", "2019-07-31T23:59:59Z", "1488", "1488.0", "");
		line.setRIFeeFields("744.0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		tag = new String[] { "us-east-1", null, "ElastiCache", "Used RIs - Heavy Utilization", "cache.m3.medium.redis", null };
		test = new ProcessTest(Which.cau, line, tag, null, Result.ignore, 31, 1.0, true, 0, 0, 2.0, null);
		test.run("2019-07-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
		// Medium Utilization - 2 RIs at 1.0/hr recurring and 0.5/hr upfront
		line = new Line(LineItemType.RIFee, "us-east-1", "", ec, "MediumUsage:cache.m3.medium", "CreateCacheCluster:0002", "USD 0.03 hourly fee per Redis, cache.m3.medium instance", null, "2019-07-01T00:00:00Z", "2019-07-31T23:59:59Z", "1488", "1488.0", "");
		line.setRIFeeFields("744.0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		tag = new String[] { "us-east-1", null, "ElastiCache", "Used RIs - Medium Utilization", "cache.m3.medium.redis", null };
		test = new ProcessTest(Which.cau, line, tag, null, Result.ignore, 31, 1.0, true, 0, 0, 2.0, null);
		test.run("2019-07-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
		// Light Utilization - 2 RIs at 1.0/hr recurring and 0.5/hr upfront
		line = new Line(LineItemType.RIFee, "us-east-1", "", ec, "LightUsage:cache.m3.medium", "CreateCacheCluster:0002", "USD 0.03 hourly fee per Redis, cache.m3.medium instance", null, "2019-07-01T00:00:00Z", "2019-07-31T23:59:59Z", "1488", "1488.0", "");
		line.setRIFeeFields("744.0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		tag = new String[] { "us-east-1", null, "ElastiCache", "Used RIs - Light Utilization", "cache.m3.medium.redis", null };
		test = new ProcessTest(Which.cau, line, tag, null, Result.ignore, 31, 1.0, true, 0, 0, 2.0, null);
		test.run("2019-07-01T00:00:00Z", "2019-01-01T00:00:00Z");		

		// Heavy Utilization - 2 RIs at 1.0/hr recurring and 0.5/hr upfront --- test with different region
		line = new Line(LineItemType.RIFee, "us-west-2", "", ec, "USW2-HeavyUsage:cache.t2.medium", "CreateCacheCluster:0002", "USD 0.03 hourly fee per Redis, cache.m3.medium instance", null, "2019-07-01T00:00:00Z", "2019-07-31T23:59:59Z", "1488", "1488.0", "");
		line.setRIFeeFields("744.0", "0", "0", "0", "2", "2017-01-01T00:00:00Z", "2020-01-01T00:00:00Z");
		tag = new String[] { "us-west-2", null, "ElastiCache", "Used RIs - Heavy Utilization", "cache.t2.medium.redis", null };
		test = new ProcessTest(Which.cau, line, tag, null, Result.ignore, 31, 1.0, true, 0, 0, 2.0, null);
		test.run("2019-07-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
	}
	
	@Test
	public void testReservedElastiCache() throws Exception {
		// Partial Upfront
		Line line = new Line(LineItemType.DiscountedUsage, "us-east-1", "", ec, "NodeUsage:cache.m3.medium", "CreateCacheCluster:0002", "Redis, cache.m3.medium reserved instance applied", PricingTerm.reserved, "2018-01-01T00:00:00Z", "2018-01-01T01:00:00Z", "1", "0", "Partial Upfront");
		line.setDiscountedUsageFields("0.25", "0.32", "0.66");
		String[] tag = new String[] { "us-east-1", null, "ElastiCache", "Bonus RIs - Partial Upfront", "cache.m3.medium.redis", null };
		ProcessTest test = new ProcessTest(Which.cau, line, tag, 1.0, 0.32, Result.hourly, 31, 0.25, 0.09);
		test.run("2018-01-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
		// All Upfront
		line = new Line(LineItemType.DiscountedUsage, "us-east-1", "", ec, "NodeUsage:cache.m3.medium", "CreateCacheCluster:0002", "Redis, cache.m3.medium reserved instance applied", PricingTerm.reserved, "2018-01-01T00:00:00Z", "2018-01-01T01:00:00Z", "1", "0", "All Upfront");
		line.setDiscountedUsageFields("0.30", "0", "0.66");
		tag = new String[] { "us-east-1", null, "ElastiCache", "Bonus RIs - All Upfront", "cache.m3.medium.redis", null };
		test = new ProcessTest(Which.cau, line, tag, 1.0, 0.0, Result.hourly, 31, 0.30, 0.36);
		test.run("2018-01-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
		// No Upfront
		line = new Line(LineItemType.DiscountedUsage, "us-east-1", "", ec, "NodeUsage:cache.m3.medium", "CreateCacheCluster:0002", "Redis, cache.m3.medium reserved instance applied", PricingTerm.reserved, "2018-01-01T00:00:00Z", "2018-01-01T01:00:00Z", "1", "0", "No Upfront");
		line.setDiscountedUsageFields("0", "0.34", "0.66");
		tag = new String[] { "us-east-1", null, "ElastiCache", "Bonus RIs - No Upfront", "cache.m3.medium.redis", null };
		test = new ProcessTest(Which.cau, line, tag, 1.0, 0.34, Result.hourly, 31, 0.0, 0.32);
		test.run("2018-01-01T00:00:00Z", "2019-01-01T00:00:00Z");

		// Heavy Utilization
		line = new Line(LineItemType.DiscountedUsage, "us-east-1", "", ec, "NodeUsage:cache.m3.medium", "CreateCacheCluster:0002", "Redis, cache.m3.medium reserved instance applied", PricingTerm.reserved, "2018-01-01T00:00:00Z", "2018-01-01T01:00:00Z", "1", "0", "Heavy Utilization");
		line.setDiscountedUsageFields("0.25", "0.32", "0.66");
		tag = new String[] { "us-east-1", null, "ElastiCache", "Bonus RIs - Heavy Utilization", "cache.m3.medium.redis", null };
		test = new ProcessTest(Which.cau, line, tag, 1.0, 0.32, Result.hourly, 31, 0.25, 0.09);
		test.run("2018-01-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
		// Medium Utilization
		line = new Line(LineItemType.DiscountedUsage, "us-east-1", "", ec, "NodeUsage:cache.m3.medium", "CreateCacheCluster:0002", "Redis, cache.m3.medium reserved instance applied", PricingTerm.reserved, "2018-01-01T00:00:00Z", "2018-01-01T01:00:00Z", "1", "0", "Medium Utilization");
		line.setDiscountedUsageFields("0.25", "0.32", "0.66");
		tag = new String[] { "us-east-1", null, "ElastiCache", "Bonus RIs - Medium Utilization", "cache.m3.medium.redis", null };
		test = new ProcessTest(Which.cau, line, tag, 1.0, 0.32, Result.hourly, 31, 0.25, 0.09);
		test.run("2018-01-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
		// Light Utilization
		line = new Line(LineItemType.DiscountedUsage, "us-east-1", "", ec, "NodeUsage:cache.m3.medium", "CreateCacheCluster:0002", "Redis, cache.m3.medium reserved instance applied", PricingTerm.reserved, "2018-01-01T00:00:00Z", "2018-01-01T01:00:00Z", "1", "0", "Light Utilization");
		line.setDiscountedUsageFields("0.25", "0.32", "0.66");
		tag = new String[] { "us-east-1", null, "ElastiCache", "Bonus RIs - Light Utilization", "cache.m3.medium.redis", null };
		test = new ProcessTest(Which.cau, line, tag, 1.0, 0.32, Result.hourly, 31, 0.25, 0.09);
		test.run("2018-01-01T00:00:00Z", "2019-01-01T00:00:00Z");
		
	}
	
	
// TODO: add support for credits
//	@Test
//	public void testLambdaCredit() throws Exception {
//		String rawLineItem = "3s5n7gjxiw5rbappdpqwtq5yx2fiwznedfq5qhw2f3jdiecjlx6q,2019-01-01T00:00:00Z/2019-02-01T00:00:00Z,123456789,AWS,Anniversary,123456789012,2019-01-01T00:00:00Z,2019-02-01T00:00:00Z,234567890123,Credit,2019-01-01T00:00:00Z,2019-02-01T00:00:00Z,AWSDataTransfer,USW2-DataTransfer-Regional-Bytes,,,,0,,,USD,,-35.576771,,-35.576771,AWS Lambda Data Transfer Pricing Adjustment,,,-35.576771,Amazon Web Services. Inc.,AWS Data Transfer,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,US West (Oregon),AWS Region,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,Data Transfer,,,,,,us-west-2,,,,,,,,,AWSDataTransfer,AWS Data Transfer,62WNQUZDQQ6GRJC5,,,,,,,,,,,,US West (Oregon),AWS Region,,,IntraRegion,,USW2-DataTransfer-Regional-Bytes,,,,,,,,,,,,331970144,0.0000000000,0.0100000000,OnDemand,GB,,,,,,,,,,,,,,,,,,1387008443,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,";
//		String[] tag = new String[] { "us-west-2", null, "DataTransfer", "", "USW2-DataTransfer-Regional-Bytes", null };
//		ProcessTest test = new ProcessTest(Which.cau, null, tag, 0.0, -35.576771 / 31, Result.monthly, 31);
//		test.run("2019-01-01T00:00:00Z", "2019-02-01T00:00:00Z", rawLineItem.split(","));
//	}
}
