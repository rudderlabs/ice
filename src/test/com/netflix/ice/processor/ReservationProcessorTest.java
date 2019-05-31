package com.netflix.ice.processor;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.ivy.util.StringUtils;
import org.joda.time.DateTime;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.basic.BasicReservationService;
import com.netflix.ice.basic.BasicResourceService;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.LineItem;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.common.TagGroupRI;
import com.netflix.ice.common.Config.TagCoverage;
import com.netflix.ice.processor.ReservationService.ReservationPeriod;
import com.netflix.ice.processor.ReservationService.ReservationKey;
import com.netflix.ice.processor.ReservationService.ReservationUtilization;
import com.netflix.ice.processor.pricelist.InstancePrices;
import com.netflix.ice.processor.pricelist.PriceListService;
import com.netflix.ice.processor.pricelist.InstancePrices.ServiceCode;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.Zone;

public class ReservationProcessorTest {
    protected Logger logger = LoggerFactory.getLogger(getClass());
	private static final String resourceDir = "src/test/resources/";

	private final Product ec2Instance = productService.getProductByName(Product.ec2Instance);
	private final Product rdsInstance = productService.getProductByName(Product.rdsInstance);

    // reservationAccounts is a cross-linked list of accounts where each account
	// can borrow reservations from any other.
	private static Map<Account, Set<String>> reservationOwners = Maps.newHashMap();
	
	private static final int numAccounts = 3;
	public static List<Account> accounts = Lists.newArrayList();
	static {
		// Auto-populate the accounts list based on numAccounts
		for (Integer i = 1; i <= numAccounts; i++) {
			// Create accounts of the form Account("111111111111", "Account1")
			accounts.add(new Account(StringUtils.repeat(i.toString(), 12), "Account" + i.toString()));
		}
		// Every account is a reservation owner for these tests
		Set<String> products = Sets.newHashSet("ec2", "rds", "redshift");
		reservationOwners.put(accounts.get(0), products);
		for (int i = 1; i < numAccounts; i++) {
			reservationOwners.put(accounts.get(i), products);
		}
		accountService = new BasicAccountService(accounts, reservationOwners, null, null);
		
		// Initialize the zones we use
		Region.EU_WEST_1.addZone("eu-west-1b");
		Region.EU_WEST_1.addZone("eu-west-1c");
		Region.US_EAST_1.addZone("us-east-1a");
		Region.US_EAST_1.addZone("us-east-1b");
		Region.US_EAST_1.addZone("us-east-1c");
		Region.US_WEST_2.addZone("us-west-2a");
		Region.US_WEST_2.addZone("us-west-2b");
		Region.US_WEST_2.addZone("us-west-2c");
		Region.AP_SOUTHEAST_2.addZone("ap-southeast-2a");
		eu_west_1b = Zone.getZone("eu-west-1b");
		eu_west_1c = Zone.getZone("eu-west-1c");
		us_east_1a = Zone.getZone("us-east-1a");
		us_east_1b = Zone.getZone("us-east-1b");
		us_east_1c = Zone.getZone("us-east-1c");
		us_west_2a = Zone.getZone("us-west-2a");
		us_west_2b = Zone.getZone("us-west-2b");
		us_west_2c = Zone.getZone("us-west-2c");
		ap_southeast_2a = Zone.getZone("ap-southeast-2a");
	}
	
	private static ProductService productService;
	private static ResourceService resourceService;
	public static AccountService accountService;
	private static PriceListService priceListService;
	private static Zone eu_west_1b;
	private static Zone eu_west_1c;
	private static Zone us_east_1a;
	private static Zone us_east_1b;
	private static Zone us_east_1c;
	private static Zone us_west_2a;
	private static Zone us_west_2b;
	private static Zone us_west_2c;
	private static Zone ap_southeast_2a;

	@BeforeClass
	public static void init() throws Exception {
		priceListService = new PriceListService(resourceDir, null, null);
		priceListService.init();

		Properties props = new Properties();
		props.setProperty("RDS", "Relational Database Service");
		props.setProperty("EC2", "Elastic Compute Cloud");
		productService = new BasicProductService(props);

		Map<String, List<String>> tagKeys = Maps.newHashMap();
		Map<String, List<String>> tagValues = Maps.newHashMap();
		resourceService = new BasicResourceService(productService, new String[]{}, new String[]{}, tagKeys, tagValues, null);
	}
	
	@Test
	public void testConstructor() throws IOException {
		assertEquals("Number of accounts should be " + numAccounts, numAccounts, accounts.size());
		ReservationProcessor rp = new DetailedBillingReservationProcessor(reservationOwners.keySet(), null, null, true);
		assertNotNull("Contructor returned null", rp);
	}
	
	public class Datum {
		public TagGroup tagGroup;
		public double value;
		
		public Datum(TagGroup tagGroup, double value)
		{
			this.tagGroup = tagGroup;
			this.value = value;
		}
		
		public Datum(Account account, Region region, Zone zone, Operation operation, String usageType, double value)
		{
			this.tagGroup = TagGroup.getTagGroup(account, region, zone, ec2Instance, operation, UsageType.getUsageType(usageType, "hours"), null);
			this.value = value;
		}

		public Datum(Account account, Region region, Zone zone, Product product, Operation operation, String usageType, double value)
		{
			this.tagGroup = TagGroup.getTagGroup(account, region, zone, product, operation, UsageType.getUsageType(usageType, "hours"), null);
			this.value = value;
		}
		
		public Datum(Account account, Region region, Zone zone, Product product, Operation operation, String usageType, ResourceGroup resource, double value)
		{
			this.tagGroup = TagGroup.getTagGroup(account, region, zone, product, operation, UsageType.getUsageType(usageType, "hours"), resource);
			this.value = value;
		}
		public Datum(Account account, Region region, Zone zone, Product product, Operation operation, String usageType, ResourceGroup resource, String rsvArn, double value)
		{
			this.tagGroup = TagGroupRI.getTagGroup(account, region, zone, product, operation, UsageType.getUsageType(usageType, "hours"), resource, rsvArn);
			this.value = value;
		}
	}
	
	private Map<TagGroup, Double> makeDataMap(Datum[] data) {
		Map<TagGroup, Double> m = Maps.newHashMap();
		for (Datum d: data) {
			m.put(d.tagGroup, d.value);
		}
		return m;
	}
	
	private void runOneHourTest(long startMillis, String[] reservationsCSV, Datum[] usageData, Datum[] costData, Datum[] expectedUsage, Datum[] expectedCost, String debugFamily) throws Exception {
		runOneHourTestWithOwners(startMillis, reservationsCSV, usageData, costData, expectedUsage, expectedCost, debugFamily, reservationOwners.keySet(), null);
	}
	private void runOneHourTestWithOwners(
			long startMillis, 
			String[] reservationsCSV, 
			Datum[] usageData, 
			Datum[] costData, 
			Datum[] expectedUsage, 
			Datum[] expectedCost, 
			String debugFamily,
			Set<Account> rsvOwners,
			Product product) throws Exception {
		DetailedBillingReservationProcessor rp = new DetailedBillingReservationProcessor(rsvOwners, new BasicProductService(null), priceListService, true);
		// Populate the accounts list in the reservation processor for borrowing
		for (Account a: accounts)
			rp.addBorrower(a);
		runOneHourTestWithOwnersAndProcessor(startMillis, reservationsCSV, usageData, costData, expectedUsage, expectedCost, debugFamily, rp, product);
	}	
	private void runOneHourTestCostAndUsage(long startMillis, String[] reservationsCSV, Datum[] usageData, Datum[] costData, Datum[] expectedUsage, Datum[] expectedCost, String debugFamily) throws Exception {
		runOneHourTestCostAndUsageWithOwners(startMillis, reservationsCSV, usageData, costData, expectedUsage, expectedCost, debugFamily, reservationOwners.keySet(), null);
	}
	private void runOneHourTestCostAndUsageWithOwners(
			long startMillis, 
			String[] reservationsCSV, 
			Datum[] usageData, 
			Datum[] costData, 
			Datum[] expectedUsage, 
			Datum[] expectedCost, 
			String debugFamily,
			Set<Account> rsvOwners,
			Product product) throws Exception {
		ReservationProcessor rp = new CostAndUsageReservationProcessor(rsvOwners, new BasicProductService(null), priceListService, true);
		runOneHourTestWithOwnersAndProcessor(startMillis, reservationsCSV, usageData, costData, expectedUsage, expectedCost, debugFamily, rp, product);
	}
	
	private void runOneHourTestWithOwnersAndProcessor(
			long startMillis, 
			String[] reservationsCSV, 
			Datum[] usageData, 
			Datum[] costData, 
			Datum[] expectedUsage, 
			Datum[] expectedCost, 
			String debugFamily,
			ReservationProcessor reservationProcessor,
			Product product) throws Exception {
		
		CostAndUsageData caud = new CostAndUsageData(null, TagCoverage.none);
		if (product != null) {
			caud.putUsage(product, new ReadWriteData());
			caud.putCost(product, new ReadWriteData());
		}
		
		Map<TagGroup, Double> hourUsageData = makeDataMap(usageData);
		Map<TagGroup, Double> hourCostData = makeDataMap(costData);

		List<Map<TagGroup, Double>> ud = new ArrayList<Map<TagGroup, Double>>();
		ud.add(hourUsageData);
		caud.getUsage(product).setData(ud, 0, false);
		List<Map<TagGroup, Double>> cd = new ArrayList<Map<TagGroup, Double>>();
		cd.add(hourCostData);
		caud.getCost(product).setData(cd, 0, false);
		
		Region debugRegion = null;
		if (usageData.length > 0)
			debugRegion = usageData[0].tagGroup.region;
		else if (expectedUsage.length > 0)
			debugRegion = expectedUsage[0].tagGroup.region;

		runTest(startMillis, reservationsCSV, caud, product, debugFamily, debugRegion, reservationProcessor);

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
	
	private static String convertStartAndEnd(String res) {
		// If start and end times are in milliseconds, convert to AWS billing format
		String[] fields = res.split(",");
		try {			
			Long start = Long.parseLong(fields[9]);
			fields[9] = LineItem.amazonBillingDateFormat.print(new DateTime(start));
		}
		catch (Exception e) {
		}
		try {
			Long end = Long.parseLong(fields[10]);
			fields[10] = LineItem.amazonBillingDateFormat.print(new DateTime(end));
		}
		catch (Exception e) {
		}
		return StringUtils.join(fields, ",");
	}
	
	public static void runTest(
			long startMillis, 
			String[] reservationsCSV, 
			CostAndUsageData data, 
			Product product, 
			String debugFamily, 
			Region debugRegion, 
			ReservationProcessor rp) throws Exception {
		
		Map<ReservationKey, CanonicalReservedInstances> reservations = Maps.newHashMap();
		for (String res: reservationsCSV) {
			String[] fields = res.split(",");
			res = convertStartAndEnd(res);
			reservations.put(new ReservationKey(fields[0], fields[2], fields[3]), new CanonicalReservedInstances(res));
		}
				
		BasicReservationService reservationService = new BasicReservationService(ReservationPeriod.oneyear, ReservationUtilization.FIXED, false);
		reservationService.updateReservations(reservations, accountService, startMillis, productService, resourceService);
		
		rp.setDebugHour(0);
		rp.setDebugFamily(debugFamily);
		Region[] debugRegions = new Region[]{ debugRegion };
		rp.setDebugRegions(debugRegions);
		DateTime start = new DateTime(startMillis);
		
		// Initialize the price lists
    	Map<Product, InstancePrices> prices = Maps.newHashMap();
    	prices.put(productService.getProductByName(Product.ec2Instance), priceListService.getPrices(start, ServiceCode.AmazonEC2));
    	if (reservationService.hasRdsReservations())
    		prices.put(productService.getProductByName(Product.rdsInstance), priceListService.getPrices(start, ServiceCode.AmazonRDS));
    	if (reservationService.hasRedshiftReservations())
    		prices.put(productService.getProductByName(Product.redshift), priceListService.getPrices(start, ServiceCode.AmazonRedshift));

		rp.process(reservationService, data, product, start, prices);
	}
	
	/*
	 * Test one AZ scoped full-upfront reservation that's used by the owner.
	 */
	@Test
	public void testUsedFixedAZ() throws Exception {
		long startMillis = 1491004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,1464702209129,1496238208000,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		
		Datum[] usageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.bonusReservedInstancesFixed, "m1.large", 1.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.reservedInstancesFixed, "m1.large", 1.0),
		};
		
		Datum[] costData = new Datum[]{				
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.bonusReservedInstancesFixed, "m1.large", 0.0),
		};
		Datum[] expectedCostData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.reservedInstancesFixed, "m1.large", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.upfrontAmortizedFixed, "m1.large", 0.095),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.savingsFixed, "m1.large", 0.175 - 0.095),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
		
		/* Cost and Usage version */
		usageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesFixed, "m1.large", null, "2aaaaaaa-bbbb-cccc-ddddddddddddddddd", 1.0),
		};
		costData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesFixed, "m1.large", null, "2aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.0),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
		
		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		costData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesFixed, "m1.large", null, "2aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.upfrontAmortizedFixed, "m1.large", null, "2aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.095),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
	}

	/*
	 * Test one AZ scoped full-upfront reservation that isn't used.
	 */
	@Test
	public void testUnusedFixedAZ() throws Exception {
		long startMillis = 1491004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,1464702209129,1496238208000,31536000,0.0,835.0,14,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		
		Datum[] usageData = new Datum[]{
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.unusedInstancesFixed, "m1.large", 14.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.unusedInstancesFixed, "m1.large", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.upfrontAmortizedFixed, "m1.large", 1.3345),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.savingsFixed, "m1.large", -1.3345),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
		
		/* Cost and Usage version */
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
	}

	/*
	 * Test two AZ scoped reservations - one HEAVY and one FIXED that are both used by the owner account.
	 */
	@Test
	public void testTwoUsedHeavyFixedAZ() throws Exception {
		long startMillis = 1491004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,1464702209129,1496238208000,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
			"111111111111,EC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,1464702209129,1496238208000,31536000,0.0,0.0,1,Linux/UNIX (Amazon VPC),active,USD,No Upfront,Hourly:0.112",
		};
		
		Datum[] usageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.bonusReservedInstancesFixed, "m1.large", 1.0),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.bonusReservedInstancesHeavy, "m1.large", 1.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.reservedInstancesFixed, "m1.large", 1.0),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.reservedInstancesHeavy, "m1.large", 1.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.reservedInstancesHeavy, "m1.large", 0.112),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.reservedInstancesFixed, "m1.large", 0.0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.upfrontAmortizedFixed, "m1.large", 0.095),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.savingsFixed, "m1.large", 0.175 - 0.095),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.savingsHeavy, "m1.large", 0.175 - 0.112),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
		
		/* Cost and Usage version */
		usageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesFixed, "m1.large", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 1.0),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesHeavy, "m1.large", null, "2aaaaaaa-bbbb-cccc-ddddddddddddddddd", 1.0),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
		
		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		costData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.upfrontAmortizedFixed, "m1.large", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.095),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesHeavy, "m1.large", null, "2aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.112),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
	}


	/*
	 * Test two equivalent AZ scoped full-upfront reservation that are both used by the owner account.
	 */
	@Test
	public void testTwoUsedSameFixedAZ() throws Exception {
		long startMillis = 1491004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,1464702209129,1496238208000,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
			"111111111111,EC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,1464702209129,1496238208000,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		
		Datum[] usageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.bonusReservedInstancesFixed, "m1.large", 2.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.reservedInstancesFixed, "m1.large", 2.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.reservedInstancesFixed, "m1.large", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.upfrontAmortizedFixed, "m1.large", 0.190),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.savingsFixed, "m1.large", 0.175 * 2.0 - 0.190),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
		
		/* Cost and Usage version */
		usageData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesFixed, "m1.large", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 1.0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesFixed, "m1.large", null, "2aaaaaaa-bbbb-cccc-ddddddddddddddddd", 1.0),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");

		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		costData = new Datum[]{				
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.upfrontAmortizedFixed, "m1.large", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.095),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.upfrontAmortizedFixed, "m1.large", null, "2aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.095),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
	}

	/*
	 * Test one AZ scoped full-upfront reservations where one instance is used by the owner account and one borrowed by a second account. Three instances are unused.
	 */
	@Test
	public void testFixedAZ() throws Exception {
		long startMillis = 1491004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.small,Availability Zone,us-east-1a,false,1464699998099,1496235997000,31536000,0.0,206.0,5,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		
		Datum[] usageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.bonusReservedInstancesFixed, "m1.small", 1.0),
			new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, Operation.bonusReservedInstancesFixed, "m1.small", 1.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.reservedInstancesFixed, "m1.small", 1.0),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.unusedInstancesFixed, "m1.small", 3.0),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.lentInstancesFixed, "m1.small", 1.0),
			new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, Operation.borrowedInstancesFixed, "m1.small", 1.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.reservedInstancesFixed, "m1.small", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.lentInstancesFixed, "m1.small", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.unusedInstancesFixed, "m1.small", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.upfrontAmortizedFixed, "m1.small", 0.1176),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.savingsFixed, "m1.small", 0.044 * 2.0 - 0.1176),
			new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, Operation.borrowedInstancesFixed, "m1.small", 0.0),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
		
		/* Cost and Usage version */
		usageData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesFixed, "m1.small", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 1.0),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesFixed, "m1.small", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 1.0),
		};
		expectedCostData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.reservedInstancesFixed, "m1.small", 0.0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.lentInstancesFixed, "m1.small", 0.0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.unusedInstancesFixed, "m1.small", 0.0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.upfrontAmortizedFixed, "m1.small", 0.09406),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, Operation.upfrontAmortizedFixed, "m1.small", 0.02352),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.savingsFixed, "m1.small", 0.044 * 1.0 - 0.09406), // penalty for unused all goes to owner
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, Operation.savingsFixed, "m1.small", 0.044 * 1.0 - 0.02352),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, Operation.borrowedInstancesFixed, "m1.small", 0.0),
			};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");

		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		costData = new Datum[]{				
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.upfrontAmortizedFixed, "m1.small", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.02352),
			new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.upfrontAmortizedFixed, "m1.small", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.02352),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
	}

	/*
	 * Test one Region scoped full-upfront reservation where one instance is used by the owner account in each of several AZs.
	 */
	@Test
	public void testFixedRegionalMultiAZ() throws Exception {
		long startMillis = 1491004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.small,Region,,false,1464699998099,1496235997000,31536000,0.0,206.0,5,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		
		Datum[] usageData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.bonusReservedInstancesFixed, "m1.small", 1.0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1b, Operation.bonusReservedInstancesFixed, "m1.small", 1.0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1c, Operation.bonusReservedInstancesFixed, "m1.small", 1.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.reservedInstancesFixed, "m1.small", 1.0),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1b, Operation.reservedInstancesFixed, "m1.small", 1.0),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1c, Operation.reservedInstancesFixed, "m1.small", 1.0),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.unusedInstancesFixed, "m1.small", 2.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.reservedInstancesFixed, "m1.small", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1b, Operation.reservedInstancesFixed, "m1.small", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1c, Operation.reservedInstancesFixed, "m1.small", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.unusedInstancesFixed, "m1.small", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.upfrontAmortizedFixed, "m1.small", 0.1176),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.savingsFixed, "m1.small", 0.044 * 3.0 - 0.1176),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");		

		/* Cost and Usage version */
		usageData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesFixed, "m1.small", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 1.0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1b, ec2Instance, Operation.bonusReservedInstancesFixed, "m1.small", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 1.0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1c, ec2Instance, Operation.bonusReservedInstancesFixed, "m1.small", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 1.0),
		};
		expectedCostData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.reservedInstancesFixed, "m1.small", 0.0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1b, Operation.reservedInstancesFixed, "m1.small", 0.0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1c, Operation.reservedInstancesFixed, "m1.small", 0.0),
				new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.unusedInstancesFixed, "m1.small", 0.0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.upfrontAmortizedFixed, "m1.small", 0.02352),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1b, Operation.upfrontAmortizedFixed, "m1.small", 0.02352),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1c, Operation.upfrontAmortizedFixed, "m1.small", 0.02352),
				new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.upfrontAmortizedFixed, "m1.small", 2.0 * 0.02352),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.savingsFixed, "m1.small", 0.044 - 0.02352),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1b, Operation.savingsFixed, "m1.small", 0.044 - 0.02352),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1c, Operation.savingsFixed, "m1.small", 0.044 - 0.02352),
				new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.savingsFixed, "m1.small", 2.0 * -0.02352),
			};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
		
		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		costData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.upfrontAmortizedFixed, "m1.small", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.02352),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1b, ec2Instance, Operation.upfrontAmortizedFixed, "m1.small", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.02352),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1c, ec2Instance, Operation.upfrontAmortizedFixed, "m1.small", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.02352),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
	}
	
	/*
	 * Test two full-upfront reservations - one AZ, one Region that are both used by the owner account.
	 */
	@Test
	public void testTwoUsedOneAZOneRegion() throws Exception {
		long startMillis = 1491004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,1464702209129,1496238208000,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
			"111111111111,EC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Region,,false,1464702209129,1496238208000,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		
		Datum[] usageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.bonusReservedInstancesFixed, "m1.large", 2.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.reservedInstancesFixed, "m1.large", 2.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.reservedInstancesFixed, "m1.large", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.upfrontAmortizedFixed, "m1.large", 0.095),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.savingsFixed, "m1.large", 0.175 - 0.095),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.upfrontAmortizedFixed, "m1.large", 0.095),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.savingsFixed, "m1.large", 0.175 - 0.095),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
		
		/* Cost and Usage version */
		usageData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesFixed, "m1.large", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 1.0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesFixed, "m1.large", null, "2aaaaaaa-bbbb-cccc-ddddddddddddddddd", 1.0),
		};
		expectedCostData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.reservedInstancesFixed, "m1.large", 0.0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.upfrontAmortizedFixed, "m1.large", 2.0 * 0.095),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.savingsFixed, "m1.large", 2.0 * (0.175 - 0.095)),
			};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
		
		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		costData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.upfrontAmortizedFixed, "m1.large", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.095),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.upfrontAmortizedFixed, "m1.large", null, "2aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.095),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
	}

	/*
	 * Test two full-upfront reservations - both AZ that are both used by the owner account.
	 */
	@Test
	public void testTwoUsedAZ() throws Exception {
		long startMillis = 1491004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,1464702209129,1496238208000,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
			"111111111111,EC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,1464702209129,1496238208000,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		
		Datum[] usageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.bonusReservedInstancesFixed, "m1.large", 2.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.reservedInstancesFixed, "m1.large", 2.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.reservedInstancesFixed, "m1.large", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.upfrontAmortizedFixed, "m1.large", 2.0 * 0.095),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.savingsFixed, "m1.large", 2.0 * 0.175 - 2.0 * 0.095),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");		
		
		/* Cost and Usage version */
		usageData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesFixed, "m1.large", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 1.0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesFixed, "m1.large", null, "2aaaaaaa-bbbb-cccc-ddddddddddddddddd", 1.0),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
		
		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		costData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.upfrontAmortizedFixed, "m1.large", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.095),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.upfrontAmortizedFixed, "m1.large", null, "2aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.095),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
	}

	/*
	 * Test two full-upfront reservations - both Region that are both used by the owner account.
	 */
	@Test
	public void testTwoUsedRegion() throws Exception {
		long startMillis = 1491004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Region,,false,1464702209129,1496238208000,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
			"111111111111,EC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Region,,false,1464702209129,1496238208000,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		
		Datum[] usageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.bonusReservedInstancesFixed, "m1.large", 2.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.reservedInstancesFixed, "m1.large", 2.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.reservedInstancesFixed, "m1.large", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.upfrontAmortizedFixed, "m1.large", 2.0 * 0.095),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.savingsFixed, "m1.large", 2.0 * (0.175 - 0.095)),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");		
		
		/* Cost and Usage version */
		usageData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesFixed, "m1.large", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 1.0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesFixed, "m1.large", null, "2aaaaaaa-bbbb-cccc-ddddddddddddddddd", 1.0),
		};
		expectedCostData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.reservedInstancesFixed, "m1.large", 0.0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.upfrontAmortizedFixed, "m1.large", 2.0 * 0.095),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.savingsFixed, "m1.large", 2.0 * (0.175 - 0.095)),
			};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
		
		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		costData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.upfrontAmortizedFixed, "m1.large", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.095),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.upfrontAmortizedFixed, "m1.large", null, "2aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.095),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
	}

	/*
	 * Test two full-upfront reservations - one AZ, one Region that are both used by a borrowing account.
	 */
	@Test
	public void testTwoUsedOneAZOneRegionBorrowed() throws Exception {
		long startMillis = 1491004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,1464702209129,1496238208000,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
			"111111111111,EC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Region,,false,1464702209129,1496238208000,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		
		Datum[] usageData = new Datum[]{
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, Operation.bonusReservedInstancesFixed, "m1.large", 1.0),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1b, Operation.bonusReservedInstancesFixed, "m1.large", 1.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.lentInstancesFixed, "m1.large", 1.0),
			new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, Operation.borrowedInstancesFixed, "m1.large", 1.0),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.lentInstancesFixed, "m1.large", 1.0),
			new Datum(accounts.get(1), Region.US_EAST_1, us_east_1b, Operation.borrowedInstancesFixed, "m1.large", 1.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.lentInstancesFixed, "m1.large", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.lentInstancesFixed, "m1.large", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.upfrontAmortizedFixed, "m1.large", 0.095),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.savingsFixed, "m1.large", 0.175 - 0.095),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.upfrontAmortizedFixed, "m1.large", 0.095),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.savingsFixed, "m1.large", 0.175 - 0.095),
			new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, Operation.borrowedInstancesFixed, "m1.large", 0.0),
			new Datum(accounts.get(1), Region.US_EAST_1, us_east_1b, Operation.borrowedInstancesFixed, "m1.large", 0.0),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");		
		
		/* Cost and Usage version */
		usageData = new Datum[]{
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesFixed, "m1.large", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 1.0),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1b, ec2Instance, Operation.bonusReservedInstancesFixed, "m1.large", null, "2aaaaaaa-bbbb-cccc-ddddddddddddddddd", 1.0),
		};
		expectedCostData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.lentInstancesFixed, "m1.large", 0.0),
				new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.lentInstancesFixed, "m1.large", 0.0),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, Operation.upfrontAmortizedFixed, "m1.large", 0.095),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, Operation.savingsFixed, "m1.large", 0.175 - 0.095),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1b, Operation.upfrontAmortizedFixed, "m1.large", 0.095),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1b, Operation.savingsFixed, "m1.large", 0.175 - 0.095),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, Operation.borrowedInstancesFixed, "m1.large", 0.0),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1b, Operation.borrowedInstancesFixed, "m1.large", 0.0),
			};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
		
		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		costData = new Datum[]{
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.upfrontAmortizedFixed, "m1.large", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.095),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1b, ec2Instance, Operation.upfrontAmortizedFixed, "m1.large", null, "2aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.095),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
	}

	/*
	 * Test one Region scoped full-upfront reservation where four small instance reservations are used by one large instance in the owner account.
	 */
	@Test
	public void testFixedRegionalFamily() throws Exception {
		long startMillis = 1491004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.small,Region,,false,1464699998099,1496235997000,31536000,0.0,206.0,4,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		
		Datum[] usageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.bonusReservedInstancesFixed, "m1.large", 1.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.familyReservedInstancesFixed, "m1.large", 1.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.familyReservedInstancesFixed, "m1.large", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.upfrontAmortizedFixed, "m1.small", 0.094),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.savingsFixed, "m1.small", 0.044 * 4.0 - 0.094),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");		
		
		/* Cost and Usage version */
		usageData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesFixed, "m1.large", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 1.0),
		};
		expectedCostData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.familyReservedInstancesFixed, "m1.large", 0.0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.upfrontAmortizedFixed, "m1.large", 0.094),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.savingsFixed, "m1.large", 0.044 * 4.0 - 0.094),
			};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
		
		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		costData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.upfrontAmortizedFixed, "m1.large", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.094),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
	}

	/*
	 * Test one Region scoped partial-upfront reservation where four small instance reservations are used by one large instance in the owner account.
	 */
	@Test
	public void testPartialRegionalFamily() throws Exception {
		long startMillis = 1491004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.small,Region,,false,1464699998099,1496235997000,31536000,0.0,123.0,4,Linux/UNIX (Amazon VPC),active,USD,Partial Upfront,Hourly:0.01",
		};
		
		Datum[] usageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.bonusReservedInstancesPartial, "m1.large", 1.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.familyReservedInstancesPartial, "m1.large", 1.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.familyReservedInstancesPartial, "m1.large", 4.0 * 0.01),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.upfrontAmortizedPartial, "m1.small", 4.0 * 0.014),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.savingsPartial, "m1.small", 4.0 * (0.044 - 0.014 - 0.01)),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");		
		
		/* Cost and Usage version */
		usageData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesPartial, "m1.large", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 1.0),
		};
		expectedCostData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.familyReservedInstancesPartial, "m1.large", 4.0 * 0.01),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.upfrontAmortizedPartial, "m1.large", 4.0 * 0.014),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.savingsPartial, "m1.large", 4.0 * (0.044 - 0.014 - 0.01)),
			};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
		
		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		costData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesPartial, "m1.large", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 4.0 * 0.01),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.upfrontAmortizedPartial, "m1.large", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 4.0 * 0.014),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
	}

	/*
	 * Test one Region scoped full-upfront reservation where one instance is used by the owner account and one borrowed by a second account - 3 instances unused.
	 */
	@Test
	public void testFixedRegional() throws Exception {
		long startMillis = 1491004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.small,Region,,false,1464699998099,1496235997000,31536000,0.0,206.0,5,Linux/UNIX (Amazon VPC),active,USD,All Upfront",
		};
		
		Datum[] usageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.bonusReservedInstancesFixed, "m1.small", 1.0),
			new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, Operation.bonusReservedInstancesFixed, "m1.small", 1.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.reservedInstancesFixed, "m1.small", 1.0),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.unusedInstancesFixed, "m1.small", 3.0),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.lentInstancesFixed, "m1.small", 1.0),
			new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, Operation.borrowedInstancesFixed, "m1.small", 1.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.reservedInstancesFixed, "m1.small", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.upfrontAmortizedFixed, "m1.small", 0.1176),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.savingsFixed, "m1.small", 0.044 * 2 - 0.1176),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.unusedInstancesFixed, "m1.small", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.lentInstancesFixed, "m1.small", 0.0),
			new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, Operation.borrowedInstancesFixed, "m1.small", 0.0),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");		
		
		/* Cost and Usage version */
		usageData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesFixed, "m1.small", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 1.0),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesFixed, "m1.small", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 1.0),
		};
		expectedCostData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.reservedInstancesFixed, "m1.small", 0.0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.upfrontAmortizedFixed, "m1.small", 0.02352),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.upfrontAmortizedFixed, "m1.small", 0.02352),
				new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.upfrontAmortizedFixed, "m1.small", 3.0 * 0.02352),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.savingsFixed, "m1.small", 0.044 - 0.02352),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, Operation.savingsFixed, "m1.small", 0.044 - 0.02352),
				new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.savingsFixed, "m1.small", 3.0 * -0.02352),
				new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.unusedInstancesFixed, "m1.small", 0.0),
				new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.lentInstancesFixed, "m1.small", 0.0),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, Operation.borrowedInstancesFixed, "m1.small", 0.0),
			};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
		
		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		costData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.upfrontAmortizedFixed, "m1.small", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.02352),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.upfrontAmortizedFixed, "m1.small", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.02352),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
	}

	/*
	 * Test two Region scoped full-upfront reservations where one instance from each is family borrowed by a third account.
	 */
	@Test
	public void testFixedTwoRegionalFamilyBorrowed() throws Exception {
		long startMillis = 1491004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
				"111111111111,EC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.small,Region,,false,1464699998099,1496235997000,31536000,0.0,206.0,4,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
				"222222222222,EC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.small,Region,,false,1464699998099,1496235997000,31536000,0.0,206.0,4,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		
		Datum[] usageData = new Datum[]{
			new Datum(accounts.get(2), Region.US_EAST_1, us_east_1a, Operation.bonusReservedInstancesFixed, "m1.xlarge", 1.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.lentInstancesFixed, "m1.small", 4.0),
			new Datum(accounts.get(1), Region.US_EAST_1, null, Operation.lentInstancesFixed, "m1.small", 4.0),
			new Datum(accounts.get(2), Region.US_EAST_1, us_east_1a, Operation.borrowedInstancesFixed, "m1.xlarge", 1.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.upfrontAmortizedFixed, "m1.small", 0.094),
				new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.savingsFixed, "m1.small", 0.044 * 4 - 0.094),
				new Datum(accounts.get(1), Region.US_EAST_1, null, Operation.upfrontAmortizedFixed, "m1.small", 0.094),
				new Datum(accounts.get(1), Region.US_EAST_1, null, Operation.savingsFixed, "m1.small", 0.044 * 4 - 0.094),
				new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.lentInstancesFixed, "m1.small", 0.0),
				new Datum(accounts.get(1), Region.US_EAST_1, null, Operation.lentInstancesFixed, "m1.small", 0.0),
				new Datum(accounts.get(2), Region.US_EAST_1, us_east_1a, Operation.borrowedInstancesFixed, "m1.xlarge", 0.0),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");		
		
		/* Cost and Usage version */
		usageData = new Datum[]{
				new Datum(accounts.get(2), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesFixed, "m1.xlarge", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.5),
				new Datum(accounts.get(2), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesFixed, "m1.xlarge", null, "2aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.5),
		};
		expectedCostData = new Datum[]{
				new Datum(accounts.get(2), Region.US_EAST_1, us_east_1a, Operation.savingsFixed, "m1.xlarge", 8.0 * (0.044 - 0.02352)),
				new Datum(accounts.get(2), Region.US_EAST_1, us_east_1a, Operation.upfrontAmortizedFixed, "m1.xlarge", 8.0 * 0.02352),
				new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.lentInstancesFixed, "m1.small", 0.0),
				new Datum(accounts.get(1), Region.US_EAST_1, null, Operation.lentInstancesFixed, "m1.small", 0.0),
				new Datum(accounts.get(2), Region.US_EAST_1, us_east_1a, Operation.borrowedInstancesFixed, "m1.xlarge", 0.0),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
		
		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		costData = new Datum[]{
				new Datum(accounts.get(2), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.upfrontAmortizedFixed, "m1.xlarge", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 4.0 * 0.02352),
				new Datum(accounts.get(2), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.upfrontAmortizedFixed, "m1.xlarge", null, "2aaaaaaa-bbbb-cccc-ddddddddddddddddd", 4.0 * 0.02352),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
	}
	
	/*
	 * Test one Region scoped full-upfront RDS reservation where the instance is used.
	 */
	@Test
	public void testFixedRDS() throws Exception {
		long startMillis = 1491004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,RDS,us-east-1,ri-2016-05-20-16-50-03-197,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,db.t2.small,,,false,1463763023778,1495299023778,31536000,0.0,195.0,1,mysql,active,USD,All Upfront,",
		};
		
		Datum[] usageData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, null, rdsInstance, Operation.bonusReservedInstancesFixed, "db.t2.small.mysql", 1.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, null, rdsInstance, Operation.reservedInstancesFixed, "db.t2.small.mysql", 1.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, null, rdsInstance, Operation.reservedInstancesFixed, "db.t2.small.mysql", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, null, rdsInstance, Operation.upfrontAmortizedFixed, "db.t2.small.mysql", 0.0223),
			new Datum(accounts.get(0), Region.US_EAST_1, null, rdsInstance, Operation.savingsFixed, "db.t2.small.mysql", 0.034 - 0.0223),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "db");
		
		/* Cost and Usage version */
		usageData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, null, rdsInstance, Operation.bonusReservedInstancesFixed, "db.t2.small.mysql", null, "ri-2016-05-20-16-50-03-197", 1.0),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "db");
		
		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		costData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, null, rdsInstance, Operation.upfrontAmortizedFixed, "db.t2.small.mysql", null, "ri-2016-05-20-16-50-03-197", 0.0223),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
	}
	
	/*
	 * Test one Region scoped full-upfront RDS reservation where the instance is used.
	 */
	@Test
	public void testPartialRDS() throws Exception {
		long startMillis = 1491004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,RDS,ap-southeast-2,ri-2017-02-01-06-08-23-918,573d345b-7d5d-42eb-a340-5c19bf82b338,db.t2.micro,,,false,1485929307960,1517465307960,31536000,0.0,79.0,2,postgresql,active,USD,Partial Upfront,Hourly:0.012",
		};
		
		Datum[] usageData = new Datum[]{
				new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, null, rdsInstance, Operation.bonusReservedInstancesPartial, "db.t2.micro.postgres", 2.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, null, rdsInstance, Operation.reservedInstancesPartial, "db.t2.micro.postgres", 2.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
			new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, null, rdsInstance, Operation.reservedInstancesPartial, "db.t2.micro.postgres", 0.024),
			new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, null, rdsInstance, Operation.upfrontAmortizedPartial, "db.t2.micro.postgres", 0.018),
			new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, null, rdsInstance, Operation.savingsPartial, "db.t2.micro.postgres", 2.0 * 0.028 - 0.018 - 2.0 * 0.012),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "db");
		
		/* Cost and Usage version */
		usageData = new Datum[]{
				new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, null, rdsInstance, Operation.bonusReservedInstancesPartial, "db.t2.micro.postgres", null, "ri-2017-02-01-06-08-23-918", 2.0),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "db");
		
		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		costData = new Datum[]{
				new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, null, rdsInstance, Operation.bonusReservedInstancesPartial, "db.t2.micro.postgres", null, "ri-2017-02-01-06-08-23-918", 0.024),
				new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, null, rdsInstance, Operation.upfrontAmortizedPartial, "db.t2.micro.postgres", null, "ri-2017-02-01-06-08-23-918", 0.018),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
	}
	
	/*
	 * Test one Region scoped partial-upfront reservation that's used by the owner.
	 */
	@Test
	public void testUsedPartialRegion() throws Exception {
		long startMillis = 1494004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,c4.2xlarge,Region,,false,1493283689633,1524819688000,31536000,0.0,1060.0,1,Linux/UNIX,active,USD,Partial Upfront,Hourly:0.121",
		};
		
		Datum[] usageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.bonusReservedInstancesPartial, "c4.2xlarge", 1.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.reservedInstancesPartial, "c4.2xlarge", 1.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.reservedInstancesPartial, "c4.2xlarge", 0.121),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.upfrontAmortizedPartial, "c4.2xlarge", 0.121),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.savingsPartial, "c4.2xlarge", 0.398 - 0.121 - 0.121),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "c4");		
		
		/* Cost and Usage version */
		usageData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesPartial, "c4.2xlarge", null, "2aaaaaaa-bbbb-cccc-ddddddddddddddddd", 1.0),
		};
		expectedCostData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.reservedInstancesPartial, "c4.2xlarge", 0.121),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.upfrontAmortizedPartial, "c4.2xlarge", 0.121),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.savingsPartial, "c4.2xlarge", 0.398 - 0.121 - 0.121),
			};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "c4");
		
		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		costData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesPartial, "c4.2xlarge", null, "2aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.121),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.upfrontAmortizedPartial, "c4.2xlarge", null, "2aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.121),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
	}

	/*
	 * Test one Region scoped partial-upfront reservation that's used by the owner in last hour of reservation.
	 * Reservation really ends at start of hour.
	 */
	@Test
	public void testUsedPartialRegionLastHour() throws Exception {
		long startMillis = DateTime.parse("2017-08-01T00:00:00Z").getMillis();
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,c4.2xlarge,Region,,false,2016-08-01 00:05:35,2017-08-01 00:05:34,31536000,0.0,1060.0,1,Linux/UNIX,active,USD,Partial Upfront,Hourly:0.121",
		};
		
		Datum[] usageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.bonusReservedInstancesPartial, "c4.2xlarge", 1.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.bonusReservedInstancesPartial, "c4.2xlarge", 1.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "c4");		
	}

	/*
	 * Test one Region scoped partial-upfront reservation for Windows that's used by the owner.
	 */
	@Test
	public void testUsedPartialRegionWindows() throws Exception {
		long startMillis = 1494004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,ap-southeast-2,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,t2.medium,Region,,false,2017-02-01 06:00:35,2018-02-01 06:00:34,31536000,0.0,289.0,1,Windows,active,USD,Partial Upfront,Hourly:0.033,",
		};
		
		Datum[] usageData = new Datum[]{
			new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, ap_southeast_2a, Operation.bonusReservedInstancesPartial, "t2.medium.windows", 1.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, ap_southeast_2a, Operation.reservedInstancesPartial, "t2.medium.windows", 1.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
			new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, ap_southeast_2a, Operation.reservedInstancesPartial, "t2.medium.windows", 0.033),
			new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, null, Operation.upfrontAmortizedPartial, "t2.medium.windows", 0.033),
			new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, null, Operation.savingsPartial, "t2.medium.windows", 0.082 - 0.033 - 0.033),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "t2");		
		
		/* Cost and Usage version */
		usageData = new Datum[]{
				new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, ap_southeast_2a, ec2Instance, Operation.bonusReservedInstancesPartial, "t2.medium.windows", null, "2aaaaaaa-bbbb-cccc-ddddddddddddddddd", 1.0),
		};
		expectedCostData = new Datum[]{
				new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, ap_southeast_2a, Operation.reservedInstancesPartial, "t2.medium.windows", 0.033),
				new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, ap_southeast_2a, Operation.upfrontAmortizedPartial, "t2.medium.windows", 0.033),
				new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, ap_southeast_2a, Operation.savingsPartial, "t2.medium.windows", 0.082 - 0.033 - 0.033),
			};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "c4");
		
		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		costData = new Datum[]{
				new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, ap_southeast_2a, ec2Instance, Operation.bonusReservedInstancesPartial, "t2.medium.windows", null, "2aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.033),
				new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, ap_southeast_2a, ec2Instance, Operation.upfrontAmortizedPartial, "t2.medium.windows", null, "2aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.033),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
	}

	@Test
	public void testUsedUnusedDifferentRegionAndBorrowedFamilyPartialRegion() throws Exception {
		long startMillis = DateTime.parse("2017-08-01").getMillis();
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,us-west-2,aaaaaaaa-382f-40b9-b2d3-8641b05313f9,,c4.large,Region,,false,2017-04-12 21:29:39,2018-04-12 21:29:38,31536000,0.0,249.85000610351562,20,Linux/UNIX,active,USD,Partial Upfront,Hourly:0.0285",
			"222222222222,EC2,eu-west-1,bbbbbbbb-0ce3-4ab0-8d0e-36deac008bdd,,c4.xlarge,Region,,false,2017-03-08 09:00:00,2017-08-18 06:07:40,31536000,0.0,340.0,2,Linux/UNIX,retired,USD,Partial Upfront,Hourly:0.039",
		};
		
		Datum[] usageData = new Datum[]{
			new Datum(accounts.get(0), Region.EU_WEST_1, eu_west_1b, Operation.bonusReservedInstancesPartial, "c4.2xlarge", 0.25),
			new Datum(accounts.get(1), Region.EU_WEST_1, eu_west_1c, Operation.bonusReservedInstancesPartial, "c4.xlarge", 1.5),
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.EU_WEST_1, eu_west_1b, Operation.borrowedInstancesPartial, "c4.2xlarge", 0.25),
			new Datum(accounts.get(1), Region.EU_WEST_1, eu_west_1c, Operation.reservedInstancesPartial, "c4.xlarge", 1.5),
			new Datum(accounts.get(1), Region.EU_WEST_1, null, Operation.lentInstancesPartial, "c4.xlarge", 0.5),
			new Datum(accounts.get(0), Region.US_WEST_2, null, Operation.unusedInstancesPartial, "c4.large", 20.0),
		};
		
		Datum[] costData = new Datum[]{
		};
		Datum[] expectedCostData = new Datum[]{
			new Datum(accounts.get(0), Region.EU_WEST_1, eu_west_1b, Operation.borrowedInstancesPartial, "c4.2xlarge", 0.039 * 0.50),
			new Datum(accounts.get(0), Region.US_WEST_2, null, Operation.unusedInstancesPartial, "c4.large", 0.0285 * 20.0),
			new Datum(accounts.get(0), Region.US_WEST_2, null, Operation.upfrontAmortizedPartial, "c4.large", 0.0285 * 20.0),
			new Datum(accounts.get(0), Region.US_WEST_2, null, Operation.savingsPartial, "c4.large", -(0.0285 + 0.0285) * 20.0),
			new Datum(accounts.get(1), Region.EU_WEST_1, eu_west_1c, Operation.reservedInstancesPartial, "c4.xlarge", 0.039 * 1.5),
			new Datum(accounts.get(1), Region.EU_WEST_1, null, Operation.upfrontAmortizedPartial, "c4.xlarge", 0.039 * 2.0),
			new Datum(accounts.get(1), Region.EU_WEST_1, null, Operation.savingsPartial, "c4.xlarge", (0.226 - 0.039 - 0.039) * 2.0),
			new Datum(accounts.get(1), Region.EU_WEST_1, null, Operation.lentInstancesPartial, "c4.xlarge", 0.5 * 0.039),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "c4");
		
		/* Cost and Usage version */
		usageData = new Datum[]{
				new Datum(accounts.get(0), Region.EU_WEST_1, eu_west_1b, ec2Instance, Operation.bonusReservedInstancesPartial, "c4.2xlarge", null, "bbbbbbbb-0ce3-4ab0-8d0e-36deac008bdd", 0.25),
				new Datum(accounts.get(1), Region.EU_WEST_1, eu_west_1c, ec2Instance, Operation.bonusReservedInstancesPartial, "c4.xlarge", null, "bbbbbbbb-0ce3-4ab0-8d0e-36deac008bdd", 1.5),
		};
		expectedCostData = new Datum[]{
				new Datum(accounts.get(0), Region.EU_WEST_1, eu_west_1b, Operation.borrowedInstancesPartial, "c4.2xlarge", 0.039 * 0.50),
				new Datum(accounts.get(0), Region.US_WEST_2, null, Operation.unusedInstancesPartial, "c4.large", 0.0285 * 20.0),
				new Datum(accounts.get(0), Region.US_WEST_2, null, Operation.upfrontAmortizedPartial, "c4.large", 0.0285 * 20.0),
				new Datum(accounts.get(0), Region.US_WEST_2, null, Operation.savingsPartial, "c4.large", -(0.0285 + 0.0285) * 20.0),
				new Datum(accounts.get(1), Region.EU_WEST_1, eu_west_1c, Operation.reservedInstancesPartial, "c4.xlarge", 0.039 * 1.5),
				new Datum(accounts.get(1), Region.EU_WEST_1, eu_west_1c, Operation.upfrontAmortizedPartial, "c4.xlarge", 0.039 * 1.5),
				new Datum(accounts.get(0), Region.EU_WEST_1, eu_west_1b, Operation.upfrontAmortizedPartial, "c4.2xlarge", 0.039 * 0.5),
				new Datum(accounts.get(1), Region.EU_WEST_1, eu_west_1c, Operation.savingsPartial, "c4.xlarge", (0.226 - 0.039 - 0.039) * 1.5),
				new Datum(accounts.get(0), Region.EU_WEST_1, eu_west_1b, Operation.savingsPartial, "c4.2xlarge", (0.226 - 0.039 - 0.039) * 0.5),
				new Datum(accounts.get(1), Region.EU_WEST_1, null, Operation.lentInstancesPartial, "c4.xlarge", 0.5 * 0.039),
			};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "c4");
		
		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		costData = new Datum[]{
				new Datum(accounts.get(0), Region.EU_WEST_1, eu_west_1b, ec2Instance, Operation.bonusReservedInstancesPartial, "c4.2xlarge", null, "bbbbbbbb-0ce3-4ab0-8d0e-36deac008bdd", 0.5 * 0.039),
				new Datum(accounts.get(0), Region.EU_WEST_1, eu_west_1b, ec2Instance, Operation.upfrontAmortizedPartial, "c4.2xlarge", null, "bbbbbbbb-0ce3-4ab0-8d0e-36deac008bdd", 0.5 * 0.039),
				new Datum(accounts.get(1), Region.EU_WEST_1, eu_west_1c, ec2Instance, Operation.bonusReservedInstancesPartial, "c4.xlarge", null, "bbbbbbbb-0ce3-4ab0-8d0e-36deac008bdd", 1.5 * 0.039),
				new Datum(accounts.get(1), Region.EU_WEST_1, eu_west_1c, ec2Instance, Operation.upfrontAmortizedPartial, "c4.xlarge", null, "bbbbbbbb-0ce3-4ab0-8d0e-36deac008bdd", 1.5 * 0.039),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
	}	

	@Test
	public void testUsedAndBorrowedPartialRegion() throws Exception {
		long startMillis = 1494004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,us-west-2,aaaaaaaa-588b-46a2-8c05-cbcf87aed53d,,c3.4xlarge,Region,,false,1492041221000,1523577220000,31536000,0.0,2477.60009765625,1,Linux/UNIX,active,USD,Partial Upfront,Hourly:0.19855",
			"222222222222,EC2,us-west-2,bbbbbbbb-1942-4e5e-892b-cec03ddb7816,,c3.4xlarge,Region,,false,1475509708914,1507045707000,31536000,0.0,2608.0,1,Linux/UNIX (Amazon VPC),active,USD,Partial Upfront,Hourly:0.209"
		};
		
		Datum[] usageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, Operation.bonusReservedInstancesPartial, "c3.4xlarge", 2.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, Operation.reservedInstancesPartial, "c3.4xlarge", 1.0),
			new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, Operation.borrowedInstancesPartial, "c3.4xlarge", 1.0),
			new Datum(accounts.get(1), Region.US_WEST_2, null, Operation.lentInstancesPartial, "c3.4xlarge", 1.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
			new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, Operation.reservedInstancesPartial, "c3.4xlarge", 0.199),
			new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, Operation.borrowedInstancesPartial, "c3.4xlarge", 0.209),
			new Datum(accounts.get(1), Region.US_WEST_2, null, Operation.lentInstancesPartial, "c3.4xlarge", 0.209),
			new Datum(accounts.get(0), Region.US_WEST_2, null, Operation.upfrontAmortizedPartial, "c3.4xlarge", 0.283),
			new Datum(accounts.get(0), Region.US_WEST_2, null, Operation.savingsPartial, "c3.4xlarge", 0.84 - 0.283 - 0.199),
			new Datum(accounts.get(1), Region.US_WEST_2, null, Operation.upfrontAmortizedPartial, "c3.4xlarge", 0.298),
			new Datum(accounts.get(1), Region.US_WEST_2, null, Operation.savingsPartial, "c3.4xlarge", 0.84 - 0.298 - 0.209),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "c3");
		
		/* Cost and Usage version */
		usageData = new Datum[]{
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.bonusReservedInstancesPartial, "c3.4xlarge", null, "aaaaaaaa-588b-46a2-8c05-cbcf87aed53d", 1.0),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.bonusReservedInstancesPartial, "c3.4xlarge", null, "bbbbbbbb-1942-4e5e-892b-cec03ddb7816", 1.0),
		};
		expectedCostData = new Datum[]{
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, Operation.reservedInstancesPartial, "c3.4xlarge", 0.199),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, Operation.borrowedInstancesPartial, "c3.4xlarge", 0.209),
				new Datum(accounts.get(1), Region.US_WEST_2, null, Operation.lentInstancesPartial, "c3.4xlarge", 0.209),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, Operation.upfrontAmortizedPartial, "c3.4xlarge", 0.283 + 0.298),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, Operation.savingsPartial, "c3.4xlarge", 0.84 - 0.283 - 0.199 + 0.84 - 0.298 - 0.209),
			};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "c4");
		
		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		costData = new Datum[]{
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.bonusReservedInstancesPartial, "c3.4xlarge", null, "aaaaaaaa-588b-46a2-8c05-cbcf87aed53d", 0.199),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.upfrontAmortizedPartial, "c3.4xlarge", null, "aaaaaaaa-588b-46a2-8c05-cbcf87aed53d", 0.283),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.bonusReservedInstancesPartial, "c3.4xlarge", null, "bbbbbbbb-1942-4e5e-892b-cec03ddb7816", 0.209),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.upfrontAmortizedPartial, "c3.4xlarge", null, "bbbbbbbb-1942-4e5e-892b-cec03ddb7816", 0.298),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
	}
	
	@Test
	public void testUsedAndBorrowedPartialRegionAndAZ() throws Exception {
		long startMillis = 1494004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,us-west-2,aaaaaaaa-08c5-4d02-99f3-d23e51968565,,c4.xlarge,Region,,false,1492033482000,1523569481000,31536000,0.0,503.5,15,Linux/UNIX,active,USD,Partial Upfront,Hourly:0.057",
			"222222222222,EC2,us-west-2,bbbbbbbb-3452-4486-804a-a3d184474ab6,,c4.xlarge,Availability Zone,us-west-2b,false,1474587867448,1506123866000,31536000,0.0,590.0,2,Linux/UNIX (Amazon VPC),active,USD,Partial Upfront,Hourly:0.067",
			"222222222222,EC2,us-west-2,cccccccc-31f5-463a-bc72-b6e53956184f,,c4.xlarge,Availability Zone,us-west-2a,false,1474587867022,1506123866000,31536000,0.0,590.0,1,Linux/UNIX (Amazon VPC),active,USD,Partial Upfront,Hourly:0.067",
		};
		
		Datum[] usageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, Operation.bonusReservedInstancesPartial, "c4.xlarge", 9.0), // 1 az + 8 regional
			new Datum(accounts.get(0), Region.US_WEST_2, us_west_2b, Operation.bonusReservedInstancesPartial, "c4.xlarge", 5.0), // 2 az + 3 regional
			new Datum(accounts.get(0), Region.US_WEST_2, us_west_2c, Operation.bonusReservedInstancesPartial, "c4.xlarge", 4.0), // 4 regional
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_WEST_2, us_west_2c, Operation.reservedInstancesPartial, "c4.xlarge", 4.0),
			new Datum(accounts.get(0), Region.US_WEST_2, us_west_2b, Operation.reservedInstancesPartial, "c4.xlarge", 3.0),
			new Datum(accounts.get(0), Region.US_WEST_2, us_west_2b, Operation.borrowedInstancesPartial, "c4.xlarge", 2.0),
			new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, Operation.reservedInstancesPartial, "c4.xlarge", 8.0),
			new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, Operation.borrowedInstancesPartial, "c4.xlarge", 1.0),
			new Datum(accounts.get(1), Region.US_WEST_2, us_west_2b, Operation.lentInstancesPartial, "c4.xlarge", 2.0),
			new Datum(accounts.get(1), Region.US_WEST_2, us_west_2a, Operation.lentInstancesPartial, "c4.xlarge", 1.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
			new Datum(accounts.get(0), Region.US_WEST_2, us_west_2c, Operation.reservedInstancesPartial, "c4.xlarge", 0.228),
			new Datum(accounts.get(0), Region.US_WEST_2, us_west_2b, Operation.reservedInstancesPartial, "c4.xlarge", 0.171),
			new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, Operation.reservedInstancesPartial, "c4.xlarge", 0.456),
			new Datum(accounts.get(0), Region.US_WEST_2, us_west_2b, Operation.borrowedInstancesPartial, "c4.xlarge", 0.134),
			new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, Operation.borrowedInstancesPartial, "c4.xlarge", 0.067),
			new Datum(accounts.get(1), Region.US_WEST_2, us_west_2b, Operation.lentInstancesPartial, "c4.xlarge", 0.134),
			new Datum(accounts.get(1), Region.US_WEST_2, us_west_2a, Operation.lentInstancesPartial, "c4.xlarge", 0.067),
			new Datum(accounts.get(0), Region.US_WEST_2, null, Operation.upfrontAmortizedPartial, "c4.xlarge", 0.862),
			new Datum(accounts.get(0), Region.US_WEST_2, null, Operation.savingsPartial, "c4.xlarge", 15.0 * 0.199 - 0.862 - 15.0 * 0.057),
			new Datum(accounts.get(1), Region.US_WEST_2, us_west_2b, Operation.upfrontAmortizedPartial, "c4.xlarge", 0.134),
			new Datum(accounts.get(1), Region.US_WEST_2, us_west_2b, Operation.savingsPartial, "c4.xlarge", 2.0 * 0.199 - 0.134 - 2.0 * 0.067),
			new Datum(accounts.get(1), Region.US_WEST_2, us_west_2a, Operation.upfrontAmortizedPartial, "c4.xlarge", 0.067),
			new Datum(accounts.get(1), Region.US_WEST_2, us_west_2a, Operation.savingsPartial, "c4.xlarge", 0.199 - 0.067 - 0.067),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "c4");
		
		/* Cost and Usage version */
		usageData = new Datum[]{
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.bonusReservedInstancesPartial, "c4.xlarge", null, "cccccccc-31f5-463a-bc72-b6e53956184f", 1.0),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.bonusReservedInstancesPartial, "c4.xlarge", null, "aaaaaaaa-08c5-4d02-99f3-d23e51968565", 8.0),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2b, ec2Instance, Operation.bonusReservedInstancesPartial, "c4.xlarge", null, "bbbbbbbb-3452-4486-804a-a3d184474ab6", 2.0),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2b, ec2Instance, Operation.bonusReservedInstancesPartial, "c4.xlarge", null, "aaaaaaaa-08c5-4d02-99f3-d23e51968565", 3.0),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2c, ec2Instance, Operation.bonusReservedInstancesPartial, "c4.xlarge", null, "aaaaaaaa-08c5-4d02-99f3-d23e51968565", 4.0),
		};
		expectedCostData = new Datum[]{
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2c, Operation.reservedInstancesPartial, "c4.xlarge", 0.228),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2b, Operation.reservedInstancesPartial, "c4.xlarge", 0.171),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, Operation.reservedInstancesPartial, "c4.xlarge", 0.456),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2b, Operation.borrowedInstancesPartial, "c4.xlarge", 0.134),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, Operation.borrowedInstancesPartial, "c4.xlarge", 0.067),
				new Datum(accounts.get(1), Region.US_WEST_2, us_west_2b, Operation.lentInstancesPartial, "c4.xlarge", 0.134),
				new Datum(accounts.get(1), Region.US_WEST_2, us_west_2a, Operation.lentInstancesPartial, "c4.xlarge", 0.067),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2c, Operation.upfrontAmortizedPartial, "c4.xlarge", 4.0 * 0.0575),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2c, Operation.savingsPartial, "c4.xlarge", 4.0 * (0.199 - 0.0575 - 0.057)),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2b, Operation.upfrontAmortizedPartial, "c4.xlarge", 2.0 * 0.0674 + 3.0 * 0.0575),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2b, Operation.savingsPartial, "c4.xlarge", 2.0 * (0.199 - 0.0674 - 0.067) + 3.0 * (0.199 - 0.0575 - 0.057)),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, Operation.upfrontAmortizedPartial, "c4.xlarge", 0.0674 + 8.0 * 0.0575),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, Operation.savingsPartial, "c4.xlarge", 1.0 * (0.199 - 0.0674 - 0.067) + 8.0 * (0.199 - 0.0575 - 0.057)),
			};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "c4");
		
		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		costData = new Datum[]{
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.bonusReservedInstancesPartial, "c4.xlarge", null, "cccccccc-31f5-463a-bc72-b6e53956184f", 0.067),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.upfrontAmortizedPartial, "c4.xlarge", null, "cccccccc-31f5-463a-bc72-b6e53956184f", 0.0674),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.bonusReservedInstancesPartial, "c4.xlarge", null, "aaaaaaaa-08c5-4d02-99f3-d23e51968565", 8.0 * 0.057),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.upfrontAmortizedPartial, "c4.xlarge", null, "aaaaaaaa-08c5-4d02-99f3-d23e51968565", 8.0 * 0.0575),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2b, ec2Instance, Operation.bonusReservedInstancesPartial, "c4.xlarge", null, "bbbbbbbb-3452-4486-804a-a3d184474ab6", 2.0 * 0.0674),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2b, ec2Instance, Operation.upfrontAmortizedPartial, "c4.xlarge", null, "bbbbbbbb-3452-4486-804a-a3d184474ab6", 2.0 * 0.067),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2b, ec2Instance, Operation.bonusReservedInstancesPartial, "c4.xlarge", null, "aaaaaaaa-08c5-4d02-99f3-d23e51968565", 3.0 * 0.057),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2b, ec2Instance, Operation.upfrontAmortizedPartial, "c4.xlarge", null, "aaaaaaaa-08c5-4d02-99f3-d23e51968565", 3.0 * 0.0575),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2c, ec2Instance, Operation.bonusReservedInstancesPartial, "c4.xlarge", null, "aaaaaaaa-08c5-4d02-99f3-d23e51968565", 4.0 * 0.057),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2c, ec2Instance, Operation.upfrontAmortizedPartial, "c4.xlarge", null, "aaaaaaaa-08c5-4d02-99f3-d23e51968565", 4.0 * 0.0575),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");
	}
	
	/*
	 * Test one AZ scoped full-upfront reservation that's used by the owner and has an additional bonus.
	 */
	@Test
	public void testBonusFixed() throws Exception {
		long startMillis = 1491004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,1464702209129,1496238208000,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		
		Datum[] usageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.bonusReservedInstancesFixed, "m1.large", 2.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.reservedInstancesFixed, "m1.large", 1.0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.bonusReservedInstancesFixed, "m1.large", 1.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.reservedInstancesFixed, "m1.large", 0.0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.bonusReservedInstancesFixed, "m1.large", 0.0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.upfrontAmortizedFixed, "m1.large", 0.095),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.savingsFixed, "m1.large", 0.175 - 0.095),
		};
		
		Set<Account> owners = Sets.newHashSet(accounts.get(0));

		runOneHourTestWithOwners(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1", owners, null);		
	}

	/*
	 * Test one AZ scoped full-upfront reservation that's borrowed by another account and has an additional bonus reservation.
	 */
	@Test
	public void testBonusBorrowedFixed() throws Exception {
		long startMillis = 1491004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"222222222222,EC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,1464702209129,1496238208000,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		
		Datum[] usageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.bonusReservedInstancesFixed, "m1.large", 2.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.borrowedInstancesFixed, "m1.large", 1.0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.bonusReservedInstancesFixed, "m1.large", 1.0),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, Operation.lentInstancesFixed, "m1.large", 1.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.borrowedInstancesFixed, "m1.large", 0.0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.bonusReservedInstancesFixed, "m1.large", 0.0),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, Operation.lentInstancesFixed, "m1.large", 0.0),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, Operation.upfrontAmortizedFixed, "m1.large", 0.095),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, Operation.savingsFixed, "m1.large", 0.175 - 0.095),
		};

		Set<Account> owners = Sets.newHashSet(accounts.get(1));

		runOneHourTestWithOwners(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1", owners, null);		
	}
	
	/*
	 * Test one Region scoped partial-upfront reservation that's used by the owner in a resource Group.
	 */
	@Test
	public void testUsedPartialRegionResourceGroup() throws Exception {
		long startMillis = 1494004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,c4.2xlarge,Region,,false,1493283689633,1524819688000,31536000,0.0,1060.0,2,Linux/UNIX,active,USD,Partial Upfront,Hourly:0.121,,,Foo:Bar",
		};
		
		ResourceGroup rg = ResourceGroup.getResourceGroup("Prod_MyAPI", false);
		Datum[] usageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesPartial, "c4.2xlarge", rg, 1.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.reservedInstancesPartial, "c4.2xlarge", rg, 1.0),
				new Datum(accounts.get(0), Region.US_EAST_1, null, ec2Instance, Operation.unusedInstancesPartial, "c4.2xlarge", ResourceGroup.getResourceGroup(Product.ec2Instance, true), 1.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		ResourceGroup unusedRg = ResourceGroup.getResourceGroup(Product.ec2Instance, true);
		Datum[] expectedCostData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.reservedInstancesPartial, "c4.2xlarge", rg, 0.121),
			new Datum(accounts.get(0), Region.US_EAST_1, null, ec2Instance, Operation.unusedInstancesPartial, "c4.2xlarge", unusedRg, 0.121),
			new Datum(accounts.get(0), Region.US_EAST_1, null, ec2Instance, Operation.upfrontAmortizedPartial, "c4.2xlarge", null, 2 * 0.121),
			new Datum(accounts.get(0), Region.US_EAST_1, null, ec2Instance, Operation.savingsPartial, "c4.2xlarge", null, 0.398 - 2 * (0.121 + 0.121)),
		};

		runOneHourTestWithOwners(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "c4", reservationOwners.keySet(), ec2Instance);
		
		/* Cost and Usage version */
		usageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesPartial, "c4.2xlarge", rg, "2aaaaaaa-bbbb-cccc-ddddddddddddddddd", 1.0),
		};
		expectedCostData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.reservedInstancesPartial, "c4.2xlarge", rg, 0.121),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.upfrontAmortizedPartial, "c4.2xlarge", rg, 0.121),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsPartial, "c4.2xlarge", rg, 0.398 - 0.121 - 0.121),
				new Datum(accounts.get(0), Region.US_EAST_1, null, ec2Instance, Operation.unusedInstancesPartial, "c4.2xlarge", unusedRg, 0.121),
				new Datum(accounts.get(0), Region.US_EAST_1, null, ec2Instance, Operation.upfrontAmortizedPartial, "c4.2xlarge", unusedRg, 0.121),
				new Datum(accounts.get(0), Region.US_EAST_1, null, ec2Instance, Operation.savingsPartial, "c4.2xlarge", unusedRg, -0.121 - 0.121),
			};
		runOneHourTestCostAndUsageWithOwners(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "c4", reservationOwners.keySet(), ec2Instance);		
	}
	
	/*
	 * Test one Region scoped partial-upfront reservation that's used by the owner. Includes a future reservation that isn't relevant.
	 */
	@Test
	public void testUsedPartialRegionWithFutureRegion() throws Exception {
		long startMillis = 1494004800000L; // 2017-05-05T17:20:00Z
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
				"111111111111,EC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,c4.2xlarge,Region,,false,2017-04-27 09:01:29,2018-04-27 09:01:28,31536000,0.0,1060.0,1,Linux/UNIX,active,USD,Partial Upfront,Hourly:0.121",
				"111111111111,EC2,us-east-1,3aaaaaaa-bbbb-cccc-ddddddddddddddddd,,c5.2xlarge,Region,,false,2018-04-27 09:01:29,2019-04-27 09:01:28,31536000,0.0,0.0,1,Linux/UNIX,active,USD,No Upfront,Hourly:0.24",
		};
		
		Datum[] usageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.bonusReservedInstancesPartial, "c4.2xlarge", 1.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.reservedInstancesPartial, "c4.2xlarge", 1.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.reservedInstancesPartial, "c4.2xlarge", 0.121),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.upfrontAmortizedPartial, "c4.2xlarge", 0.121),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.savingsPartial, "c4.2xlarge", 0.398 - 0.121 - 0.121),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "c4");		
	}

	/*
	 * Test Partial Upfront Amortized and Unused costs from CUR line items - first released 2018-01
	 */
	@Test
	public void testPartialUpfrontNetCosts() throws Exception {
		long startMillis = DateTime.parse("2019-01-01").getMillis();
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.small,Region,,false,2018-07-01 00:00:01,2019-07-01 00:00:00,31536000,0.0,123.0,2,Linux/UNIX (Amazon VPC),active,USD,Partial Upfront,Hourly:0.01",
		};
		
		/* One instance used, one unused */
		Datum[] usageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesPartial, "m1.small", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 1.0),
		};
		Datum[] costData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesPartial, "m1.small", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.024),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.upfrontAmortizedPartial, "m1.small", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.014),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsPartial, "m1.small", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.020),
			new Datum(accounts.get(0), Region.US_EAST_1, null, ec2Instance, Operation.upfrontAmortizedPartial, "m1.small", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.014),
			new Datum(accounts.get(0), Region.US_EAST_1, null, ec2Instance, Operation.unusedInstancesPartial, "m1.small", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.010),
		};
		
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.reservedInstancesPartial, "m1.small", 1.0),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.unusedInstancesPartial, "m1.small", 1.0),
		};
		
		Datum[] expectedCostData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.reservedInstancesPartial, "m1.small", 0.024),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.upfrontAmortizedPartial, "m1.small", 0.014),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.upfrontAmortizedPartial, "m1.small", 0.014),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.unusedInstancesPartial, "m1.small", 0.010),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.savingsPartial, "m1.small", 0.020),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.savingsPartial, "m1.small", -(0.010 + 0.014)),
		};
		
		runOneHourTestCostAndUsageWithOwners(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1", reservationOwners.keySet(), null);		
	}
	
	/*
	 * Test No Upfront Unused costs from CUR line items - first released 2018-01
	 */
	@Test
	public void testNoUpfrontNetCosts() throws Exception {
		long startMillis = DateTime.parse("2019-01-01T00:00:00Z").getMillis();
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.small,Region,,false,2018-07-01 00:00:01,2019-07-01 00:00:00,31536000,0.0,0.0,2,Linux/UNIX (Amazon VPC),active,USD,No Upfront,Hourly:0.028",
		};
		
		/* One instance used, one unused */
		Datum[] usageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesHeavy, "m1.small", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 1.0),
		};
		Datum[] costData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesHeavy, "m1.small", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.028),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsHeavy, "m1.small", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.016),
			new Datum(accounts.get(0), Region.US_EAST_1, null, ec2Instance, Operation.unusedInstancesHeavy, "m1.small", null, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd", 0.022),
		};
		
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.reservedInstancesHeavy, "m1.small", 1.0),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.unusedInstancesHeavy, "m1.small", 1.0),
		};
		
		Datum[] expectedCostData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.reservedInstancesHeavy, "m1.small", 0.028),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.unusedInstancesHeavy, "m1.small", 0.022),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, Operation.savingsHeavy, "m1.small", 0.016),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.savingsHeavy, "m1.small", -0.022),
		};
		
		runOneHourTestCostAndUsageWithOwners(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1", reservationOwners.keySet(), null);		
	}
}
