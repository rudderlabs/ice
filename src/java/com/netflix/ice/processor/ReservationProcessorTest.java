package com.netflix.ice.processor;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.ivy.util.StringUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicReservationService;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.Ec2InstanceReservationPrice.ReservationPeriod;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.Zone;

public class ReservationProcessorTest {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    // reservationAccounts is a cross-linked list of accounts where each account
	// can borrow reservations from any other.
	private static Map<Account, List<Account>> payerAccounts = Maps.newHashMap();
	private static Map<Account, Set<String>> reservationOwners = Maps.newHashMap();
	
	private static final int numAccounts = 3;
	private static List<Account> accounts = Lists.newArrayList();
	static {
		// Auto-populate the accounts list based on numAccounts
		for (Integer i = 1; i <= numAccounts; i++) {
			// Create accounts of the form Account("111111111111", "Account1")
			accounts.add(new Account(StringUtils.repeat(i.toString(), 12), "Account" + i.toString()));
		}
		// Populate the payerAccounts map - first account is payer, others are linked
		// Every account is a reservation owner for these tests
		Set<String> products = Sets.newHashSet("ec2", "rds", "redshift");
		reservationOwners.put(accounts.get(0), products);
        List<Account> linked = Lists.newArrayList();
		for (int i = 1; i < numAccounts; i++) {
			linked.add(accounts.get(i));
			reservationOwners.put(accounts.get(i), products);
		}
    	payerAccounts.put(accounts.get(0), linked);
	}

	@Test
	public void testConstructor() {
		assertEquals("Number of accounts should be " + numAccounts, numAccounts, accounts.size());
		ReservationProcessor rp = new ReservationProcessor(payerAccounts, reservationOwners.keySet());
		assertNotNull("Contructor returned null", rp);
	}
	
	public static class Datum {
		public TagGroup tagGroup;
		public double value;
		
		public Datum(TagGroup tagGroup, double value)
		{
			this.tagGroup = tagGroup;
			this.value = value;
		}
		
		public Datum(Account account, Region region, Zone zone, Operation operation, String usageType, double value)
		{
			this.tagGroup = new TagGroup(account, region, zone, Product.ec2_instance, operation, UsageType.getUsageType(usageType, "hours"), null);
			this.value = value;
		}
		
		public Datum(Account account, Region region, Zone zone, Product product, Operation operation, String usageType, double value)
		{
			this.tagGroup = new TagGroup(account, region, zone, product, operation, UsageType.getUsageType(usageType, "hours"), null);
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
	private void runOneHourTest(long startMillis, String[] reservationsCSV, Datum[] usageData, Datum[] costData, Datum[] expectedUsage, Datum[] expectedCost, String debugFamily) {
		runOneHourTestWithOwners(startMillis, reservationsCSV, usageData, costData, expectedUsage, expectedCost, debugFamily, reservationOwners.keySet());
	}
	
	private void runOneHourTestWithOwners(
			long startMillis, 
			String[] reservationsCSV, 
			Datum[] usageData, 
			Datum[] costData, 
			Datum[] expectedUsage, 
			Datum[] expectedCost, 
			String debugFamily,
			Set<Account> rsvOwners) {
		
		Map<TagGroup, Double> hourUsageData = makeDataMap(usageData);
		Map<TagGroup, Double> hourCostData = makeDataMap(costData);

		List<Map<TagGroup, Double>> ud = new ArrayList<Map<TagGroup, Double>>();
		ud.add(hourUsageData);
		ReadWriteData usage = new ReadWriteData();
		usage.setData(ud, 0, false);
		List<Map<TagGroup, Double>> cd = new ArrayList<Map<TagGroup, Double>>();
		cd.add(hourCostData);
		ReadWriteData cost = new ReadWriteData();
		cost.setData(cd, 0, false);

		runTest(startMillis, reservationsCSV, usage, cost, debugFamily, rsvOwners);

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
	
	private void runTest(long startMillis, String[] reservationsCSV, ReadWriteData usage, ReadWriteData cost, String debugFamily, Set<Account> rsvOwners) {
		Map<String, CanonicalReservedInstances> reservations = Maps.newHashMap();
		for (String res: reservationsCSV) {
			String[] fields = res.split(",");
			reservations.put(fields[0]+","+fields[2]+","+fields[3], new CanonicalReservedInstances(res));
		}
		
		AccountService accountService = new BasicAccountService(accounts, payerAccounts, reservationOwners, null, null);
		
		BasicReservationService reservationService = new BasicReservationService(ReservationPeriod.oneyear, Ec2InstanceReservationPrice.ReservationUtilization.FIXED);
		reservationService.updateReservations(reservations, accountService, startMillis);		

		ReservationProcessor rp = new ReservationProcessor(payerAccounts, rsvOwners);
		rp.setDebugHour(0);
		rp.setDebugFamily(debugFamily);
		rp.process(Ec2InstanceReservationPrice.ReservationUtilization.HEAVY, reservationService, usage, cost, startMillis);
		rp.process(Ec2InstanceReservationPrice.ReservationUtilization.HEAVY_PARTIAL, reservationService, usage, cost, startMillis);
		rp.process(Ec2InstanceReservationPrice.ReservationUtilization.FIXED, reservationService, usage, cost, startMillis);		
	}
	
	/*
	 * Test one AZ scoped full-upfront reservation that's used by the owner.
	 */
	@Test
	public void testUsedFixedAZ() {
		long startMillis = 1491004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,1464702209129,1496238208000,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		
		Datum[] usageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.bonusReservedInstancesFixed, "m1.large", 1.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.reservedInstancesFixed, "m1.large", 1.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.reservedInstancesFixed, "m1.large", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.upfrontAmortizedFixed, "m1.large", 0.095),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");		
	}

	/*
	 * Test one AZ scoped full-upfront reservation that isn't used.
	 */
	@Test
	public void testUnusedFixedAZ() {
		long startMillis = 1491004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,1464702209129,1496238208000,31536000,0.0,835.0,14,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		
		Datum[] usageData = new Datum[]{
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.unusedInstancesFixed, "m1.large", 14.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.unusedInstancesFixed, "m1.large", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.upfrontAmortizedFixed, "m1.large", 1.3345),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");		
	}

	/*
	 * Test two AZ scoped reservations - one HEAVY and one FIXED that are both used by the owner account.
	 */
	@Test
	public void testTwoUsedHeavyFixedAZ() {
		long startMillis = 1491004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,1464702209129,1496238208000,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
			"111111111111,EC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,1464702209129,1496238208000,31536000,0.0,0.0,1,Linux/UNIX (Amazon VPC),active,USD,No Upfront,Hourly:0.112",
		};
		
		Datum[] usageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.bonusReservedInstancesFixed, "m1.large", 1.0),
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.bonusReservedInstancesHeavy, "m1.large", 1.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.reservedInstancesFixed, "m1.large", 1.0),
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.reservedInstancesHeavy, "m1.large", 1.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.reservedInstancesHeavy, "m1.large", 0.112),
				new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.reservedInstancesFixed, "m1.large", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.upfrontAmortizedFixed, "m1.large", 0.095),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");		
	}


	/*
	 * Test two equivalent AZ scoped full-upfront reservation that are both used by the owner account.
	 */
	@Test
	public void testTwoUsedSameFixedAZ() {
		long startMillis = 1491004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,1464702209129,1496238208000,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
			"111111111111,EC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,1464702209129,1496238208000,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		
		Datum[] usageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.bonusReservedInstancesFixed, "m1.large", 2.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.reservedInstancesFixed, "m1.large", 2.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.reservedInstancesFixed, "m1.large", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.upfrontAmortizedFixed, "m1.large", 0.190),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");		
	}

	/*
	 * Test one AZ scoped full-upfront reservations where one instance is used by the owner account and one borrowed by a second account. Three instances are unused.
	 */
	@Test
	public void testFixedAZ() {
		long startMillis = 1491004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.small,Availability Zone,us-east-1a,false,1464699998099,1496235997000,31536000,0.0,206.0,5,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		
		Datum[] usageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.bonusReservedInstancesFixed, "m1.small", 1.0),
			new Datum(accounts.get(1), Region.US_EAST_1, Zone.US_EAST_1A, Operation.bonusReservedInstancesFixed, "m1.small", 1.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.reservedInstancesFixed, "m1.small", 1.0),
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.unusedInstancesFixed, "m1.small", 3.0),
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.lentInstancesFixed, "m1.small", 1.0),
			new Datum(accounts.get(1), Region.US_EAST_1, Zone.US_EAST_1A, Operation.borrowedInstancesFixed, "m1.small", 1.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.reservedInstancesFixed, "m1.small", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.lentInstancesFixed, "m1.small", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.unusedInstancesFixed, "m1.small", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.upfrontAmortizedFixed, "m1.small", 0.1176),
			new Datum(accounts.get(1), Region.US_EAST_1, Zone.US_EAST_1A, Operation.borrowedInstancesFixed, "m1.small", 0.0),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");		
	}

	/*
	 * Test one Region scoped full-upfront reservation where one instance is used by the owner account in each of several AZs.
	 */
	@Test
	public void testFixedRegionalMultiAZ() {
		long startMillis = 1491004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.small,Region,,false,1464699998099,1496235997000,31536000,0.0,206.0,5,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		
		Datum[] usageData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.bonusReservedInstancesFixed, "m1.small", 1.0),
				new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1B, Operation.bonusReservedInstancesFixed, "m1.small", 1.0),
				new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1C, Operation.bonusReservedInstancesFixed, "m1.small", 1.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.reservedInstancesFixed, "m1.small", 1.0),
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1B, Operation.reservedInstancesFixed, "m1.small", 1.0),
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1C, Operation.reservedInstancesFixed, "m1.small", 1.0),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.unusedInstancesFixed, "m1.small", 2.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.reservedInstancesFixed, "m1.small", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1B, Operation.reservedInstancesFixed, "m1.small", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1C, Operation.reservedInstancesFixed, "m1.small", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.unusedInstancesFixed, "m1.small", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.upfrontAmortizedFixed, "m1.small", 0.1176),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");		
	}
	
	/*
	 * Test two full-upfront reservations - one AZ, one Region that are both used by the owner account.
	 */
	@Test
	public void testTwoUsedOneAZOneRegion() {
		long startMillis = 1491004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,1464702209129,1496238208000,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
			"111111111111,EC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Region,,false,1464702209129,1496238208000,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		
		Datum[] usageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.bonusReservedInstancesFixed, "m1.large", 2.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.reservedInstancesFixed, "m1.large", 2.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.reservedInstancesFixed, "m1.large", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.upfrontAmortizedFixed, "m1.large", 0.095),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.upfrontAmortizedFixed, "m1.large", 0.095),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");		
	}

	/*
	 * Test two full-upfront reservations - one AZ, one Region that are both used by a borrowing account.
	 */
	@Test
	public void testTwoUsedOneAZOneRegionBorrowed() {
		long startMillis = 1491004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,1464702209129,1496238208000,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
			"111111111111,EC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Region,,false,1464702209129,1496238208000,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		
		Datum[] usageData = new Datum[]{
				new Datum(accounts.get(1), Region.US_EAST_1, Zone.US_EAST_1A, Operation.bonusReservedInstancesFixed, "m1.large", 1.0),
				new Datum(accounts.get(1), Region.US_EAST_1, Zone.US_EAST_1B, Operation.bonusReservedInstancesFixed, "m1.large", 1.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.lentInstancesFixed, "m1.large", 1.0),
			new Datum(accounts.get(1), Region.US_EAST_1, Zone.US_EAST_1A, Operation.borrowedInstancesFixed, "m1.large", 1.0),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.lentInstancesFixed, "m1.large", 1.0),
			new Datum(accounts.get(1), Region.US_EAST_1, Zone.US_EAST_1B, Operation.borrowedInstancesFixed, "m1.large", 1.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.lentInstancesFixed, "m1.large", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.lentInstancesFixed, "m1.large", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.upfrontAmortizedFixed, "m1.large", 0.095),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.upfrontAmortizedFixed, "m1.large", 0.095),
			new Datum(accounts.get(1), Region.US_EAST_1, Zone.US_EAST_1A, Operation.borrowedInstancesFixed, "m1.large", 0.0),
			new Datum(accounts.get(1), Region.US_EAST_1, Zone.US_EAST_1B, Operation.borrowedInstancesFixed, "m1.large", 0.0),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");		
	}

	/*
	 * Test one Region scoped full-upfront reservation where four small instance reservations are used by one large instance in the owner account.
	 */
	@Test
	public void testFixedRegionalFamily() {
		long startMillis = 1491004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.small,Region,,false,1464699998099,1496235997000,31536000,0.0,206.0,4,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		
		Datum[] usageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.bonusReservedInstancesFixed, "m1.large", 1.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.familyReservedInstancesFixed, "m1.large", 1.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.familyReservedInstancesFixed, "m1.large", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.upfrontAmortizedFixed, "m1.small", 0.094),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");		
	}

	/*
	 * Test one Region scoped full-upfront reservation where one instance is used by the owner account and one borrowed by a second account.
	 */
	@Test
	public void testFixedRegional() {
		long startMillis = 1491004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.small,Region,,false,1464699998099,1496235997000,31536000,0.0,206.0,5,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		
		Datum[] usageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.bonusReservedInstancesFixed, "m1.small", 1.0),
			new Datum(accounts.get(1), Region.US_EAST_1, Zone.US_EAST_1A, Operation.bonusReservedInstancesFixed, "m1.small", 1.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.reservedInstancesFixed, "m1.small", 1.0),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.unusedInstancesFixed, "m1.small", 3.0),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.lentInstancesFixed, "m1.small", 1.0),
			new Datum(accounts.get(1), Region.US_EAST_1, Zone.US_EAST_1A, Operation.borrowedInstancesFixed, "m1.small", 1.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.reservedInstancesFixed, "m1.small", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.upfrontAmortizedFixed, "m1.small", 0.1176),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.unusedInstancesFixed, "m1.small", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.lentInstancesFixed, "m1.small", 0.0),
			new Datum(accounts.get(1), Region.US_EAST_1, Zone.US_EAST_1A, Operation.borrowedInstancesFixed, "m1.small", 0.0),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");		
	}

	/*
	 * Test two Region scoped full-upfront reservations where one instance from each is family borrowed by a third account.
	 */
	@Test
	public void testFixedTwoRegionalFamilyBorrowed() {
		long startMillis = 1491004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
				"111111111111,EC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.small,Region,,false,1464699998099,1496235997000,31536000,0.0,206.0,4,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
				"222222222222,EC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.small,Region,,false,1464699998099,1496235997000,31536000,0.0,206.0,4,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		
		Datum[] usageData = new Datum[]{
			new Datum(accounts.get(2), Region.US_EAST_1, Zone.US_EAST_1A, Operation.bonusReservedInstancesFixed, "m1.xlarge", 1.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.lentInstancesFixed, "m1.small", 4.0),
			new Datum(accounts.get(1), Region.US_EAST_1, null, Operation.lentInstancesFixed, "m1.small", 4.0),
			new Datum(accounts.get(2), Region.US_EAST_1, Zone.US_EAST_1A, Operation.borrowedInstancesFixed, "m1.xlarge", 1.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.upfrontAmortizedFixed, "m1.small", 0.094),
				new Datum(accounts.get(1), Region.US_EAST_1, null, Operation.upfrontAmortizedFixed, "m1.small", 0.094),
				new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.lentInstancesFixed, "m1.small", 0.0),
				new Datum(accounts.get(1), Region.US_EAST_1, null, Operation.lentInstancesFixed, "m1.small", 0.0),
				new Datum(accounts.get(2), Region.US_EAST_1, Zone.US_EAST_1A, Operation.borrowedInstancesFixed, "m1.xlarge", 0.0),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1");		
	}
	
	/*
	 * Test one Region scoped full-upfront RDS reservation where the instance is used.
	 */
	@Test
	public void testFixedRDS() {
		long startMillis = 1491004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,RDS,us-east-1,ri-2016-05-20-16-50-03-197,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,db.t2.small,,,false,1463763023778,1495299023778,31536000,0.0,195.0,1,mysql,active,USD,All Upfront,",
		};
		
		Datum[] usageData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, null, Product.rds_instance, Operation.bonusReservedInstancesFixed, "db.t2.small.mysql", 1.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, null, Product.rds_instance, Operation.reservedInstancesFixed, "db.t2.small.mysql", 1.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, null, Product.rds_instance, Operation.reservedInstancesFixed, "db.t2.small.mysql", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Product.rds_instance, Operation.upfrontAmortizedFixed, "db.t2.small.mysql", 0.0223),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "db");
		
	}
	
	/*
	 * Test one Region scoped full-upfront RDS reservation where the instance is used.
	 */
	@Test
	public void testPartialRDS() {
		long startMillis = 1491004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,RDS,ap-southeast-2,ri-2017-02-01-06-08-23-918,573d345b-7d5d-42eb-a340-5c19bf82b338,db.t2.micro,,,false,1485929307960,1517465307960,31536000,0.0,79.0,2,postgresql,active,USD,Partial Upfront,Hourly:0.012",
		};
		
		Datum[] usageData = new Datum[]{
				new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, null, Product.rds_instance, Operation.bonusReservedInstancesHeavyPartial, "db.t2.micro.postgresql", 2.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, null, Product.rds_instance, Operation.reservedInstancesHeavyPartial, "db.t2.micro.postgresql", 2.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
			new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, null, Product.rds_instance, Operation.reservedInstancesHeavyPartial, "db.t2.micro.postgresql", 0.024),
			new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, null, Product.rds_instance, Operation.upfrontAmortizedHeavyPartial, "db.t2.micro.postgresql", 0.018),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "db");
		
	}
	
	/*
	 * Test one Region scoped partial-upfront reservation that's used by the owner.
	 */
	@Test
	public void testUsedPartialRegion() {
		long startMillis = 1494004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,c4.2xlarge,Region,,false,1493283689633,1524819688000,31536000,0.0,1060.0,1,Linux/UNIX,active,USD,Partial Upfront,Hourly:0.121",
		};
		
		Datum[] usageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.bonusReservedInstancesHeavyPartial, "c4.2xlarge", 1.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.reservedInstancesHeavyPartial, "c4.2xlarge", 1.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.reservedInstancesHeavyPartial, "c4.2xlarge", 0.121),
			new Datum(accounts.get(0), Region.US_EAST_1, null, Operation.upfrontAmortizedHeavyPartial, "c4.2xlarge", 0.121),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "c4");		
	}

	@Test
	public void testUsedAndBorrowedPartialRegion() {
		long startMillis = 1494004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,us-west-2,e098729a-588b-46a2-8c05-cbcf87aed53d,,c3.4xlarge,Region,,false,1492041221000,1523577220000,31536000,0.0,2477.60009765625,1,Linux/UNIX,active,USD,Partial Upfront,Hourly:0.19855",
			"222222222222,EC2,us-west-2,8c587942-1942-4e5e-892b-cec03ddb7816,,c3.4xlarge,Region,,false,1475509708914,1507045707000,31536000,0.0,2608.0,1,Linux/UNIX (Amazon VPC),active,USD,Partial Upfront,Hourly:0.209"
		};
		
		Datum[] usageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_WEST_2, Zone.US_WEST_2A, Operation.bonusReservedInstancesHeavyPartial, "c3.4xlarge", 2.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_WEST_2, Zone.US_WEST_2A, Operation.reservedInstancesHeavyPartial, "c3.4xlarge", 1.0),
			new Datum(accounts.get(0), Region.US_WEST_2, Zone.US_WEST_2A, Operation.borrowedInstancesHeavyPartial, "c3.4xlarge", 1.0),
			new Datum(accounts.get(1), Region.US_WEST_2, null, Operation.lentInstancesHeavyPartial, "c3.4xlarge", 1.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
			new Datum(accounts.get(0), Region.US_WEST_2, Zone.US_WEST_2A, Operation.reservedInstancesHeavyPartial, "c3.4xlarge", 0.199),
			new Datum(accounts.get(0), Region.US_WEST_2, Zone.US_WEST_2A, Operation.borrowedInstancesHeavyPartial, "c3.4xlarge", 0.209),
			new Datum(accounts.get(1), Region.US_WEST_2, null, Operation.lentInstancesHeavyPartial, "c3.4xlarge", 0.209),
			new Datum(accounts.get(0), Region.US_WEST_2, null, Operation.upfrontAmortizedHeavyPartial, "c3.4xlarge", 0.283),
			new Datum(accounts.get(1), Region.US_WEST_2, null, Operation.upfrontAmortizedHeavyPartial, "c3.4xlarge", 0.298),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "c3");
	}
	
	@Test
	public void testUsedAndBorrowedPartialRegionAndAZ() {
		long startMillis = 1494004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,us-west-2,2e73d4b7-08c5-4d02-99f3-d23e51968565,,c4.xlarge,Region,,false,1492033482000,1523569481000,31536000,0.0,503.5,15,Linux/UNIX,active,USD,Partial Upfront,Hourly:0.057",
			"222222222222,EC2,us-west-2,46852280-3452-4486-804a-a3d184474ab6,,c4.xlarge,Availability Zone,us-west-2b,false,1474587867448,1506123866000,31536000,0.0,590.0,2,Linux/UNIX (Amazon VPC),active,USD,Partial Upfront,Hourly:0.067",
			"222222222222,EC2,us-west-2,b6876c3a-31f5-463a-bc72-b6e53956184f,,c4.xlarge,Availability Zone,us-west-2a,false,1474587867022,1506123866000,31536000,0.0,590.0,1,Linux/UNIX (Amazon VPC),active,USD,Partial Upfront,Hourly:0.067",
		};
		
		Datum[] usageData = new Datum[]{
				new Datum(accounts.get(0), Region.US_WEST_2, Zone.US_WEST_2A, Operation.bonusReservedInstancesHeavyPartial, "c4.xlarge", 9.0),
				new Datum(accounts.get(0), Region.US_WEST_2, Zone.US_WEST_2B, Operation.bonusReservedInstancesHeavyPartial, "c4.xlarge", 5.0),
				new Datum(accounts.get(0), Region.US_WEST_2, Zone.US_WEST_2C, Operation.bonusReservedInstancesHeavyPartial, "c4.xlarge", 4.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_WEST_2, Zone.US_WEST_2C, Operation.reservedInstancesHeavyPartial, "c4.xlarge", 4.0),
			new Datum(accounts.get(0), Region.US_WEST_2, Zone.US_WEST_2B, Operation.reservedInstancesHeavyPartial, "c4.xlarge", 3.0),
			new Datum(accounts.get(0), Region.US_WEST_2, Zone.US_WEST_2B, Operation.borrowedInstancesHeavyPartial, "c4.xlarge", 2.0),
			new Datum(accounts.get(0), Region.US_WEST_2, Zone.US_WEST_2A, Operation.reservedInstancesHeavyPartial, "c4.xlarge", 8.0),
			new Datum(accounts.get(0), Region.US_WEST_2, Zone.US_WEST_2A, Operation.borrowedInstancesHeavyPartial, "c4.xlarge", 1.0),
			new Datum(accounts.get(1), Region.US_WEST_2, Zone.US_WEST_2B, Operation.lentInstancesHeavyPartial, "c4.xlarge", 2.0),
			new Datum(accounts.get(1), Region.US_WEST_2, Zone.US_WEST_2A, Operation.lentInstancesHeavyPartial, "c4.xlarge", 1.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
				new Datum(accounts.get(0), Region.US_WEST_2, Zone.US_WEST_2C, Operation.reservedInstancesHeavyPartial, "c4.xlarge", 0.228),
				new Datum(accounts.get(0), Region.US_WEST_2, Zone.US_WEST_2B, Operation.reservedInstancesHeavyPartial, "c4.xlarge", 0.171),
				new Datum(accounts.get(0), Region.US_WEST_2, Zone.US_WEST_2A, Operation.reservedInstancesHeavyPartial, "c4.xlarge", 0.456),
				new Datum(accounts.get(0), Region.US_WEST_2, Zone.US_WEST_2B, Operation.borrowedInstancesHeavyPartial, "c4.xlarge", 0.134),
				new Datum(accounts.get(0), Region.US_WEST_2, Zone.US_WEST_2A, Operation.borrowedInstancesHeavyPartial, "c4.xlarge", 0.067),
				new Datum(accounts.get(1), Region.US_WEST_2, Zone.US_WEST_2B, Operation.lentInstancesHeavyPartial, "c4.xlarge", 0.134),
				new Datum(accounts.get(1), Region.US_WEST_2, Zone.US_WEST_2A, Operation.lentInstancesHeavyPartial, "c4.xlarge", 0.067),
			new Datum(accounts.get(0), Region.US_WEST_2, null, Operation.upfrontAmortizedHeavyPartial, "c4.xlarge", 0.862),
			new Datum(accounts.get(1), Region.US_WEST_2, Zone.US_WEST_2B, Operation.upfrontAmortizedHeavyPartial, "c4.xlarge", 0.134),
			new Datum(accounts.get(1), Region.US_WEST_2, Zone.US_WEST_2A, Operation.upfrontAmortizedHeavyPartial, "c4.xlarge", 0.067),
		};

		runOneHourTest(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "c4");
	}
	
	/*
	 * Test one AZ scoped full-upfront reservation that's used by the owner.
	 */
	@Test
	public void testBonusFixed() {
		long startMillis = 1491004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,EC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,1464702209129,1496238208000,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		
		Datum[] usageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.bonusReservedInstancesFixed, "m1.large", 2.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.reservedInstancesFixed, "m1.large", 1.0),
				new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.bonusReservedInstancesFixed, "m1.large", 1.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.reservedInstancesFixed, "m1.large", 0.0),
				new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.bonusReservedInstancesFixed, "m1.large", 0.0),
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.upfrontAmortizedFixed, "m1.large", 0.095),
		};
		
		Set<Account> owners = Sets.newHashSet(accounts.get(0));

		runOneHourTestWithOwners(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1", owners);		
	}

	/*
	 * Test one AZ scoped full-upfront reservation that's used by the owner.
	 */
	@Test
	public void testBonusBorrowedFixed() {
		long startMillis = 1491004800000L;
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"222222222222,EC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,1464702209129,1496238208000,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		
		Datum[] usageData = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.bonusReservedInstancesFixed, "m1.large", 2.0),
		};
				
		Datum[] expectedUsageData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.borrowedInstancesFixed, "m1.large", 1.0),
				new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.bonusReservedInstancesFixed, "m1.large", 1.0),
				new Datum(accounts.get(1), Region.US_EAST_1, Zone.US_EAST_1A, Operation.lentInstancesFixed, "m1.large", 1.0),
		};
		
		Datum[] costData = new Datum[]{				
		};
		Datum[] expectedCostData = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.borrowedInstancesFixed, "m1.large", 0.0),
				new Datum(accounts.get(0), Region.US_EAST_1, Zone.US_EAST_1A, Operation.bonusReservedInstancesFixed, "m1.large", 0.0),
				new Datum(accounts.get(1), Region.US_EAST_1, Zone.US_EAST_1A, Operation.lentInstancesFixed, "m1.large", 0.0),
			new Datum(accounts.get(1), Region.US_EAST_1, Zone.US_EAST_1A, Operation.upfrontAmortizedFixed, "m1.large", 0.095),
		};

		Set<Account> owners = Sets.newHashSet(accounts.get(1));

		runOneHourTestWithOwners(startMillis, resCSV, usageData, costData, expectedUsageData, expectedCostData, "m1", owners);		
	}

}
