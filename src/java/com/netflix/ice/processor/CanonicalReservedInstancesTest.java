package com.netflix.ice.processor;

import static org.junit.Assert.*;

import java.util.List;

import org.joda.time.DateTime;
import org.junit.Test;

import com.netflix.ice.processor.CanonicalReservedInstances.RecurringCharge;

public class CanonicalReservedInstancesTest {
	// test values
	private static final String account = "111111111111";
	private static final String product = "EC2";
	private static final String region = "us-east-1";
	private static final String reservationId = "aaaaaaaa-bbbb-cccc-ddddddddddddddddd";
	private static final String offeringId = "eeee-fffffff-gggggg-hhhhhhhh";
	private static final String instanceType = "m1.large";
	private static final String scope = "Availability Zone";
	private static final String zone = "us-east-1a";
	private static final String multiAZ = "false";
	private static final String start = "2017-06-09 21:21:37";
	private static final String end = "2018-06-09 21:21:36";
	private static final String duration = "31536000";
	private static final String usagePrice = "10.00";
	private static final String fixedPrice = "20.00";
	private static final String instanceCount = "5";
	private static final String description = "Linux/UNIX (Amazon VPC)";
	private static final String state = "active";
	private static final String currency = "USD";
	private static final String offeringType = "All Upfront";
	private static final String recurringCharges = "hourly:1.0|monthly:30.0";
	private static final String parentReservationId = "bbbbbbbb-cccc-cccc-ddddddddddddddddd";
	
	
	private void compareValues(CanonicalReservedInstances cri, int numRecurring) {
		assertTrue("Account doesn't match", account.equals(cri.getAccountId()));
		assertTrue("Region doesn't match", region.equals(cri.getRegion()));
		assertTrue("Product doesn't match", product.equals(cri.getProduct()));
		assertTrue("ReservationID doesn't match", reservationId.equals(cri.getReservationId()));
		assertTrue("OfferingId doesn't match", offeringId.equals(cri.getReservationOfferingId()));
		assertTrue("InstanceType doesn't match", instanceType.equals(cri.getInstanceType()));
		assertTrue("Scope doesn't match", scope.equals(cri.getScope()));
		assertTrue("Zone doesn't match", zone.equals(cri.getAvailabilityZone()));
		assertTrue("MultiAZ doesn't match", multiAZ.equals(cri.getMultiAZ().toString()));
		String s = LineItemProcessor.amazonBillingDateFormat.print(new DateTime(cri.getStart().getTime()));
		assertTrue("Start doesn't match", start.equals(s));
		String e = LineItemProcessor.amazonBillingDateFormat.print(new DateTime(cri.getEnd().getTime()));
		assertTrue("End doesn't match", end.equals(e));
		assertTrue("Duration doesn't match", duration.equals(cri.getDuration().toString()));
		assertTrue("UsagePrice doesn't match", Double.parseDouble(usagePrice) - cri.getUsagePrice() < 0.001);
		assertTrue("FixedPrice doesn't match", Double.parseDouble(fixedPrice) - cri.getFixedPrice() < 0.001);
		assertTrue("InstanceCount doesn't match", instanceCount.equals(cri.getInstanceCount().toString()));
		assertTrue("Description doesn't match", description.equals(cri.getProductDescription()));
		assertTrue("State doesn't match", state.equals(cri.getState()));
		assertTrue("Currency doesn't match", currency.equals(cri.getCurrencyCode()));
		assertTrue("OfferingType doesn't match", offeringType.equals(cri.getOfferingType()));
		List<RecurringCharge> rcsA = cri.getRecurringCharges();
		String[] rcsB = recurringCharges.split("\\|");
		assertTrue("Number of recurring charges is wrong", rcsA.size() == numRecurring);
		for (int i = 0; i < numRecurring; i++) {
			String[] rcB = rcsB[i].split(":");
			assertTrue("Recurrence frequency for index " + i + " doesn't match", rcB[0].equals(rcsA.get(i).frequency));
			assertTrue("Recurrence cost for index " + i + " doesn't match", Double.parseDouble(rcB[1]) - rcsA.get(i).cost < 0.001);
		}
		assertTrue("ParentReservationId doesn't match", parentReservationId.equals(cri.getParentReservationId()));
	}

	@Test
	public void testCSVConstructor() {
		String testRes = account + "," + product + "," + region + "," + reservationId + "," + offeringId + "," + instanceType + "," + scope + "," +
				zone + "," + multiAZ + "," + start + "," + end + "," + duration + "," + usagePrice + "," + fixedPrice + "," + instanceCount + "," +
				description + "," + state + "," + currency + "," + offeringType + "," + recurringCharges + "," + parentReservationId;
		CanonicalReservedInstances cri = new CanonicalReservedInstances(testRes);
		compareValues(cri, 2);
	}
	
	@Test
	public void testCSVConstructorNoRecurring() {
		String testRes = account + "," + product + "," + region + "," + reservationId + "," + offeringId + "," + instanceType + "," + scope + "," +
				zone + "," + multiAZ + "," + start + "," + end + "," + duration + "," + usagePrice + "," + fixedPrice + "," + instanceCount + "," +
				description + "," + state + "," + currency + "," + offeringType + ",," + parentReservationId;
		CanonicalReservedInstances cri = new CanonicalReservedInstances(testRes);
		compareValues(cri, 0);
	}
}
