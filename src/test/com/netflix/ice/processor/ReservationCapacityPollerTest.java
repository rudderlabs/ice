package com.netflix.ice.processor;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.amazonaws.services.ec2.model.ReservedInstances;
import com.amazonaws.services.ec2.model.ReservedInstancesId;
import com.amazonaws.services.ec2.model.ReservedInstancesModification;
import com.amazonaws.services.ec2.model.ReservedInstancesModificationResult;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.processor.ReservationCapacityPoller.Ec2Mods;
import com.netflix.ice.processor.ReservationService.ReservationKey;

public class ReservationCapacityPollerTest {
	static Ec2Mods ec2mods;

	@BeforeClass
	public static void initMods() {
		List<ReservedInstancesModification> mods = Lists.newArrayList();
		ReservedInstancesModificationResult[] modificationResults = new ReservedInstancesModificationResult[]{
				new ReservedInstancesModificationResult().withReservedInstancesId("0bd43db3-dd52-4d5f-8770-642d2198ceb9"),
				new ReservedInstancesModificationResult().withReservedInstancesId("c4bcda44-e778-4e4e-aaa9-637fe88d5957"),
				new ReservedInstancesModificationResult().withReservedInstancesId("c5479449-f359-4ed1-8671-32cdc235ea24"),
			
		};
		ReservedInstancesModification mod = new ReservedInstancesModification()
			.withStatus("fulfilled")
			.withReservedInstancesIds(new ReservedInstancesId().withReservedInstancesId("1fcd999c-c669-46bf-9911-8b873f6e09b9"))
			.withModificationResults(modificationResults);
		
		mods.add(mod);
		ec2mods = new ReservationCapacityPoller().new Ec2Mods(mods);
	}
	
	@Test
	public void testEc2Mods() {
		String parentId = ec2mods.getModResId("0bd43db3-dd52-4d5f-8770-642d2198ceb9");
		assertTrue("Didn't find parent reservation", parentId.equals("1fcd999c-c669-46bf-9911-8b873f6e09b9"));		
	}
	
	@Test
	public void testHandleEC2Modifications() {
		ReservedInstances parentRI = new ReservedInstances()
		.withReservedInstancesId("1fcd999c-c669-46bf-9911-8b873f6e09b9")
		.withInstanceType("c4.2xlarge")
		.withInstanceCount(1)
		.withFixedPrice((float) 100.0)
		.withUsagePrice((float) 0.0)
		.withOfferingType("Partial Upfront");
	
		ReservedInstances childRI = new ReservedInstances()
		.withReservedInstancesId("0bd43db3-dd52-4d5f-8770-642d2198ceb9")
		.withInstanceType("c4.xlarge")
		.withInstanceCount(2)
		.withFixedPrice((float) 0.0)
		.withUsagePrice((float) 0.0)
		.withOfferingType("Partial Upfront");

	
		Map<ReservationKey, CanonicalReservedInstances> reservations = Maps.newHashMap();
		
		ReservationKey parentKey = new ReservationKey("1", "us-east-1", "1fcd999c-c669-46bf-9911-8b873f6e09b9");
		ReservationKey childKey = new ReservationKey("1", "us-east-1", "0bd43db3-dd52-4d5f-8770-642d2198ceb9");
		reservations.put(parentKey, new CanonicalReservedInstances("1", "us-east-1", parentRI, ""));
		reservations.put(childKey, new CanonicalReservedInstances("1", "us-east-1", childRI, ""));
		ReservationCapacityPoller poller = new ReservationCapacityPoller();
		poller.handleEC2Modifications(reservations, ec2mods);
		
		CanonicalReservedInstances child = reservations.get(childKey);
		assertEquals("Wrong fixed price, expected " + 100.0 + ", got " + child.getFixedPrice(), child.getFixedPrice(), 50.0, 0.001);		
	}
	
	@Test
	public void testMultiplier() {
		ReservationCapacityPoller rcp = new ReservationCapacityPoller();
		assertEquals("Wrong multipliers converting micro to xlarge", rcp.multiplier("xlarge") / rcp.multiplier("micro"), 16.0, 0.001);
		assertEquals("Wrong multipliers converting small to 4xlarge", rcp.multiplier("4xlarge") / rcp.multiplier("small"), 32.0, 0.001);
	}
}
