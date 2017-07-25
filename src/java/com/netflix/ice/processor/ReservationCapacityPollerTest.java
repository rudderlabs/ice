package com.netflix.ice.processor;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import com.amazonaws.services.ec2.model.ReservedInstancesId;
import com.amazonaws.services.ec2.model.ReservedInstancesModification;
import com.amazonaws.services.ec2.model.ReservedInstancesModificationResult;
import com.google.common.collect.Lists;
import com.netflix.ice.processor.ReservationCapacityPoller.Ec2Mods;

public class ReservationCapacityPollerTest {

	@Test
	public void testEc2Mods() {
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

		Ec2Mods ec2mods = new ReservationCapacityPoller().new Ec2Mods(mods);
		String parentId = ec2mods.getModResId("0bd43db3-dd52-4d5f-8770-642d2198ceb9");
		assertTrue("Didn't find parent reservation", parentId.equals("1fcd999c-c669-46bf-9911-8b873f6e09b9"));
		
	}

}
