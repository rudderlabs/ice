package com.netflix.ice.basic;

import static org.junit.Assert.*;

import org.junit.Test;

import com.netflix.ice.basic.BasicLineItemProcessor.ReformedMetaData;
import com.netflix.ice.processor.Ec2InstanceReservationPrice.ReservationUtilization;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;

public class BasicLineItemProcessorTest {

	@Test
	public void testReform() {
		BasicLineItemProcessor processor = new BasicLineItemProcessor();
	    ReformedMetaData rmd = processor.reform(ReservationUtilization.HEAVY, Product.ec2, false, "RunInstances:SV001", "USW2-SpotUsage:c4.large", "c4.large Linux/UNIX Spot Instance-hour in US West (Oregon) in VPC Zone #1", 0.02410000);
	    assertTrue("Operation should be spot instance but got " + rmd.operation, rmd.operation == Operation.spotInstances);
	}

}
