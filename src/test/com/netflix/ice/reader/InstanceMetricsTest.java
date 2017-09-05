package com.netflix.ice.reader;

import static org.junit.Assert.*;

import org.junit.Test;

import com.netflix.ice.tag.UsageType;

public class InstanceMetricsTest {

	@Test
	public void testGetECU() {
		InstanceMetrics im = new InstanceMetrics();
		im.add("m1.small", 1, 1, 1);
		im.add("m4.large", 2, 6.5, 1);
		
		assertEquals("m1.small should have ECU of 1", 1.0, im.getECU(UsageType.getUsageType("m1.small", "hours")), 0.1);
		assertEquals("m4.large.rhel should have ECU of 6.5", 6.5, im.getECU(UsageType.getUsageType("m4.large.rhel", "hours")), 0.1);
		assertEquals("db.m4.large.rhel should have ECU of 6.5", 6.5, im.getECU(UsageType.getUsageType("m4.large.rhel", "hours")), 0.1);
	}

	@Test
	public void testGetVCpu() {
		InstanceMetrics im = new InstanceMetrics();
		im.add("m1.small", 1, 1, 1);
		im.add("m4.large", 2, 6.5, 1);
		
		assertEquals("m1.small should have vCPU of 1", 1.0, im.getVCpu(UsageType.getUsageType("m1.small", "hours")), 0.1);
		assertEquals("m4.large.rhel should have vCPU of 2", 2.0, im.getVCpu(UsageType.getUsageType("m4.large.rhel", "hours")), 0.1);
		assertEquals("db.m4.large.rhel should have vCPU of 2", 2.0, im.getVCpu(UsageType.getUsageType("m4.large.rhel", "hours")), 0.1);
	}

	@Test
	public void testGetNormalizationFactor() {
		InstanceMetrics im = new InstanceMetrics();
		im.add("m1.small", 1, 1, 1);
		im.add("m1.xlarge", 1, 1, 8);
		
		assertEquals("m1.small should have normalization of 1", 1.0, im.getNormalizationFactor(UsageType.getUsageType("m1.small", "hours")), 0.1);
		assertEquals("m1.xlarge should have normalization of 8", 8.0, im.getNormalizationFactor(UsageType.getUsageType("m1.xlarge", "hours")), 0.1);
	}

}
