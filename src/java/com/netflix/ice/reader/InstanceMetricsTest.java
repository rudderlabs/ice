package com.netflix.ice.reader;

import static org.junit.Assert.*;

import org.junit.Test;

import com.netflix.ice.tag.UsageType;

public class InstanceMetricsTest {

	@Test
	public void testGetECU() {
		assertEquals("m1.small should have ECU of 1", 1.0, InstanceMetrics.getECU(UsageType.getUsageType("m1.small", "")), 0.1);
		assertEquals("m4.large.rhel should have ECU of 6.5", 6.5, InstanceMetrics.getECU(UsageType.getUsageType("m4.large.rhel", "")), 0.1);
		assertEquals("db.m4.large.rhel should have ECU of 6.5", 6.5, InstanceMetrics.getECU(UsageType.getUsageType("m4.large.rhel", "")), 0.1);
	}

	@Test
	public void testGetVCpu() {
		assertEquals("m1.small should have vCPU of 1", 1.0, InstanceMetrics.getVCpu(UsageType.getUsageType("m1.small", "")), 0.1);
		assertEquals("m4.large.rhel should have vCPU of 2", 2.0, InstanceMetrics.getVCpu(UsageType.getUsageType("m4.large.rhel", "")), 0.1);
		assertEquals("db.m4.large.rhel should have vCPU of 2", 2.0, InstanceMetrics.getVCpu(UsageType.getUsageType("m4.large.rhel", "")), 0.1);
	}

}
