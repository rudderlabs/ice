package com.netflix.ice.processor;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;

public class TagCoverageMetricsTest {

	@Test
	public void testAdd() {
		TagCoverageMetrics metrics = new TagCoverageMetrics(1);
		metrics = TagCoverageMetrics.add(metrics, new boolean[]{ true });
		assertEquals("total is wrong", 1, metrics.total);
		
		metrics = TagCoverageMetrics.add(metrics, new boolean[]{ false });
		assertEquals("total is wrong", 2, metrics.total);
		assertEquals("count is wrong", 1, metrics.counts[0]);
	}

	@Test
	public void testPercentage() {
		TagCoverageMetrics metrics = new TagCoverageMetrics(5);
		assertEquals("test for empty metrics failed", 0.0, metrics.getPercentage(0), 0.001);
		
		metrics = new TagCoverageMetrics(5, new int[]{ 5 });
		assertEquals("test for 100% is wrong", 100.0, metrics.getPercentage(0), 0.001);
		
		metrics = new TagCoverageMetrics(5, new int[]{ 0 });
		assertEquals("test for 100% is wrong", 0.0, metrics.getPercentage(0), 0.001);
		
		metrics = new TagCoverageMetrics(10, new int[]{ 5 });
		assertEquals("test for 50% is wrong", 50.0, metrics.getPercentage(0), 0.001);
	}
	
	@Test
	public void testSerializer() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(output);
		
        TagCoverageMetrics metrics = new TagCoverageMetrics(10, new int[]{ 0, 1, 2, 3, 4 });
		
        metrics.serialize(out);
		ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
		DataInput in = new DataInputStream(input);
		metrics = TagCoverageMetrics.deserialize(in, 5);
		
		assertEquals("total is wrong", 10, metrics.total);
		for (int i = 0; i < 5; i++)
			assertEquals("count is wrong for index " + i, i, metrics.counts[i]);

	}
}
