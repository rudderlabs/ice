package com.netflix.ice.processor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TagCoverageMetrics {
	int total;
	int[] counts;

	private TagCoverageMetrics() {		
	}
	
	public TagCoverageMetrics(int size) {
		total = 0;
		counts = new int[size];
	}
	
	TagCoverageMetrics(int total, int[] counts) {
		this.total = total;
		this.counts = counts;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder(64);
		for (int c: counts)
			sb.append(((Integer) c).toString() + ", ");
		return "{total:" + total + ", counts:[" + sb.toString() + "]}";
	}
	
	public int size() {
		return counts.length;
	}
	
	public int getTotal() {
		return total;
	}
	
	public int getCount(int i) {
		return counts[i];
	}
	
	public double getPercentage(int index) {
		if (total == 0)
			return 0.0;
		return (double) counts[index] / (double) total * 100.0;
	}
	
	public void add(TagCoverageMetrics other) throws ArithmeticException {
		if (other.total == 0)
			return;
		
		total += other.total;
		for (int i = 0; i < counts.length; i++) {
			counts[i] += other.counts[i];
			if (counts[i] > total)
				throw new ArithmeticException("Count exceeds total");
		}
	}
	
	public void serialize(DataOutput out) throws IOException {
        out.writeInt(total);
		for (int i = 0; i < counts.length; i++)
			out.writeInt(counts[i]);
	}
	
	static public TagCoverageMetrics deserialize(DataInput in, int numUserTags) throws IOException {
		TagCoverageMetrics metrics = new TagCoverageMetrics();
		
		metrics.total = in.readInt();
		metrics.counts = new int[numUserTags];
		for (int i = 0; i < numUserTags; i++) {
			metrics.counts[i] = in.readInt();
			if (metrics.counts[i] > metrics.total)
				throw new IOException("Count of " + metrics.counts[i] + " is larger than total " + metrics.total + ", index " + i);
		}
		
		return metrics;
	}
	
	static public TagCoverageMetrics add(TagCoverageMetrics existing, boolean[] userTagCoverage) {
		TagCoverageMetrics metrics = existing == null ? new TagCoverageMetrics(userTagCoverage.length) : existing;
		
		metrics.total++;
		for (int i = 0; i < userTagCoverage.length; i++)
			metrics.counts[i] += userTagCoverage[i] ? 1 : 0;
		
		return metrics;
	}
}
