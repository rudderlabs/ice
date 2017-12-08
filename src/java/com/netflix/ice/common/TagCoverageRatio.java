package com.netflix.ice.common;

/**
 * Class for packing a ratio of two integers into a double
 */
public class TagCoverageRatio {
	// A big number that won't overflow a double when squared.
	// Double mantissa is about 52 bits, so square root of 2^52 is
	// roughly 6.7e7
	protected static final double scaleFactor = 6.7e7d;
	
	final public long count;
	final public long total;

	public TagCoverageRatio(boolean hasTag) {
		this.count = hasTag ? 1 : 0;
		this.total = 1;
	}

	public TagCoverageRatio() {
		this.count = 0;
		this.total = 0;
	}

	public TagCoverageRatio(long count, long total) {
		this.count = count;
		this.total = total;
	}
	
	public TagCoverageRatio(Double ratio) {
		// Decode the ratio		
		count = (ratio == null) ? 0 : (long) Math.floor(ratio / scaleFactor);
		total = (ratio == null) ? 0 : (long) Math.floor(ratio % scaleFactor);		
	}

	/**
	 * Add one data value to the ratio
	 */
	public TagCoverageRatio add(boolean hasTag) {
		TagCoverageRatio ratio = new TagCoverageRatio(count + (hasTag ? 1 : 0), total + 1);
		return ratio;
	}
	
	/**
	 * Add two ratios
	 */
	public TagCoverageRatio add(TagCoverageRatio ratio) {
		return new TagCoverageRatio(count + ratio.count, total + ratio.total);
	}
	
	/**
	 * Convert ratio to a percentage
	 */
	public double toPercentage() {
		if (total == 0)
			return 0;
		return (double) count / total * 100.0;
	}
	
	/**
	 * Encode the ratio as a double
	 */
	public double toDouble() {
		return count * scaleFactor + total;
	}
	
	/**
	 * Add a value to an encoded ratio and return the encoded result
	 */
	static public double add(Double existing, boolean hasTag) {
		return new TagCoverageRatio(existing).add(hasTag).toDouble();
	}
	
	/**
	 * Add two encoded ratios and return an encoded result
	 */
	static public double add(Double a, Double b) {
		return new TagCoverageRatio(a).add(new TagCoverageRatio(b)).toDouble();
	}
}
