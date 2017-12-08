package com.netflix.ice.common;

import static org.junit.Assert.*;

import org.junit.Test;

public class TagCoverageRatioTest {

	@Test
	public void testConstructor() {
		TagCoverageRatio ratio = new TagCoverageRatio();
		assertEquals("Wrong count", 0, ratio.count);
		assertEquals("Wrong total", 0, ratio.total);
		
		ratio = new TagCoverageRatio(1, 1);
		assertEquals("Wrong count after construction", 1, ratio.count);
		assertEquals("Wrong total after construction", 1, ratio.total);
		
		ratio = new TagCoverageRatio(ratio.toDouble());
		assertEquals("Wrong count after conversion", 1, ratio.count);
		assertEquals("Wrong total after conversion", 1, ratio.total);
	}

	@Test
	public void testAddDoubles() {
		double a = new TagCoverageRatio(1, 1).toDouble();
		double b = new TagCoverageRatio(1, 1).toDouble();
		double sum = TagCoverageRatio.add(a, b);
		TagCoverageRatio ratio = new TagCoverageRatio(sum);
		assertEquals("Wrong count after conversion", 2, ratio.count);
		assertEquals("Wrong total after conversion", 2, ratio.total);
	}

	@Test
	public void testMaxCount() {
		assertEquals("ScaleFactor gets truncated", TagCoverageRatio.scaleFactor, (double) ((long) TagCoverageRatio.scaleFactor), 0.0001);

		final long maxValue = (long) TagCoverageRatio.scaleFactor - 1;
		TagCoverageRatio maxRatio = new TagCoverageRatio(maxValue, maxValue);
		assertEquals("Wrong count after construction", maxValue, maxRatio.count);
		assertEquals("Wrong total after construction", maxValue, maxRatio.total);

		double max = maxRatio.toDouble();
		TagCoverageRatio ratio = new TagCoverageRatio(max);
		assertEquals("Wrong count after conversion", maxValue, ratio.count);
		assertEquals("Wrong total after conversion", maxValue, ratio.total);
	}

	@Test
	public void testAddBool() {
		TagCoverageRatio a = new TagCoverageRatio(1, 1);
		TagCoverageRatio b = a.add(true);
		assertEquals("Wrong count", 2, b.count);
		assertEquals("Wrong total", 2, b.total);
		
		b = a.add(false);
		assertEquals("Wrong count", 1, b.count);
		assertEquals("Wrong total", 2, b.total);
						
		double percent = b.toPercentage();
		assertEquals("Wrong percentage", 50.0, percent, 0.0001);
	}

}
