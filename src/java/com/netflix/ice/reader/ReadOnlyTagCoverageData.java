package com.netflix.ice.reader;

import java.io.DataInput;
import java.io.IOException;

import com.google.common.collect.Lists;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.TagCoverageMetrics;

public class ReadOnlyTagCoverageData extends ReadOnlyGenericData<TagCoverageMetrics> {
	private final int numUserTags;

	public ReadOnlyTagCoverageData(int numUserTags) {
		super(new TagCoverageMetrics[][]{}, Lists.<TagGroup>newArrayList());
		this.numUserTags = numUserTags;
	}

	@Override
	protected TagCoverageMetrics[][] newDataMatrix(int size) {
		return new TagCoverageMetrics[size][];
	}

	@Override
	protected TagCoverageMetrics[] newDataArray(int size) {
		return new TagCoverageMetrics[size];
	}

	@Override
	protected TagCoverageMetrics readValue(DataInput in) throws IOException {
		Boolean hasValue = in.readBoolean();
		return hasValue ? TagCoverageMetrics.deserialize(in, numUserTags) : null;
	}

}
