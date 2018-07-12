package com.netflix.ice.processor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import com.netflix.ice.common.TagGroup;

public class ReadWriteTagCoverageData extends ReadWriteGenericData<TagCoverageMetrics> {
	private final int numUserTags;
	
    static public Map<TagGroup, TagCoverageMetrics> getCreateData(List<Map<TagGroup, TagCoverageMetrics>> data, int i) {
        if (i >= data.size()) {
            for (int j = data.size(); j <= i; j++) {
                data.add(Maps.<TagGroup, TagCoverageMetrics>newHashMap());
            }
        }
        return data.get(i);
    }
    
	public ReadWriteTagCoverageData(int numUserTags) {
		super();
		this.numUserTags = numUserTags;
	}

	@Override
	protected void writeValue(DataOutput out, TagCoverageMetrics value) throws IOException {
		out.writeBoolean(value != null);
		if (value != null)
			value.serialize(out);	
	}

	@Override
	protected TagCoverageMetrics readValue(DataInput in) throws IOException {
		Boolean hasValue = in.readBoolean();
		return hasValue ? TagCoverageMetrics.deserialize(in, numUserTags) : null;
	}        
}
