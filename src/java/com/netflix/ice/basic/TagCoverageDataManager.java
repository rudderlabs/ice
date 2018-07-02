package com.netflix.ice.basic;

import java.util.Map;

import org.joda.time.DateTime;

import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ConsolidateType;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagCoverageRatio;
import com.netflix.ice.reader.DataManager;
import com.netflix.ice.reader.TagGroupManager;
import com.netflix.ice.reader.UsageUnit;
import com.netflix.ice.tag.Tag;
import com.netflix.ice.tag.TagType;
import com.netflix.ice.tag.UsageType;

public class TagCoverageDataManager extends CommonDataManager implements DataManager {

	public TagCoverageDataManager(DateTime startDate, String dbName, ConsolidateType consolidateType, TagGroupManager tagGroupManager, boolean compress,
			int monthlyCacheSize, AccountService accountService, ProductService productService) {
		super(startDate, dbName, consolidateType, tagGroupManager, compress, monthlyCacheSize, accountService, productService);
	}
    
	@Override
    public double add(double to, double from, UsageUnit usageUnit, UsageType usageType) {
    	return TagCoverageRatio.add(to, from);
    }
    
    @Override
    public void addData(double[] from, double[] to) {
        for (int i = 0; i < from.length; i++) {
            to[i] = TagCoverageRatio.add(to[i], from[i]);
        }
    }
    
    @Override
    public void putResult(Map<Tag, double[]> result, Tag tag, double[] data, TagType groupBy) {
    	// Check for values in the data array and ignore if all zeros
    	boolean noData = true;
    	for (double d: data) {
    		if (d != 0.0) {
    			noData = false;
    			break;
    		}
    	}
    	if (noData)
    		return;
    	
    	result.put(tag, data);
    }

}
