/*
 *
 *  Copyright 2013 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.ice.basic;

import java.util.Map;

import com.netflix.ice.common.*;
import com.netflix.ice.reader.*;
import com.netflix.ice.tag.Tag;
import com.netflix.ice.tag.TagType;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.UserTag;

import org.joda.time.*;

/**
 * This class reads data from s3 bucket and feeds the data to UI
 */
public class BasicDataManager extends CommonDataManager implements DataManager {

    protected InstanceMetricsService instanceMetricsService;
    
    public BasicDataManager(DateTime startDate, String dbName, ConsolidateType consolidateType, TagGroupManager tagGroupManager, boolean compress,
    		int monthlyCacheSize, AccountService accountService, ProductService productService, InstanceMetricsService instanceMetricsService) {
    	super(startDate, dbName, consolidateType, tagGroupManager, compress, monthlyCacheSize, accountService, productService);
        this.instanceMetricsService = instanceMetricsService;

        start();
    }
        
    @Override
    public double add(double to, double from, UsageUnit usageUnit, UsageType usageType) {
    	return to + adjustForUsageUnit(usageUnit, usageType, from);
    }
    
    private double adjustForUsageUnit(UsageUnit usageUnit, UsageType usageType, double value) {
    	double multiplier = 1.0;
    	
    	switch (usageUnit) {
    	default:
    		return value;
    	
    	case ECUs:
    		multiplier = instanceMetricsService.getInstanceMetrics().getECU(usageType);
    		break;
    		
    	case vCPUs:
    		multiplier = instanceMetricsService.getInstanceMetrics().getVCpu(usageType);
    		break;
    	case Normalized:
    		multiplier = instanceMetricsService.getInstanceMetrics().getNormalizationFactor(usageType);
    		break;
    	}
    	return value * multiplier;    		
    }

	@Override
    public void addData(double[] from, double[] to) {
        for (int i = 0; i < from.length; i++)
            to[i] += from[i];
    }

    @Override
    public void putResult(Map<Tag, double[]> result, Tag tag, double[] data, TagType groupBy) {        
        if (groupBy == TagType.Tag) {
        	Tag userTag = (UserTag) (tag.name.isEmpty() ? UserTag.get(UserTag.none) : tag);
        	
        	//logger.info("resourceGroup: " + tag + " -> " + userTag + " value: " + data[0] + ", " + dbName);
        	
			if (result.containsKey(userTag)) {
				// aggregate current data with the one already in the map
				addData(data, result.get(userTag));
			}
			else {
				// Put in map using the user tag
				result.put(userTag, data);
			}
        }
        else {
        	result.put(tag, data);
        }
    }
}
