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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.*;
import com.netflix.ice.reader.*;
import com.netflix.ice.tag.Tag;
import com.netflix.ice.tag.TagType;
import com.netflix.ice.tag.UsageType;

import org.joda.time.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * This class reads data from s3 bucket and feeds the data to UI
 */
public class BasicDataManager extends DataFilePoller implements DataManager {

    protected TagGroupManager tagGroupManager;
    protected InstanceMetricsService instanceMetricsService;

    public BasicDataManager(DateTime startDate, String dbName, ConsolidateType consolidateType, TagGroupManager tagGroupManager, boolean compress,
    		int monthlyCacheSize, AccountService accountService, ProductService productService, InstanceMetricsService instanceMetricsService) {
    	super(startDate, dbName, consolidateType, compress, monthlyCacheSize, accountService, productService);
        this.tagGroupManager = tagGroupManager;
        this.instanceMetricsService = instanceMetricsService;

        start();
    }
    
    private double[] getData(Interval interval, TagLists tagLists, UsageUnit usageUnit) throws ExecutionException {
    	Interval adjusted = getAdjustedInterval(interval);
        DateTime start = adjusted.getStart();
        DateTime end = adjusted.getEnd();

        int num = getSize(interval);
        double[] result = new double[num];

        do {
            ReadOnlyData data = getReadOnlyData(start);

            int resultIndex = 0;
            int fromIndex = 0;

            if (interval.getStart().isBefore(start)) {
                if (consolidateType == ConsolidateType.hourly) {
                    resultIndex = Hours.hoursBetween(interval.getStart(), start).getHours();
                }
                else if (consolidateType == ConsolidateType.daily) {
                    resultIndex = Days.daysBetween(interval.getStart(), start).getDays();
                }
                else if (consolidateType == ConsolidateType.weekly) {
                    resultIndex = Weeks.weeksBetween(interval.getStart(), start).getWeeks();
                }
                else if (consolidateType == ConsolidateType.monthly) {
                    resultIndex = Months.monthsBetween(interval.getStart(), start).getMonths();
                }
            }
            else {
                if (consolidateType == ConsolidateType.hourly) {
                    fromIndex = Hours.hoursBetween(start, interval.getStart()).getHours();
                }
                else if (consolidateType == ConsolidateType.daily) {
                    fromIndex = Days.daysBetween(start, interval.getStart()).getDays();
                }
                else if (consolidateType == ConsolidateType.weekly) {
                    fromIndex = Weeks.weeksBetween(start, interval.getStart()).getWeeks();
                    if (start.getDayOfWeek() != interval.getStart().getDayOfWeek())
                        fromIndex++;
                }
                else if (consolidateType == ConsolidateType.monthly) {
                    fromIndex = Months.monthsBetween(start, interval.getStart()).getMonths();
                }
            }

            List<Integer> columnIndecies = Lists.newArrayList();
            List<UsageType> usageTypes = Lists.newArrayList();
            int columnIndex = 0;
            for (TagGroup tagGroup: data.getTagGroups()) {
                if (tagLists.contains(tagGroup)) {
                	columnIndecies.add(columnIndex);
                    usageTypes.add(tagGroup.usageType);
                }
                columnIndex++;
            }
            while (resultIndex < num && fromIndex < data.getNum()) {
                double[] fromData = data.getData(fromIndex++);
                for (int i = 0; i < columnIndecies.size(); i++)
                    result[resultIndex] += adjustForUsageUnit(usageUnit, usageTypes.get(i), fromData[columnIndecies.get(i)]);
                resultIndex++;
            }

            if (consolidateType  == ConsolidateType.hourly)
                start = start.plusMonths(1);
            else if (consolidateType  == ConsolidateType.daily)
                start = start.plusYears(1);
            else
                break;
        }
        while (start.isBefore(end));
        
        return result;
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

    private void addData(double[] from, double[] to) {
        for (int i = 0; i < from.length; i++)
            to[i] += from[i];
    }

    public Map<Tag, double[]> getData(Interval interval, TagLists tagLists, TagType groupBy, AggregateType aggregate, boolean forReservation, UsageUnit usageUnit) {

        Map<Tag, TagLists> tagListsMap;

        if (groupBy == null) {
            tagListsMap = Maps.newHashMap();
            tagListsMap.put(Tag.aggregated, tagLists);
        }
        else
            tagListsMap = tagGroupManager.getTagListsMap(interval, tagLists, groupBy, forReservation);

        Map<Tag, double[]> result = Maps.newTreeMap();
        double[] aggregated = null;

        for (Tag tag: tagListsMap.keySet()) {
            try {
                double[] data = getData(interval, tagListsMap.get(tag), usageUnit);
                result.put(tag, data);
                if (aggregate != AggregateType.none && tagListsMap.size() > 1) {
                    if (aggregated == null)
                        aggregated = new double[data.length];
                    addData(data, aggregated);
                }
            }
            catch (ExecutionException e) {
                logger.error("error in getData for " + tag + " " + interval, e);
            }
        }
        if (aggregated != null)
            result.put(Tag.aggregated, aggregated);
        return result;
    }

}
