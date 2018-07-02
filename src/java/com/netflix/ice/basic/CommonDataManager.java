package com.netflix.ice.basic;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Hours;
import org.joda.time.Interval;
import org.joda.time.Months;
import org.joda.time.Weeks;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ConsolidateType;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.reader.AggregateType;
import com.netflix.ice.reader.DataManager;
import com.netflix.ice.reader.ReadOnlyData;
import com.netflix.ice.reader.TagGroupManager;
import com.netflix.ice.reader.TagLists;
import com.netflix.ice.reader.UsageUnit;
import com.netflix.ice.tag.Tag;
import com.netflix.ice.tag.TagType;
import com.netflix.ice.tag.UsageType;

public abstract class CommonDataManager  extends DataFilePoller implements DataManager {

    protected TagGroupManager tagGroupManager;
    
	public CommonDataManager(DateTime startDate, String dbName, ConsolidateType consolidateType, TagGroupManager tagGroupManager, boolean compress,
    		int monthlyCacheSize, AccountService accountService, ProductService productService) {
    	super(startDate, dbName, consolidateType, compress, monthlyCacheSize, accountService, productService);
        this.tagGroupManager = tagGroupManager;
	}

    abstract public double add(double to, double from, UsageUnit usageUnit, UsageType usageType);

    public double[] getData(Interval interval, TagLists tagLists, UsageUnit usageUnit, TagType groupBy) throws ExecutionException {
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
            List<TagGroup> tagGroups = Lists.newArrayList();
            int columnIndex = 0;
            for (TagGroup tagGroup: data.getTagGroups()) {
            	boolean contains = tagLists.contains(tagGroup, true);
//            	if (tagLists instanceof TagListsWithUserTags) {
//            		logger.debug("resource: " + contains + ", " + tagGroup + ", " + columnIndex);
//            	}
                if (contains) {
                	columnIndecies.add(columnIndex);
                	tagGroups.add(tagGroup);
                }
                columnIndex++;
            }
//        	if (tagLists instanceof TagListsWithUserTags || groupBy == TagType.ResourceGroup) {
//        		logger.info("tagGroups = " + tagGroups.size() /* + ", tagLists: " + tagLists*/);
//                double[] fromData = data.getData(fromIndex);
//                for (int i = 0; i < columnIndecies.size(); i++)
//                	logger.info("      " + fromData[columnIndecies.get(i)] + ", " + tagGroups.get(i) + ", " + (tagGroups.get(i).resourceGroup == null ? "null" : tagGroups.get(i).resourceGroup.isProductName()));
//        		
//        	}
            while (resultIndex < num && fromIndex < data.getNum()) {
                double[] fromData = data.getData(fromIndex++);
                for (int i = 0; i < columnIndecies.size(); i++)
                    result[resultIndex] = add(result[resultIndex], fromData[columnIndecies.get(i)], usageUnit, tagGroups.get(i).usageType);
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

    abstract public void addData(double[] from, double[] to);
    
    abstract public void putResult(Map<Tag, double[]> result, Tag tag, double[] data, TagType groupBy);

	@Override
    public Map<Tag, double[]> getData(Interval interval, TagLists tagLists, TagType groupBy, AggregateType aggregate, boolean forReservation, UsageUnit usageUnit, int userTagGroupByIndex) {

        Map<Tag, TagLists> tagListsMap;

        if (groupBy == null) {
            tagListsMap = Maps.newHashMap();
            tagListsMap.put(Tag.aggregated, tagLists);
        }
        else
            tagListsMap = tagGroupManager.getTagListsMap(interval, tagLists, groupBy, forReservation, userTagGroupByIndex);

        Map<Tag, double[]> result = Maps.newTreeMap();
        
        for (Tag tag: tagListsMap.keySet()) {
            try {
                //logger.info("Tag: " + tag + ", TagLists: " + tagListsMap.get(tag));
                double[] data = getData(interval, tagListsMap.get(tag), usageUnit, groupBy);
                putResult(result, tag, data, groupBy);
            }
            catch (ExecutionException e) {
                logger.error("error in getData for " + tag + " " + interval, e);
            }
        }
        if (aggregate != AggregateType.none) {
            double[] aggregated = null;
        	for (double[] data: result.values()) {
                if (aggregated == null)
                    aggregated = new double[data.length];
                addData(data, aggregated);
            }
            if (aggregated != null)
                result.put(Tag.aggregated, aggregated);
            
            // debugging
//            if (aggregated != null) {
//            	for (Tag tag: result.keySet()) {
//            		double total = 0;
//            		double[] data = result.get(tag);
//    	            for (int i = 0; i < data.length; i++)
//    	            	total += data[i];
//    	            logger.info("      " + tag + ": " + total);            		
//            	}
//	            double total = 0;
//	            for (int i = 0; i < aggregated.length; i++)
//	            	total += aggregated[i];
//	            logger.info("   Total: " + total);
//            }
		}
        
        return result;
    }


	@Override
	public Map<Tag, double[]> getData(Interval interval, TagLists tagLists,
			TagType groupBy, AggregateType aggregate, boolean forReservation,
			UsageUnit usageUnit) {
		return getData(interval, tagLists, groupBy, aggregate, forReservation, usageUnit, 0);
	}

}
