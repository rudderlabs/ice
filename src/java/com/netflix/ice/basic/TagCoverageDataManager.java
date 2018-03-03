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
import com.netflix.ice.common.TagCoverageRatio;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.reader.AggregateType;
import com.netflix.ice.reader.DataManager;
import com.netflix.ice.reader.ReadOnlyData;
import com.netflix.ice.reader.TagGroupManager;
import com.netflix.ice.reader.TagLists;
import com.netflix.ice.reader.UsageUnit;
import com.netflix.ice.tag.Tag;
import com.netflix.ice.tag.TagType;

public class TagCoverageDataManager extends DataFilePoller implements DataManager {
    protected TagGroupManager tagGroupManager;

	public TagCoverageDataManager(DateTime startDate, String dbName, ConsolidateType consolidateType, TagGroupManager tagGroupManager, boolean compress,
			int monthlyCacheSize, AccountService accountService, ProductService productService) {
		super(startDate, dbName, consolidateType, compress, monthlyCacheSize, accountService, productService);
		this.tagGroupManager = tagGroupManager;
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
            	switch (consolidateType) {
            	case hourly:	resultIndex = Hours.hoursBetween(interval.getStart(), start).getHours();	break;
            	case daily:		resultIndex = Days.daysBetween(interval.getStart(), start).getDays();		break;
            	case weekly:	resultIndex = Weeks.weeksBetween(interval.getStart(), start).getWeeks();	break;
            	case monthly:	resultIndex = Months.monthsBetween(interval.getStart(), start).getMonths();	break;
            	}
            }
            else {
            	switch (consolidateType) {
            	case hourly:	fromIndex = Hours.hoursBetween(start, interval.getStart()).getHours();		break;
            	case daily:		fromIndex = Days.daysBetween(start, interval.getStart()).getDays();			break;
            	case monthly:	fromIndex = Months.monthsBetween(start, interval.getStart()).getMonths();	break;
            	case weekly:
                    fromIndex = Weeks.weeksBetween(start, interval.getStart()).getWeeks();
                    if (start.getDayOfWeek() != interval.getStart().getDayOfWeek())
                        fromIndex++;
                    break;                    
            	}
            }

            List<Integer> columnIndecies = Lists.newArrayList();
            int columnIndex = 0;
            for (TagGroup tagGroup: data.getTagGroups()) {
                if (tagLists.contains(tagGroup)) {
                	columnIndecies.add(columnIndex);
                }
                columnIndex++;
            }
            while (resultIndex < num && fromIndex < data.getNum()) {
                double[] fromData = data.getData(fromIndex++);
                for (int i = 0; i < columnIndecies.size(); i++)
                    result[resultIndex] = TagCoverageRatio.add(result[resultIndex], fromData[columnIndecies.get(i)]);
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
    
    private void addData(double[] from, double[] to) {
        for (int i = 0; i < from.length; i++) {
            to[i] = TagCoverageRatio.add(to[i], from[i]);
        }
    }
    
	@Override
	public Map<Tag, double[]> getData(Interval interval, TagLists tagLists,
			TagType groupBy, AggregateType aggregate, boolean forReservation,
			UsageUnit usageUnit) {

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
        if (aggregated != null) {
            result.put(Tag.aggregated, aggregated);
        }
        
        return result;
	}

	@Override
	public Map<Tag, double[]> getData(Interval interval, TagLists tagLists,
			TagType groupBy, AggregateType aggregate, boolean forReservation,
			UsageUnit usageUnit, int userTagGroupByIndex) {
		return getData(interval, tagLists, groupBy, aggregate, forReservation, usageUnit, 0);
	}
}
