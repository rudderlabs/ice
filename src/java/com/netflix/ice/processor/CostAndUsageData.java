package com.netflix.ice.processor;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Months;
import org.joda.time.Weeks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.basic.BasicReservationService.Reservation;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.Config;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.common.Config.TagCoverage;
import com.netflix.ice.processor.ProcessorConfig.JsonFiles;
import com.netflix.ice.processor.pricelist.PriceListService;
import com.netflix.ice.reader.InstanceMetrics;
import com.netflix.ice.tag.Product;

public class CostAndUsageData {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    private Map<Product, ReadWriteData> usageDataByProduct;
    private Map<Product, ReadWriteData> costDataByProduct;
    private Map<Product, ReadWriteTagCoverageData> tagCoverage;
    private List<String> userTags;
    private boolean collectTagCoverageWithUserTags;
    private Map<String, Reservation> reservations;
    
	public CostAndUsageData(List<String> userTags, Config.TagCoverage tagCoverage) {
		this.usageDataByProduct = Maps.newHashMap();
		this.costDataByProduct = Maps.newHashMap();
        this.usageDataByProduct.put(null, new ReadWriteData());
        this.costDataByProduct.put(null, new ReadWriteData());
        this.tagCoverage = null;
        this.userTags = userTags;
        this.collectTagCoverageWithUserTags = tagCoverage == TagCoverage.withUserTags;
        if (userTags != null && tagCoverage != TagCoverage.none) {
    		this.tagCoverage = Maps.newHashMap();
        	this.tagCoverage.put(null, new ReadWriteTagCoverageData(userTags.size()));
        }
        this.reservations = Maps.newHashMap();
	}
	
	public ReadWriteData getUsage(Product product) {
		return usageDataByProduct.get(product);
	}
	
	public void putUsage(Product product, ReadWriteData data) {
		usageDataByProduct.put(product,  data);
	}
	
	public ReadWriteData getCost(Product product) {
		return costDataByProduct.get(product);
	}
	
	public void putCost(Product product, ReadWriteData data) {
		costDataByProduct.put(product,  data);
	}
	
	public ReadWriteTagCoverageData getTagCoverage(Product product) {
		return tagCoverage.get(product);
	}
	
	public void putTagCoverage(Product product, ReadWriteTagCoverageData data) {
		tagCoverage.put(product,  data);
	}
	
	
	public void putAll(CostAndUsageData data) {
		// Add all the data from the supplied CostAndUsageData
		
		for (Entry<Product, ReadWriteData> entry: data.usageDataByProduct.entrySet()) {
			ReadWriteData usage = getUsage(entry.getKey());
			if (usage == null) {
				usageDataByProduct.put(entry.getKey(), entry.getValue());
			}
			else {
				usage.putAll(entry.getValue());
			}
		}
		for (Entry<Product, ReadWriteData> entry: data.costDataByProduct.entrySet()) {
			ReadWriteData cost = getCost(entry.getKey());
			if (cost == null) {
				costDataByProduct.put(entry.getKey(), entry.getValue());
			}
			else {
				cost.putAll(entry.getValue());
			}
		}
		if (tagCoverage != null && data.tagCoverage != null) {
			for (Entry<Product, ReadWriteTagCoverageData> entry: data.tagCoverage.entrySet()) {
				ReadWriteTagCoverageData tc = getTagCoverage(entry.getKey());
				if (tc == null) {
					tagCoverage.put(entry.getKey(), entry.getValue());
				}
				else {
					tc.putAll(entry.getValue());
				}
			}	
		}
	}
	
    public void cutData(int hours) {
        for (ReadWriteData data: usageDataByProduct.values()) {
            data.cutData(hours);
        }
        for (ReadWriteData data: costDataByProduct.values()) {
            data.cutData(hours);
        }
    }
    
    public void addReservation(Reservation reservation) {
    	reservations.put(reservation.id, reservation);
    }
    
    public Map<String, Reservation> getReservations() {
    	return reservations;
    }
    
    public boolean hasReservations() {
    	return reservations != null && reservations.size() > 0;
    }
    
    /**
     * Add an entry to the tag coverage statistics for the given TagGroup
     */
    public void addTagCoverage(Product product, int index, TagGroup tagGroup, boolean[] userTagCoverage) {
    	if (tagCoverage == null || !tagGroup.product.hasResourceTags()) {
    		return;
    	}
    	
    	if (!collectTagCoverageWithUserTags && product != null) {
    		return;
    	}
    	
    	ReadWriteTagCoverageData data = tagCoverage.get(product);
    	if (data == null) {
    		data = new ReadWriteTagCoverageData(userTags == null ? 0 : userTags.size());
    		tagCoverage.put(product, data);
    	}
    	
    	Map<TagGroup, TagCoverageMetrics> hourData = data.getData(index);    	
    	hourData.put(tagGroup, TagCoverageMetrics.add(hourData.get(tagGroup), userTagCoverage));
    }

    public void archive(long startMilli, DateTime startDate, boolean compress, JsonFiles writeJsonFiles, InstanceMetrics instanceMetrics, PriceListService priceListService, int numThreads) throws Exception {
    	ExecutorService pool = Executors.newFixedThreadPool(numThreads);
    	List<Future<Void>> futures = Lists.newArrayList();
    	
        if (writeJsonFiles != JsonFiles.no)
        	futures.add(archiveHourlyJson(startMilli, writeJsonFiles, instanceMetrics, priceListService, pool));
    	
        int totalResourceTagGroups = 0;
        for (Product product: costDataByProduct.keySet()) {
            TagGroupWriter writer = new TagGroupWriter(product == null ? "all" : product.getServiceCode(), true);
            Collection<TagGroup> tagGroups = costDataByProduct.get(product).getTagGroups();
            logger.info("Write " + tagGroups.size() + " tagGroups for " + (product == null ? "all" : product.name));
            if (product != null)
            	totalResourceTagGroups += tagGroups.size();
            writer.archive(startMilli, tagGroups);
        }
        logger.info("Total of " + totalResourceTagGroups + " resource tagGroups");

        archiveSummary(startMilli, startDate, usageDataByProduct, "usage_", compress, pool, futures);
        archiveSummary(startMilli, startDate, costDataByProduct, "cost_", compress, pool, futures);
        archiveSummaryTagCoverage(startMilli, startDate, compress, pool, futures);

        archiveHourly(startMilli, usageDataByProduct, "usage_", compress, pool, futures);
        archiveHourly(startMilli, costDataByProduct, "cost_", compress, pool, futures);  
        //archiveHourlyTagCoverage(startMilli, compress);

    
		// Wait for completion
		for (Future<Void> f: futures) {
			f.get();
		}
    }
    
    public Future<Void> archiveHourlyJson(final long startMilli, final JsonFiles writeJsonFiles, final InstanceMetrics instanceMetrics, final PriceListService priceListService, ExecutorService pool) throws Exception {
    	return pool.submit(new Callable<Void>() {
    		@Override
    		public Void call() throws Exception {
    	        logger.info("archiving JSON data...");
    	        
    	        DateTime monthDateTime = new DateTime(startMilli, DateTimeZone.UTC);
    	        DataJsonWriter writer = new DataJsonWriter("hourly_all_" + AwsUtils.monthDateFormat.print(monthDateTime) + ".json",
    	        		monthDateTime, userTags, writeJsonFiles, costDataByProduct, usageDataByProduct, instanceMetrics, priceListService);
    	        writer.archive();
    	        return null;
    		}
    	});        
    }
    
    private void archiveHourly(long startMilli, Map<Product, ReadWriteData> dataMap, String prefix, boolean compress, ExecutorService pool, List<Future<Void>> futures) throws Exception {
        DateTime monthDateTime = new DateTime(startMilli, DateTimeZone.UTC);
        for (Product product: dataMap.keySet()) {
            String prodName = product == null ? "all" : product.getServiceCode();
            String name = prefix + "hourly_" + prodName + "_" + AwsUtils.monthDateFormat.print(monthDateTime);
            futures.add(archiveHourlyFile(name, dataMap.get(product), compress, pool));
        }
    }
    
    private Future<Void> archiveHourlyFile(final String name, final ReadWriteDataSerializer data, final boolean compress, ExecutorService pool) {
    	return pool.submit(new Callable<Void>() {
    		@Override
    		public Void call() throws Exception {
                DataWriter writer = new DataWriter(name, data, compress, false);
                writer.archive();
                return null;
    		}
    	});
    }

//    private void archiveHourlyTagCoverage(long startMilli, boolean compress) throws Exception {
//    	logger.info("archiving tag coverage data... ");
//        DateTime monthDateTime = new DateTime(startMilli, DateTimeZone.UTC);
//        for (Product product: tagCoverage.keySet()) {
//            String prodName = product == null ? "all" : product.getFileName();
//	        DataWriter writer = new DataWriter("coverage_hourly_" + prodName + "_" + AwsUtils.monthDateFormat.print(monthDateTime), tagCoverage.get(product), compress, false);
//	        writer.archive();
//        }
//    }

    private void addValue(List<Map<TagGroup, Double>> list, int index, TagGroup tagGroup, double v) {
        Map<TagGroup, Double> map = ReadWriteData.getCreateData(list, index);
        Double existedV = map.get(tagGroup);
        map.put(tagGroup, existedV == null ? v : existedV + v);
    }


    private void archiveSummary(long startMilli, DateTime startDate, Map<Product, ReadWriteData> dataMap, String prefix, boolean compress, ExecutorService pool, List<Future<Void>> futures) throws Exception {

        DateTime monthDateTime = new DateTime(startMilli, DateTimeZone.UTC);

        for (Product product: dataMap.keySet()) {
            String prodName = product == null ? "all" : product.getServiceCode();
            ReadWriteData data = dataMap.get(product);
            
            futures.add(archiveSummaryProduct(monthDateTime, startDate, prodName, data, prefix, compress, pool));
        }
    }
    
    private Future<Void> archiveSummaryProduct(final DateTime monthDateTime, final DateTime startDate, final String prodName, final ReadWriteData data, final String prefix, final boolean compress, ExecutorService pool) {
    	return pool.submit(new Callable<Void>() {
    		@Override
    		public Void call() throws Exception {
    		
	            Collection<TagGroup> tagGroups = data.getTagGroups();
	
	            // init daily, weekly and monthly
	            List<Map<TagGroup, Double>> daily = Lists.newArrayList();
	            List<Map<TagGroup, Double>> weekly = Lists.newArrayList();
	            List<Map<TagGroup, Double>> monthly = Lists.newArrayList();
	
	            // get last month data
	            ReadWriteData lastMonthData = new ReadWriteData();
	            DataWriter writer = new DataWriter(prefix + "hourly_" + prodName + "_" + AwsUtils.monthDateFormat.print(monthDateTime.minusMonths(1)), lastMonthData, compress, true);
	
	            // aggregate to daily, weekly and monthly
	            int dayOfWeek = monthDateTime.getDayOfWeek();
	            int daysFromLastMonth = dayOfWeek - 1;
	            int lastMonthNumHours = monthDateTime.minusMonths(1).dayOfMonth().getMaximumValue() * 24;
	            for (int hour = 0 - daysFromLastMonth * 24; hour < data.getNum(); hour++) {
	                if (hour < 0) {
	                    // handle data from last month, add to weekly
	                    Map<TagGroup, Double> prevData = lastMonthData.getData(lastMonthNumHours + hour);
	                    for (TagGroup tagGroup: tagGroups) {
	                        Double v = prevData.get(tagGroup);
	                        if (v != null && v != 0) {
	                            addValue(weekly, 0, tagGroup, v);
	                        }
	                    }
	                }
	                else {
	                    // this month, add to weekly, monthly and daily
	                    Map<TagGroup, Double> map = data.getData(hour);
	
	                    for (TagGroup tagGroup: tagGroups) {
	                        Double v = map.get(tagGroup);
	                        if (v != null && v != 0) {
	                            addValue(monthly, 0, tagGroup, v);
	                            addValue(daily, hour/24, tagGroup, v);
	                            addValue(weekly, (hour + daysFromLastMonth*24) / 24/7, tagGroup, v);
	                        }
	                    }
	                }
	            }
	            
	            // delete the local hourly data from last month, we no longer need it
	            writer.delete();
	            
	            // archive daily
	            int year = monthDateTime.getYear();
	            ReadWriteData dailyData = new ReadWriteData();
	            writer = new DataWriter(prefix + "daily_" + prodName + "_" + year, dailyData, compress, true);
	            dailyData.setData(daily, monthDateTime.getDayOfYear() -1, false);
	            writer.archive();
	
	            // archive monthly
	            ReadWriteData monthlyData = new ReadWriteData();
	            int numMonths = Months.monthsBetween(startDate, monthDateTime).getMonths();            
	            writer = new DataWriter(prefix + "monthly_" + prodName, monthlyData, compress, true);
	            monthlyData.setData(monthly, numMonths, false);            
	            writer.archive();
	
	            // archive weekly
	            DateTime weekStart = monthDateTime.withDayOfWeek(1);
	            int index;
	            if (!weekStart.isAfter(startDate))
	                index = 0;
	            else
	                index = Weeks.weeksBetween(startDate, weekStart).getWeeks() + (startDate.dayOfWeek() == weekStart.dayOfWeek() ? 0 : 1);
	            ReadWriteData weeklyData = new ReadWriteData();
	            writer = new DataWriter(prefix + "weekly_" + prodName, weeklyData, compress, true);
	            weeklyData.setData(weekly, index, true);
	            writer.archive();
	            return null;
    		}
        });
    }
    
    private void addTagCoverageValue(List<Map<TagGroup, TagCoverageMetrics>> list, int index, TagGroup tagGroup, TagCoverageMetrics v) {
        Map<TagGroup, TagCoverageMetrics> map = ReadWriteTagCoverageData.getCreateData(list, index);
        TagCoverageMetrics existedV = map.get(tagGroup);
        if (existedV == null)
        	map.put(tagGroup, v);
        else
        	existedV.add(v);
    }


    /**
     * Archive summary data for tag coverage. For tag coverage, we don't keep hourly data.
     */
    private void archiveSummaryTagCoverage(long startMilli, DateTime startDate, boolean compress, ExecutorService pool, List<Future<Void>> futures) throws Exception {
    	if (tagCoverage == null) {
    		return;
    	}

        DateTime monthDateTime = new DateTime(startMilli, DateTimeZone.UTC);

        for (Product product: tagCoverage.keySet()) {

            String prodName = product == null ? "all" : product.getServiceCode();
            ReadWriteTagCoverageData data = tagCoverage.get(product);
            
            futures.add(archiveSummaryTagCoverageProduct(monthDateTime, startDate, prodName, data, compress, pool));
        }
        
    }
    
    private Future<Void> archiveSummaryTagCoverageProduct(final DateTime monthDateTime, final DateTime startDate, final String prodName, final ReadWriteTagCoverageData data, final boolean compress, ExecutorService pool) {
    	return pool.submit(new Callable<Void>() {
    		@Override
    		public Void call() throws Exception {
    	        int numUserTags = userTags == null ? 0 : userTags.size();
                Collection<TagGroup> tagGroups = data.getTagGroups();

                // init daily, weekly and monthly
                List<Map<TagGroup, TagCoverageMetrics>> daily = Lists.newArrayList();
                List<Map<TagGroup, TagCoverageMetrics>> weekly = Lists.newArrayList();
                List<Map<TagGroup, TagCoverageMetrics>> monthly = Lists.newArrayList();

                int dayOfWeek = monthDateTime.getDayOfWeek();
                int daysFromLastMonth = dayOfWeek - 1;
                DataWriter writer = null;
                
                if (daysFromLastMonth > 0) {
                	// Get the daily data from last month so we can add it to the weekly data
                	DateTime previousMonthStartDay = monthDateTime.minusDays(daysFromLastMonth);
    	            int previousMonthYear = previousMonthStartDay.getYear();
    	            
    	            ReadWriteTagCoverageData previousDailyData = new ReadWriteTagCoverageData(numUserTags);
    	            writer = new DataWriter("coverage_daily_" + prodName + "_" + previousMonthYear, previousDailyData, compress, true);
    	            
    	            int day = previousMonthStartDay.getDayOfYear();
    	            for (int i = 0; i < daysFromLastMonth; i++) {
                        Map<TagGroup, TagCoverageMetrics> prevData = previousDailyData.getData(day);
                        day++;
                        for (TagGroup tagGroup: tagGroups) {
                            TagCoverageMetrics v = prevData.get(tagGroup);
                            if (v != null) {
                            	addTagCoverageValue(weekly, 0, tagGroup, v);
                            }
                        }
    	            }
                }
                
                // aggregate to daily, weekly and monthly
                for (int hour = 0; hour < data.getNum(); hour++) {
                    // this month, add to weekly, monthly and daily
                    Map<TagGroup, TagCoverageMetrics> map = data.getData(hour);

                    for (TagGroup tagGroup: tagGroups) {
                        TagCoverageMetrics v = map.get(tagGroup);
                        if (v != null) {
                        	addTagCoverageValue(monthly, 0, tagGroup, v);
                        	addTagCoverageValue(daily, hour/24, tagGroup, v);
                        	addTagCoverageValue(weekly, (hour + daysFromLastMonth*24) / 24/7, tagGroup, v);
                        }
                    }
                }
                
                // archive daily
                ReadWriteTagCoverageData dailyData = new ReadWriteTagCoverageData(numUserTags);
                writer = new DataWriter("coverage_daily_" + prodName + "_" + monthDateTime.getYear(), dailyData, compress, true);
                dailyData.setData(daily, monthDateTime.getDayOfYear() -1, false);
                writer.archive();

                // archive monthly
                ReadWriteTagCoverageData monthlyData = new ReadWriteTagCoverageData(numUserTags);
                int numMonths = Months.monthsBetween(startDate, monthDateTime).getMonths();            
                writer = new DataWriter("coverage_monthly_" + prodName, monthlyData, compress, true);
                monthlyData.setData(monthly, numMonths, false);            
                writer.archive();

                // archive weekly
                DateTime weekStart = monthDateTime.withDayOfWeek(1);
                int index;
                if (!weekStart.isAfter(startDate))
                    index = 0;
                else
                    index = Weeks.weeksBetween(startDate, weekStart).getWeeks() + (startDate.dayOfWeek() == weekStart.dayOfWeek() ? 0 : 1);
                ReadWriteTagCoverageData weeklyData = new ReadWriteTagCoverageData(numUserTags);
                writer = new DataWriter("coverage_weekly_" + prodName, weeklyData, compress, true);
                weeklyData.setData(weekly, index, true);
                writer.archive();
                return null;
            }
    			
    	});
    }

}



