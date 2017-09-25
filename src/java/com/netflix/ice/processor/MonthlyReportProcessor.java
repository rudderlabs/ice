package com.netflix.ice.processor;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.joda.time.DateTime;

import com.netflix.ice.tag.Product;

/*
 * Monthly cost and usage data
 */
public interface MonthlyReportProcessor {
	public TreeMap<DateTime, List<MonthlyReport>> getReportsToProcess();
	
	abstract public long downloadAndProcessReport(
			DateTime dataTime,
			MonthlyReport report,
			String localDir,
			long lastProcessed,
			Map<Product, ReadWriteData> usageDataByProduct,
		    Map<Product, ReadWriteData> costDataByProduct,
		    Instances instances) throws Exception;

}
