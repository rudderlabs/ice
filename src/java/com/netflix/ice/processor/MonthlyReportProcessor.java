package com.netflix.ice.processor;

import java.util.List;
import java.util.TreeMap;

import org.joda.time.DateTime;

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
			CostAndUsageData costAndUsageData,
		    Instances instances) throws Exception;

}
