package com.netflix.ice.processor;

import java.io.File;
import java.io.IOException;
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
	public Long processReport(
			DateTime dataTime,
			MonthlyReport report,
			List<File> files,
			Map<Product, ReadWriteData> usageDataByProduct,
		    Map<Product, ReadWriteData> costDataByProduct,
		    Instances instances) throws IOException;

	abstract public List<File> downloadReport(MonthlyReport report, String localDir, long lastProcessed);
}
