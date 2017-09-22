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
package com.netflix.ice.processor;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.StopInstancesRequest;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClient;
import com.amazonaws.services.simpleemail.model.*;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.*;
import com.netflix.ice.processor.pricelist.InstancePrices;
import com.netflix.ice.processor.pricelist.InstancePrices.ServiceCode;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Operation.ReservationOperation;
import com.netflix.ice.tag.Product;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Months;
import org.joda.time.Weeks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.NumberFormat;
import java.util.*;

/**
 * Class to process billing files and produce tag, usage, cost output files for reader/UI.
 */
public class BillingFileProcessor extends Poller {
    protected static Logger staticLogger = LoggerFactory.getLogger(BillingFileProcessor.class);

    private ProcessorConfig config;
    private Long startMilli;
    private Long endMilli;
    /**
     * The usageDataByProduct map holds both the usage data for each
     * individual product that has resourceIDs (if ResourceService is enabled) and a "null"
     * key entry for aggregated data for "all" services.
     * i.e. the null key means "all"
     */
    private Map<Product, ReadWriteData> usageDataByProduct;
    private Map<Product, ReadWriteData> costDataByProduct;
    private Instances instances;
    private Double ondemandThreshold;
    private String fromEmail;
    private String alertEmails;
    private String urlPrefix;
    
    ReservationProcessor reservationProcessor;
    private MonthlyReportProcessor dbrProcessor;
    private MonthlyReportProcessor cauProcessor;
    

    public BillingFileProcessor(ProcessorConfig config, String urlPrefix, Double ondemandThreshold, String fromEmail, String alertEmails) throws Exception {
    	this.config = config;
        this.ondemandThreshold = ondemandThreshold;
        this.fromEmail = fromEmail;
        this.alertEmails = alertEmails;
        this.urlPrefix = urlPrefix;
        
        reservationProcessor = new ReservationProcessor(
        							config.accountService.getPayerAccounts(),
        							config.accountService.getReservationAccounts().keySet(),
        							config.productService,
        							config.priceListService);
        dbrProcessor = new DetailedBillingReportProcessor(config);
        cauProcessor = new CostAndUsageReportProcessor(config);
    }

    @Override
    protected void poll() throws Exception {
        TreeMap<DateTime, List<MonthlyReport>> reportsToProcess = dbrProcessor.getReportsToProcess();
        reportsToProcess.putAll(cauProcessor.getReportsToProcess());
                
        for (DateTime dataTime: reportsToProcess.keySet()) {
            startMilli = endMilli = dataTime.getMillis();
            init();
            
            long lastProcessed = lastProcessTime(AwsUtils.monthDateFormat.print(dataTime));
            long processTime = new DateTime(DateTimeZone.UTC).getMillis();

            boolean hasTags = false;
            boolean hasNewFiles = false;
            for (MonthlyReport report: reportsToProcess.get(dataTime)) {
            	hasTags |= report.hasTags();
            	
                if (report.getLastModifiedMillis() < lastProcessed) {
                    logger.info("data has been processed. ignoring " + report.getReportKey() + "...");
                    continue;
                }
                hasNewFiles = true;
            }
            
            if (!hasNewFiles) {
                logger.info("data has been processed. ignoring all files at " + AwsUtils.monthDateFormat.print(dataTime));
                continue;
            }
            
            for (MonthlyReport report: reportsToProcess.get(dataTime)) {
            	List<File> files = report.getProcessor().downloadReport(report, config.localDir, lastProcessed);
            	String fileKey = report.getReportKey();
                logger.info("processing " + fileKey + "...");
                Long end = report.getProcessor().processReport(dataTime, report, files,
                		usageDataByProduct, costDataByProduct, instances);
                endMilli = Math.max(endMilli, end);

                logger.info("done processing " + fileKey + ", end is " + LineItem.amazonBillingDateFormat.print(new DateTime(end)));
            }
        	
            if (dataTime.equals(reportsToProcess.lastKey())) {
                int hours = (int) ((endMilli - startMilli)/3600000L);
    	        String start = LineItem.amazonBillingDateFormat.print(new DateTime(startMilli));
    	        String end = LineItem.amazonBillingDateFormat.print(new DateTime(endMilli));

                logger.info("cut hours to " + hours + ", " + start + " to " + end);
                cutData(hours);
            }
            
            
            
            /***** Debugging */
//            ReadWriteData costData = costDataByProduct.get(null);
//            Map<TagGroup, Double> costMap = costData.getData(0);
//            TagGroup redshiftHeavyTagGroup = new TagGroup(config.accountService.getAccountByName("IntegralReach"), Region.US_EAST_1, null, Product.redshift, Operation.reservedInstancesHeavy, UsageType.getUsageType("dc1.8xlarge", Operation.reservedInstancesHeavy, ""), null);
//            Double used = costMap.get(redshiftHeavyTagGroup);
//            logger.info("First hour cost is " + used + " for " + redshiftHeavyTagGroup + " before reservation processing");
            
            // now get reservation capacity to calculate upfront and un-used cost
            for (Ec2InstanceReservationPrice.ReservationUtilization utilization: Ec2InstanceReservationPrice.ReservationUtilization.values()) {
            	// We no longer support Light and Medium
            	reservationProcessor.process(utilization, config.reservationService, usageDataByProduct.get(null), costDataByProduct.get(null), dataTime);
            }
            
            logger.info("adding savings data for " + dataTime + "...");
            addSavingsData(dataTime, usageDataByProduct.get(null), costDataByProduct.get(null));

            /***** Debugging */
//            used = costMap.get(redshiftHeavyTagGroup);
//            logger.info("First hour cost is " + used + " for " + redshiftHeavyTagGroup + " after reservation processing");

            if (hasTags && config.resourceService != null)
                config.resourceService.commit();

            logger.info("archiving results for " + dataTime + "...");
            archive();
            logger.info("done archiving " + dataTime);

            updateProcessTime(AwsUtils.monthDateFormat.print(dataTime), processTime);
            if (dataTime.equals(reportsToProcess.lastKey())) {
                sendOndemandCostAlert();
            }
        }

        logger.info("AWS usage processed.");
        if (config.processOnce) {
        	// We're done. If we're running on an AWS EC2 instance, stop the instance
            logger.info("Stopping EC2 Instance " + config.processorInstanceId + " in region " + config.processorRegion);
            
            AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard()
            		.withRegion(config.processorRegion)
            		.withCredentials(AwsUtils.awsCredentialsProvider)
            		.withClientConfiguration(AwsUtils.clientConfig)
            		.build();

            try {
	            StopInstancesRequest request = new StopInstancesRequest().withInstanceIds(new String[] { config.processorInstanceId });
	            ec2.stopInstances(request);
            }
            catch (Exception e) {
                logger.error("error in stopInstances", e);
            }
            ec2.shutdown();
        }
    }
    
    private void addSavingsData(DateTime month, ReadWriteData usageData, ReadWriteData costData) throws Exception {
        // Get price list
    	InstancePrices ec2Prices = config.priceListService.getPrices(month, ServiceCode.AmazonEC2);
        
    	/*
    	 * Run through all the spot instance usage and add savings data
    	 */
    	for (TagGroup tg: usageData.getTagGroups()) {
    		if (tg.operation == ReservationOperation.spotInstances) {
    			TagGroup savingsTag = new TagGroup(tg.account, tg.region, tg.zone, tg.product, ReservationOperation.spotInstanceSavings, tg.usageType, tg.resourceGroup);
    			for (int i = 0; i < usageData.getNum(); i++) {
    				// For each hour of usage...
    				Double usage = usageData.getData(i).get(tg);
    				Double cost = costData.getData(i).get(tg);
    				if (usage != null && cost != null) {
    					double onDemandRate = ec2Prices.getProduct(new InstancePrices.Key(tg.region, tg.usageType)).getOnDemandRate();
    					costData.getData(i).put(savingsTag, onDemandRate * usage - cost);
    				}
    			}
    		}
    	}
    }
    
    void cutData(int hours) {
        for (ReadWriteData data: usageDataByProduct.values()) {
            data.cutData(hours);
        }
        for (ReadWriteData data: costDataByProduct.values()) {
            data.cutData(hours);
        }
    }

    private void archive() throws Exception {

        logger.info("archiving tag data...");

        for (Product product: costDataByProduct.keySet()) {
            TagGroupWriter writer = new TagGroupWriter(product == null ? "all" : product.getFileName());
            writer.archive(startMilli, costDataByProduct.get(product).getTagGroups());
            // Debugging file output
            //writer.outputCsv(config.localDir + "/csv");
        }

        logger.info("archiving summary data...");

        archiveSummary(usageDataByProduct, "usage_");
        archiveSummary(costDataByProduct, "cost_");

        logger.info("archiving hourly data...");

        archiveHourly(usageDataByProduct, "usage_");
        archiveHourly(costDataByProduct, "cost_");
        
        logger.info("archiving instance data...");
        archiveInstances();
        
        logger.info("archiving data done.");
    }
    
    private void archiveInstances() throws Exception {
        DateTime monthDateTime = new DateTime(startMilli, DateTimeZone.UTC);
        instances.archive(config, "instances_" + AwsUtils.monthDateFormat.print(monthDateTime)); 	
    }

    private void archiveHourly(Map<Product, ReadWriteData> dataMap, String prefix) throws Exception {
        DateTime monthDateTime = new DateTime(startMilli, DateTimeZone.UTC);
        for (Product product: dataMap.keySet()) {
            String prodName = product == null ? "all" : product.getFileName();
            DataWriter writer = new DataWriter(prefix + "hourly_" + prodName + "_" + AwsUtils.monthDateFormat.print(monthDateTime), false);
            writer.archive(dataMap.get(product));
        }
    }

    private void addValue(List<Map<TagGroup, Double>> list, int index, TagGroup tagGroup, double v) {
        Map<TagGroup, Double> map = ReadWriteData.getCreateData(list, index);
        Double existedV = map.get(tagGroup);
        map.put(tagGroup, existedV == null ? v : existedV + v);
    }


    private void archiveSummary(Map<Product, ReadWriteData> dataMap, String prefix) throws Exception {

        DateTime monthDateTime = new DateTime(startMilli, DateTimeZone.UTC);

        for (Product product: dataMap.keySet()) {

            String prodName = product == null ? "all" : product.getFileName();
            ReadWriteData data = dataMap.get(product);
            Collection<TagGroup> tagGroups = data.getTagGroups();

            // init daily, weekly and monthly
            List<Map<TagGroup, Double>> daily = Lists.newArrayList();
            List<Map<TagGroup, Double>> weekly = Lists.newArrayList();
            List<Map<TagGroup, Double>> monthly = Lists.newArrayList();

            // get last month data
            ReadWriteData lastMonthData = new DataWriter(prefix + "hourly_" + prodName + "_" + AwsUtils.monthDateFormat.print(monthDateTime.minusMonths(1)), true).getData();

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

            // archive daily
            int year = monthDateTime.getYear();
            DataWriter writer = new DataWriter(prefix + "daily_" + prodName + "_" + year, true);
            ReadWriteData dailyData = writer.getData();
            dailyData.setData(daily, monthDateTime.getDayOfYear() -1, false);
            writer.archive();

            // archive monthly
            writer = new DataWriter(prefix + "monthly_" + prodName, true);
            ReadWriteData monthlyData = writer.getData();
            monthlyData.setData(monthly, Months.monthsBetween(config.startDate, monthDateTime).getMonths(), false);
            writer.archive();

            // archive weekly
            writer = new DataWriter(prefix + "weekly_" + prodName, true);
            ReadWriteData weeklyData = writer.getData();
            DateTime weekStart = monthDateTime.withDayOfWeek(1);
            int index;
            if (!weekStart.isAfter(config.startDate))
                index = 0;
            else
                index = Weeks.weeksBetween(config.startDate, weekStart).getWeeks() + (config.startDate.dayOfWeek() == weekStart.dayOfWeek() ? 0 : 1);
            weeklyData.setData(weekly, index, true);
            writer.archive();
        }
    }

    void init() {
        usageDataByProduct = new HashMap<Product, ReadWriteData>();
        costDataByProduct = new HashMap<Product, ReadWriteData>();
        usageDataByProduct.put(null, new ReadWriteData());
        costDataByProduct.put(null, new ReadWriteData());
        instances = new Instances();
    }


    private Map<Long, Map<Ec2InstanceReservationPrice.Key, Double>> getOndemandCosts(long fromMillis) {
        Map<Long, Map<Ec2InstanceReservationPrice.Key, Double>> ondemandCostsByHour = Maps.newHashMap();
        ReadWriteData costs = costDataByProduct.get(null);

        Collection<TagGroup> tagGroups = costs.getTagGroups();
        for (int i = 0; i < costs.getNum(); i++) {
            Long millis = startMilli + i * AwsUtils.hourMillis;
            if (millis < fromMillis)
                continue;

            Map<Ec2InstanceReservationPrice.Key, Double> ondemandCosts = Maps.newHashMap();
            ondemandCostsByHour.put(millis, ondemandCosts);

            Map<TagGroup, Double> data = costs.getData(i);
            for (TagGroup tagGroup : tagGroups) {
                if (tagGroup.product.isEc2Instance() && tagGroup.operation == Operation.ondemandInstances &&
                    data.get(tagGroup) != null) {
                    Ec2InstanceReservationPrice.Key key = new Ec2InstanceReservationPrice.Key(tagGroup.region, tagGroup.usageType);
                    if (ondemandCosts.get(key) != null)
                        ondemandCosts.put(key, data.get(tagGroup) + ondemandCosts.get(key));
                    else
                        ondemandCosts.put(key, data.get(tagGroup));
                }
            }
        }

        return ondemandCostsByHour;
    }

    private void updateLastMillis(long millis, String filename) {
        AmazonS3Client s3Client = AwsUtils.getAmazonS3Client();
        s3Client.putObject(config.workS3BucketName, config.workS3BucketPrefix + filename, IOUtils.toInputStream(millis + ""), new ObjectMetadata());
    }

    private Long getLastMillis(String filename) {
        AmazonS3Client s3Client = AwsUtils.getAmazonS3Client();
        InputStream in = null;
        try {
            in = s3Client.getObject(config.workS3BucketName, config.workS3BucketPrefix + filename).getObjectContent();
            return Long.parseLong(IOUtils.toString(in));
        }
        catch (Exception e) {
            logger.error("Error reading from file " + filename, e);
            return 0L;
        }
        finally {
            if (in != null)
                try {in.close();} catch (Exception e){}
        }
    }

    private Long lastProcessTime(String timeStr) {
        return getLastMillis("lastProcessMillis_" + timeStr);
    }

    private void updateProcessTime(String timeStr, long millis) {
        updateLastMillis(millis, "lastProcessMillis_" + timeStr);
    }

    private Long lastAlertMillis() {
        return getLastMillis("ondemandAlertMillis");
    }

    private void updateLastAlertMillis(Long millis) {
        updateLastMillis(millis, "ondemandAlertMillis");
    }

    private void sendOndemandCostAlert() {

        if (ondemandThreshold == null || StringUtils.isEmpty(fromEmail) || StringUtils.isEmpty(alertEmails) ||
            endMilli < lastAlertMillis() + AwsUtils.hourMillis * 24)
            return;

        Map<Long, Map<Ec2InstanceReservationPrice.Key, Double>> ondemandCosts = getOndemandCosts(lastAlertMillis() + AwsUtils.hourMillis);
        Long maxHour = null;
        double maxTotal = ondemandThreshold;

        for (Long hour: ondemandCosts.keySet()) {
            double total = 0;
            for (Double value: ondemandCosts.get(hour).values())
                total += value;

            if (total > maxTotal) {
                maxHour = hour;
                maxTotal = total;
            }
        }

        if (maxHour != null) {
            NumberFormat numberFormat = NumberFormat.getNumberInstance(Locale.US);
            String subject = String.format("Alert: Ondemand cost per hour reached $%s at %s",
                    numberFormat.format(maxTotal), AwsUtils.dateFormatter.print(maxHour));
            StringBuilder body = new StringBuilder();
            body.append(String.format("Total ondemand cost $%s at %s:<br><br>",
                    numberFormat.format(maxTotal), AwsUtils.dateFormatter.print(maxHour)));
            TreeMap<Double, String> costs = Maps.newTreeMap();
            for (Map.Entry<Ec2InstanceReservationPrice.Key, Double> entry: ondemandCosts.get(maxHour).entrySet()) {
                costs.put(entry.getValue(), entry.getKey().region + " " + entry.getKey().usageType + ": ");
            }
            for (Double cost: costs.descendingKeySet()) {
                if (cost > 0)
                    body.append(costs.get(cost)).append("$" + numberFormat.format(cost)).append("<br>");
            }
            body.append("<br>Please go to <a href=\"" + urlPrefix + "dashboard/reservation#usage_cost=cost&groupBy=UsageType&product=ec2_instance&operation=OndemandInstances\">Ice</a> for details.");
            SendEmailRequest request = new SendEmailRequest();
            request.withSource(fromEmail);
            List<String> emails = Lists.newArrayList(alertEmails.split(","));
            request.withDestination(new Destination(emails));
            request.withMessage(new Message(new Content(subject), new Body().withHtml(new Content(body.toString()))));

            AmazonSimpleEmailServiceClient emailService = AwsUtils.getAmazonSimpleEmailServiceClient();
            try {
                emailService.sendEmail(request);
                updateLastAlertMillis(endMilli);
                logger.info("updateLastAlertMillis " + endMilli);
            }
            catch (Exception e) {
                logger.error("Error in sending alert emails", e);
            }
        }
    }

}

