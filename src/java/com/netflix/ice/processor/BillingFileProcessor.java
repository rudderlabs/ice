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
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClient;
import com.amazonaws.services.simpleemail.model.*;
import com.csvreader.CsvReader;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.*;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Months;
import org.joda.time.Weeks;

import java.io.*;
import java.text.NumberFormat;
import java.util.*;

/**
 * Class to process billing files and produce tag, usage, cost output files for reader/UI.
 */
public class BillingFileProcessor extends Poller {

    private static Map<String, Double> ondemandRate = Maps.newHashMap();
    private ProcessorConfig config = ProcessorConfig.getInstance();
    private Long startMilli;
    private Long endMilli;
    private boolean processingMonitor;
    /**
     * The usageDataByProduct map holds both the usage data for each
     * individual product (if ResourceService is enabled) and a "null"
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
    
    private ReservationProcessor reservationProcessor;
    

    public BillingFileProcessor(String urlPrefix, Double ondemandThreshold, String fromEmail, String alertEmails) {
        this.ondemandThreshold = ondemandThreshold;
        this.fromEmail = fromEmail;
        this.alertEmails = alertEmails;
        this.urlPrefix = urlPrefix;
        
        reservationProcessor = new ReservationProcessor(config.accountService.getPayerAccounts(), config.accountService.getReservationAccounts().keySet());
    }

    @Override
    protected void poll() throws Exception {

        TreeMap<DateTime, List<BillingFile>> filesToProcess = Maps.newTreeMap();
        Map<DateTime, List<BillingFile>> monitorFilesToProcess = Maps.newTreeMap();

        // list the tar.gz file in billing file folder
        for (int i = 0; i < config.billingS3BucketNames.length; i++) {
            String billingS3BucketName = config.billingS3BucketNames[i];
            String billingS3BucketPrefix = config.billingS3BucketPrefixes.length > i ? config.billingS3BucketPrefixes[i] : "";
            String accountId = config.billingAccountIds.length > i ? config.billingAccountIds[i] : "";
            String billingAccessRoleName = config.billingAccessRoleNames.length > i ? config.billingAccessRoleNames[i] : "";
            String billingAccessExternalId = config.billingAccessExternalIds.length > i ? config.billingAccessExternalIds[i] : "";

            logger.info("trying to list objects in billing bucket " + billingS3BucketName + " using assume role, and external id "
                    + billingAccessRoleName + " " + billingAccessExternalId);
            List<S3ObjectSummary> objectSummaries = AwsUtils.listAllObjects(billingS3BucketName, billingS3BucketPrefix,
                    accountId, billingAccessRoleName, billingAccessExternalId);
            logger.info("found " + objectSummaries.size() + " in billing bucket " + billingS3BucketName);
            TreeMap<DateTime, S3ObjectSummary> filesToProcessInOneBucket = Maps.newTreeMap();
            Map<DateTime, S3ObjectSummary> monitorFilesToProcessInOneBucket = Maps.newTreeMap();

            // for each file, download&process if not needed
            for (S3ObjectSummary objectSummary : objectSummaries) {

                String fileKey = objectSummary.getKey();
                DateTime dataTime = AwsUtils.getDateTimeFromFileNameWithTags(fileKey);
                boolean withTags = true;
                if (dataTime == null) {
                    dataTime = AwsUtils.getDateTimeFromFileName(fileKey);
                    withTags = false;
                }

                if (dataTime != null && !dataTime.isBefore(config.startDate)) {
                    if (!filesToProcessInOneBucket.containsKey(dataTime) ||
                        withTags && config.resourceService != null || !withTags && config.resourceService == null)
                        filesToProcessInOneBucket.put(dataTime, objectSummary);
                    else
                        logger.info("ignoring file " + objectSummary.getKey());
                }
                else {
                    logger.info("ignoring file " + objectSummary.getKey());
                }
            }

            for (S3ObjectSummary objectSummary : objectSummaries) {
                String fileKey = objectSummary.getKey();
                DateTime dataTime = AwsUtils.getDateTimeFromFileNameWithMonitoring(fileKey);

                if (dataTime != null && !dataTime.isBefore(config.startDate)) {
                    monitorFilesToProcessInOneBucket.put(dataTime, objectSummary);
                }
            }

            for (DateTime key: filesToProcessInOneBucket.keySet()) {
                List<BillingFile> list = filesToProcess.get(key);
                if (list == null) {
                    list = Lists.newArrayList();
                    filesToProcess.put(key, list);
                }
                list.add(new BillingFile(filesToProcessInOneBucket.get(key), accountId, billingAccessRoleName, billingAccessExternalId, billingS3BucketPrefix));
            }

            for (DateTime key: monitorFilesToProcessInOneBucket.keySet()) {
                List<BillingFile> list = monitorFilesToProcess.get(key);
                if (list == null) {
                    list = Lists.newArrayList();
                    monitorFilesToProcess.put(key, list);
                }
                list.add(new BillingFile(monitorFilesToProcessInOneBucket.get(key), accountId, billingAccessRoleName, billingAccessExternalId, billingS3BucketPrefix));
            }
        }


        for (DateTime dataTime: filesToProcess.keySet()) {
            startMilli = endMilli = dataTime.getMillis();
            init();

            boolean hasNewFiles = false;
            boolean hasTags = false;
            long lastProcessed = lastProcessTime(AwsUtils.monthDateFormat.print(dataTime));

            for (BillingFile billingFile: filesToProcess.get(dataTime)) {
                S3ObjectSummary objectSummary = billingFile.s3ObjectSummary;
                if (objectSummary.getLastModified().getTime() < lastProcessed) {
                    logger.info("data has been processed. ignoring " + objectSummary.getKey() + "...");
                    continue;
                }
                hasNewFiles = true;
            }

            if (!hasNewFiles) {
                logger.info("data has been processed. ignoring all files at " + AwsUtils.monthDateFormat.print(dataTime));
                continue;
            }

            long processTime = new DateTime(DateTimeZone.UTC).getMillis();
            for (BillingFile billingFile: filesToProcess.get(dataTime)) {

                S3ObjectSummary objectSummary = billingFile.s3ObjectSummary;
                String fileKey = objectSummary.getKey();

                File file = new File(config.localDir, fileKey.substring(billingFile.prefix.length()));
                logger.info("trying to download " + fileKey + "...");
                boolean downloaded = AwsUtils.downloadFileIfChangedSince(objectSummary.getBucketName(), billingFile.prefix, file, lastProcessed,
                        billingFile.accountId, billingFile.accessRoleName, billingFile.externalId);
                if (downloaded)
                    logger.info("downloaded " + fileKey);
                else {
                    logger.info("file already downloaded " + fileKey + "...");
                }

                logger.info("processing " + fileKey + "...");
                boolean withTags = fileKey.contains("with-resources-and-tags");
                hasTags = hasTags || withTags;
                processingMonitor = false;
                processBillingZipFile(file, withTags);
                logger.info("done processing " + fileKey);
            }

            if (monitorFilesToProcess.get(dataTime) != null) {
                for (BillingFile monitorBillingFile: monitorFilesToProcess.get(dataTime)) {

                    S3ObjectSummary monitorObjectSummary = monitorBillingFile.s3ObjectSummary;
                    if (monitorObjectSummary != null) {
                        String monitorFileKey = monitorObjectSummary.getKey();
                        logger.info("processing " + monitorFileKey + "...");
                        File monitorFile = new File(config.localDir, monitorFileKey.substring(monitorFileKey.lastIndexOf("/") + 1));
                        logger.info("trying to download " + monitorFileKey + "...");
                        boolean downloaded = AwsUtils.downloadFileIfChangedSince(monitorObjectSummary.getBucketName(), monitorBillingFile.prefix, monitorFile, lastProcessed,
                                monitorBillingFile.accountId, monitorBillingFile.accessRoleName, monitorBillingFile.externalId);
                        if (downloaded)
                            logger.info("downloaded " + monitorFile);
                        else
                            logger.warn(monitorFile + "already downloaded...");
                        FileInputStream in = new FileInputStream(monitorFile);
                        try {
                            processingMonitor = true;
                            processBillingFile(monitorFile.getName(), in, true);
                        }
                        catch (Exception e) {
                            logger.error("Error processing " + monitorFile, e);
                        }
                        finally {
                            in.close();
                        }
                    }
                }
            }

            if (dataTime.equals(filesToProcess.lastKey())) {
                int hours = (int) ((endMilli - startMilli)/3600000L);
                logger.info("cut hours to " + hours);
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
            	if (utilization == Ec2InstanceReservationPrice.ReservationUtilization.LIGHT ||
            			utilization == Ec2InstanceReservationPrice.ReservationUtilization.MEDIUM)
            		continue;
            	
            	reservationProcessor.process(utilization, config.reservationService, usageDataByProduct.get(null), costDataByProduct.get(null), startMilli);
            }

            /***** Debugging */
//            used = costMap.get(redshiftHeavyTagGroup);
//            logger.info("First hour cost is " + used + " for " + redshiftHeavyTagGroup + " after reservation processing");

            if (hasTags && config.resourceService != null)
                config.resourceService.commit();

            logger.info("archiving results for " + dataTime + "...");
            archive();
            logger.info("done archiving " + dataTime);

            updateProcessTime(AwsUtils.monthDateFormat.print(dataTime), processTime);
            if (dataTime.equals(filesToProcess.lastKey())) {
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

    private void cutData(int hours) {
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
            TagGroupWriter writer = new TagGroupWriter(product == null ? "all" : product.name);
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
            String prodName = product == null ? "all" : product.name;
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

            String prodName = product == null ? "all" : product.name;
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

    private void init() {
        usageDataByProduct = new HashMap<Product, ReadWriteData>();
        costDataByProduct = new HashMap<Product, ReadWriteData>();
        usageDataByProduct.put(null, new ReadWriteData());
        costDataByProduct.put(null, new ReadWriteData());
        instances = new Instances();
    }

    private void processBillingZipFile(File file, boolean withTags) throws IOException {

        InputStream input = new FileInputStream(file);
        ZipArchiveInputStream zipInput = new ZipArchiveInputStream(input);

        try {
            ArchiveEntry entry;
            while ((entry = zipInput.getNextEntry()) != null) {
                if (entry.isDirectory())
                    continue;

                processBillingFile(entry.getName(), zipInput, withTags);
            }
        }
        catch (IOException e) {
            if (e.getMessage().equals("Stream closed"))
                logger.info("reached end of file.");
            else
                logger.error("Error processing " + file, e);
        }
        finally {
            try {
                zipInput.close();
            } catch (IOException e) {
                logger.error("Error closing " + file, e);
            }
            try {
                input.close();
            }
            catch (IOException e1) {
                logger.error("Cannot close input for " + file, e1);
            }
        }
    }

    private void processBillingFile(String fileName, InputStream tempIn, boolean withTags) {

        CsvReader reader = new CsvReader(new InputStreamReader(tempIn), ',');

        long lineNumber = 0;
        List<String[]> delayedItems = Lists.newArrayList();
        try {
            reader.readRecord();
            String[] headers = reader.getValues();

            config.lineItemProcessor.initIndexes(config, withTags, headers);

            while (reader.readRecord()) {
                String[] items = reader.getValues();
                try {
                    processOneLine(delayedItems, items);
                }
                catch (Exception e) {
                    logger.error(StringUtils.join(items, ","), e);
                }
                lineNumber++;

                if (lineNumber % 500000 == 0) {
                    logger.info("processed " + lineNumber + " lines...");
                }
//                if (lineNumber == 40000000) {//100000000      //
//                    break;
//                }
            }
        }
        catch (IOException e ) {
            logger.error("Error processing " + fileName + " at line " + lineNumber, e);
        }
        finally {
            try {
                reader.close();
            }
            catch (Exception e) {
                logger.error("Cannot close BufferedReader...", e);
            }
        }

        for (String[] items: delayedItems) {
            processOneLine(null, items);
        }
    }

    private void processOneLine(List<String[]> delayedItems, String[] items) {

        LineItemProcessor.Result result = config.lineItemProcessor.process(startMilli, delayedItems == null, config, items, usageDataByProduct, costDataByProduct, ondemandRate, instances);

        if (result == LineItemProcessor.Result.delay) {
            delayedItems.add(items);
        }
        else if (result == LineItemProcessor.Result.hourly && !processingMonitor) {
            endMilli = Math.max(endMilli, config.lineItemProcessor.getEndMillis(items));
        }
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
                if (tagGroup.product == Product.ec2_instance && tagGroup.operation == Operation.ondemandInstances &&
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

    private class BillingFile {
        final S3ObjectSummary s3ObjectSummary;
        final String accountId;
        final String accessRoleName;
        final String externalId;
        final String prefix;

        BillingFile(S3ObjectSummary s3ObjectSummary, String accountId, String accessRoleName, String externalId, String prefix) {
            this.s3ObjectSummary = s3ObjectSummary;
            this.accountId = accountId;
            this.accessRoleName = accessRoleName;
            this.externalId = externalId;
            this.prefix = prefix;
        }
    }
}

