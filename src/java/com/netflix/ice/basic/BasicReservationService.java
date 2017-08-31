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

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.Poller;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.Ec2InstanceReservationPrice;
import com.netflix.ice.processor.Ec2InstanceReservationPrice.*;
import com.netflix.ice.processor.CanonicalReservedInstances;
import com.netflix.ice.processor.ProcessorConfig;
import com.netflix.ice.processor.ReservationService;
import com.netflix.ice.tag.*;
import com.netflix.ice.tag.Region;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class BasicReservationService extends Poller implements ReservationService {
    protected Logger logger = LoggerFactory.getLogger(BasicReservationService.class);
    protected ProcessorConfig config;
    protected Map<ReservationUtilization, Map<Ec2InstanceReservationPrice.Key, Ec2InstanceReservationPrice>> ec2InstanceReservationPrices;
    protected Map<ReservationUtilization, Map<TagGroup, List<Reservation>>> reservations;
    protected ReservationPeriod term;
    protected ReservationUtilization defaultUtilization;
    protected Map<ReservationUtilization, File> files;
    protected Long futureMillis = new DateTime().withYearOfCentury(99).getMillis();
    private boolean hasEc2Reservations;
    private boolean hasRdsReservations;
    private boolean hasRedshiftReservations;

    protected static Map<String, String> instanceTypes = Maps.newHashMap();
    protected static Map<String, String> instanceSizes = Maps.newHashMap();
    static {
        instanceTypes.put("stdResI", "m1");
        instanceTypes.put("secgenstdResI", "m3");
        instanceTypes.put("uResI", "t1");
        instanceTypes.put("hiMemResI", "m2");
        instanceTypes.put("hiCPUResI", "c1");
        instanceTypes.put("clusterCompResI", "cc1");
        instanceTypes.put("clusterHiMemResI", "cr1");
        instanceTypes.put("clusterGPUResI", "cg1");
        instanceTypes.put("hiIoResI", "hi1");
        instanceTypes.put("hiStoreResI", "hs1");

        instanceSizes.put("xxxxxxxxl", "8xlarge");
        instanceSizes.put("xxxxl", "4xlarge");
        instanceSizes.put("xxl", "2xlarge");
        instanceSizes.put("xl", "xlarge");
        instanceSizes.put("sm", "small");
        instanceSizes.put("med", "medium");
        instanceSizes.put("lg", "large");
        instanceSizes.put("u", "micro");
    }

    public BasicReservationService(ReservationPeriod term, ReservationUtilization defaultUtilization) {
        this.term = term;
        this.defaultUtilization = defaultUtilization;

        ec2InstanceReservationPrices = Maps.newHashMap();
        for (ReservationUtilization utilization: ReservationUtilization.values()) {
            ec2InstanceReservationPrices.put(utilization, new ConcurrentSkipListMap<Ec2InstanceReservationPrice.Key, Ec2InstanceReservationPrice>());
        }

        reservations = Maps.newHashMap();
        for (ReservationUtilization utilization: ReservationUtilization.values()) {
            reservations.put(utilization, Maps.<TagGroup, List<Reservation>>newHashMap());
        }
    }

    public void init() {
        this.config = ProcessorConfig.getInstance();
        files = Maps.newHashMap();
        for (ReservationUtilization utilization: ReservationUtilization.values()) {
            files.put(utilization,  new File(config.localDir, "reservation_prices." + term.name() + "." + utilization.name()));
        }

        boolean fileExisted = false;
        for (ReservationUtilization utilization: ReservationUtilization.values()) {
            File file = files.get(utilization);
            AwsUtils.downloadFileIfNotExist(config.workS3BucketName, config.workS3BucketPrefix, file);
            fileExisted = file.exists();
        }
        if (!fileExisted) {
            try {
                pollAPI();
            }
            catch (Exception e) {
                logger.error("failed to poll reservation prices", e);
                throw new RuntimeException("failed to poll reservation prices for " + e.getMessage());
            }
        }
        else {
            for (ReservationUtilization utilization: ReservationUtilization.values()) {
                try {
                    File file = files.get(utilization);
                    if (file.exists()) {
                        DataInputStream in = new DataInputStream(new FileInputStream(file));
                        ec2InstanceReservationPrices.put(utilization, Serializer.deserialize(in));
                        in.close();
                    }
                }
                catch (Exception e) {
                    throw new RuntimeException("failed to load reservation prices for " + utilization.name() + ", " + e.getMessage());
                }
            }
        }
        
        start(3600, 3600*24, true);
    }
    
    /**
     * Methods to indicate that we have reservations for each corresponding service.
     */
    public boolean hasEc2Reservations() {
    	return this.hasEc2Reservations;
    }
    public boolean hasRdsReservations() {
    	return this.hasRdsReservations;
    }
    public boolean hasRedshiftReservations() {
    	return this.hasRedshiftReservations;
    }

    @Override
    protected void poll() throws Exception {
        logger.info("start polling reservation prices. it might take a while...");
        pollAPI();
    }

    private void pollAPI() throws Exception {
        long currentTime = new DateTime().withTimeAtStartOfDay().getMillis();

        // We only want one price for the region since all AZs should be the same,
        // so we ask for the Region scoped reservations (New in Nov. 2016).
        Integer duration = 3600 * 24 * 365 * (this.term == Ec2InstanceReservationPrice.ReservationPeriod.oneyear ? 1 : 3);
        DescribeReservedInstancesOfferingsRequest req =  new DescribeReservedInstancesOfferingsRequest()
                .withFilters(new com.amazonaws.services.ec2.model.Filter().withName("marketplace").withValues("false"),
                		     new com.amazonaws.services.ec2.model.Filter().withName("scope").withValues("Region"),
                		     new com.amazonaws.services.ec2.model.Filter().withName("duration").withValues(duration.toString()));
        String token = null;
        boolean hasNewPrice = false;
        
        for (Region region: Region.getAllRegions()) {
            AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard()
            		.withCredentials(AwsUtils.awsCredentialsProvider)
            		.withClientConfiguration(AwsUtils.clientConfig)
            		.withRegion(region.name)
            		.build();

            logger.info("Setting RI prices for " + region.name);
            do {
                if (!StringUtils.isEmpty(token))
                    req.setNextToken(token);
                DescribeReservedInstancesOfferingsResult offers = ec2.describeReservedInstancesOfferings(req);
                token = offers.getNextToken();

                for (ReservedInstancesOffering offer: offers.getReservedInstancesOfferings()) {
                    ReservationUtilization utilization = ReservationUtilization.get(offer.getOfferingType());

                    double hourly = offer.getUsagePrice();
                    if (hourly <= 0) {
                        for (RecurringCharge recurringCharge: offer.getRecurringCharges()) {
                            if (recurringCharge.getFrequency().equals("Hourly")) {
                                hourly = recurringCharge.getAmount();
                                break;
                            }
                        }
                    }
                    UsageType usageType = getUsageType(offer.getInstanceType(), offer.getProductDescription());
                    hasNewPrice = setPrice(utilization, currentTime, region, usageType,
                                offer.getFixedPrice(), hourly) || hasNewPrice;

                    logger.debug("Setting RI price for " + region + " " + usageType + " " + utilization + " " + offer.getFixedPrice() + " " + hourly);
                }
            } while (!StringUtils.isEmpty(token));
            ec2.shutdown();
        }

        if (hasNewPrice) {
            for (ReservationUtilization utilization: files.keySet()) {
                File file = files.get(utilization);
                DataOutputStream out = new DataOutputStream(new FileOutputStream(file));
                try {
                    Serializer.serialize(out, this.ec2InstanceReservationPrices.get(utilization));
                    AwsUtils.upload(config.workS3BucketName, config.workS3BucketPrefix, file);
                }
                finally {
                    out.close();
                }
            }
        }
    }

    private UsageType getUsageType(String type, String productDescription) {
        return UsageType.getUsageType(type + InstanceOs.withDescription(productDescription).usageType, Operation.reservedInstancesHeavy, "");
    }

//    private UsageType getUsageType(String type, String size, boolean isWindows) {
//        type = instanceTypes.get(type);
//        size = instanceSizes.get(size);
//
//        if (type.equals("cc1") && size.equals("8xlarge"))
//            type = "cc2";
//        return UsageType.getUsageType(type + "." + size + (isWindows ? "." + InstanceOs.windows : ""), Operation.reservedInstances, "");
//    }

    public static class Serializer {
        public static void serialize(DataOutput out,
                                     Map<Ec2InstanceReservationPrice.Key, Ec2InstanceReservationPrice> reservationPrices)
                throws IOException {

            out.writeInt(reservationPrices.size());
            for (Ec2InstanceReservationPrice.Key key: reservationPrices.keySet()) {
                Ec2InstanceReservationPrice.Key.Serializer.serialize(out, key);
                Ec2InstanceReservationPrice.Serializer.serialize(out, reservationPrices.get(key));
            }
        }

        public static Map<Ec2InstanceReservationPrice.Key, Ec2InstanceReservationPrice> deserialize(DataInput in)
                throws IOException {

            int size = in.readInt();
            Map<Ec2InstanceReservationPrice.Key, Ec2InstanceReservationPrice> result =
                    new ConcurrentSkipListMap<Ec2InstanceReservationPrice.Key, Ec2InstanceReservationPrice>();
            for (int i = 0; i < size; i++) {
                Ec2InstanceReservationPrice.Key key = Ec2InstanceReservationPrice.Key.Serializer.deserialize(in);
                Ec2InstanceReservationPrice price = Ec2InstanceReservationPrice.Serializer.deserialize(in);
                result.put(key, price);
            }

            return result;
        }
    }

    private boolean setPrice(ReservationUtilization utilization, long currentTime, Region region, UsageType usageType, double upfront, double hourly) {

        Ec2InstanceReservationPrice.Key key = new Ec2InstanceReservationPrice.Key(region, usageType);
        Ec2InstanceReservationPrice reservationPrice = ec2InstanceReservationPrices.get(utilization).get(key);

        if (reservationPrice == null)  {
            reservationPrice = new Ec2InstanceReservationPrice();
            ec2InstanceReservationPrices.get(utilization).put(key, reservationPrice);
        }

        Ec2InstanceReservationPrice.Price latestHourly = reservationPrice.hourlyPrice.getCreatePrice(futureMillis);
        Ec2InstanceReservationPrice.Price latestUpfront = reservationPrice.upfrontPrice.getCreatePrice(futureMillis);

        if (latestHourly.getListPrice() == null) {
            latestHourly.setListPrice(hourly);
            latestUpfront.setListPrice(upfront);

            //logger.info("setting reservation price for " + usageType + " in " + region + ": " + upfront + " "  + hourly);
            return true;
        }
        else if (latestHourly.getListPrice() != hourly || latestUpfront.getListPrice() != upfront) {
            Ec2InstanceReservationPrice.Price oldHourly = reservationPrice.hourlyPrice.getCreatePrice(currentTime);
            Ec2InstanceReservationPrice.Price oldUpfront = reservationPrice.upfrontPrice.getCreatePrice(currentTime);
            oldHourly.setListPrice(latestHourly.getListPrice());
            oldUpfront.setListPrice(latestUpfront.getListPrice());

            latestHourly.setListPrice(hourly);
            latestUpfront.setListPrice(upfront);

            //logger.info("changing reservation price for " + usageType + " in " + region + ": " + upfront + " "  + hourly);
            return true;
        }
        else {
            //logger.info("existing reservation price for " + usageType + " in " + region + ": " + upfront + " "  + hourly);
            return false;
        }
    }

    public static class Reservation {
        final int count;
        final long start; // Reservation start time rounded down to starting hour mark where it takes effect
        final long end; // Reservation end time rounded down to ending hour mark where reservation actually ends
        final ReservationUtilization utilization;
        final double hourlyFixedPrice; // The hourly fixed price - used to compute amortization
        final double usagePrice; // usage price plus the recurring hourly charge

        public Reservation(
                int count,
                long start,
                long end,
                ReservationUtilization utilization,
                double hourlyFixedPrice,
                double usagePrice) {
            this.count = count;
            this.start = start;
            this.end = end;
            this.utilization = utilization;
            this.hourlyFixedPrice = hourlyFixedPrice;
            this.usagePrice = usagePrice;
        }
    }

    protected double getEc2Tier(long time) {
        return 0;
    }

    public Collection<TagGroup> getTagGroups(ReservationUtilization utilization) {
        return reservations.get(utilization).keySet();
    }

    public ReservationUtilization getDefaultReservationUtilization(long time) {
        return defaultUtilization;
    }

    public double getLatestHourlyTotalPrice(
            long time,
            Region region,
            UsageType usageType,
            ReservationUtilization utilization) {
        Ec2InstanceReservationPrice ec2Price =
            ec2InstanceReservationPrices.get(utilization).get(new Ec2InstanceReservationPrice.Key(region, usageType));

        double tier = getEc2Tier(time);
        return ec2Price.hourlyPrice.getPrice(null).getPrice(tier) +
               ec2Price.upfrontPrice.getPrice(null).getUpfrontAmortized(time, term, tier);
    }
    
    public ReservationInfo getReservation(
        long time,
        TagGroup tagGroup,
        ReservationUtilization utilization) {

	    double tier = getEc2Tier(time);
	
	    double upfrontAmortized = 0;
	    double hourlyCost = 0;
	
	    int count = 0;
	    if (this.reservations.get(utilization).containsKey(tagGroup)) {
	        for (Reservation reservation : this.reservations.get(utilization).get(tagGroup)) {
	            if (time >= reservation.start && time < reservation.end) {
	                count += reservation.count;
	
	                upfrontAmortized += reservation.count * reservation.hourlyFixedPrice;
	                hourlyCost += reservation.count * reservation.usagePrice;
	            }
	        }
	    }
	    else {
	        logger.debug("Not able to find " + utilization.name() + " reservation at " + AwsUtils.dateFormatter.print(time) + " for " + tagGroup);
	    }
	    
	    if (count == 0) {
	    	//logger.info("No active reservation for tagGroup: " + tagGroup);
	    	
	    	// Either we didn't find the reservation, or there is no longer an active reservation
	    	// for this usage. Pull the prices from the pricelist.
	        if (tagGroup.product.isEc2Instance()) {
		        Ec2InstanceReservationPrice.Key key = new Ec2InstanceReservationPrice.Key(tagGroup.region, tagGroup.usageType);
		        Ec2InstanceReservationPrice ec2Price = ec2InstanceReservationPrices.get(utilization).get(key);
		        if (ec2Price != null) { // remove this...
		            upfrontAmortized = ec2Price.upfrontPrice.getPrice(null).getUpfrontAmortized(time, term, tier);
		            hourlyCost = ec2Price.hourlyPrice.getPrice(null).getPrice(tier);
		        }
		        else {
		        	// We get here if we're looking up a price for a long retired reservation such as for a "Light Utilization"
		        	// which is no longer offered.
		            //logger.error("Not able to find EC2 reservation price for " + utilization.name() + " " + key);
		        }
	    	}
	    }
	    else {
	        upfrontAmortized = upfrontAmortized / count;
	        hourlyCost = hourlyCost / count;
	    }
	    
	    return new ReservationInfo(count, upfrontAmortized, hourlyCost);
	}
    
    private long getEffectiveReservationTime(Date d) {
    	Calendar c = new GregorianCalendar();
    	c.setTime(d);
    	c.set(Calendar.MINUTE, 0);
    	c.set(Calendar.SECOND, 0);
    	c.set(Calendar.MILLISECOND, 0);
    	return c.getTime().getTime();
    }
        
    public void updateReservations(Map<ReservationKey, CanonicalReservedInstances> reservationsFromApi, AccountService accountService, long startMillis, ProductService productService) {
        Map<ReservationUtilization, Map<TagGroup, List<Reservation>>> reservationMap = Maps.newTreeMap();
        for (ReservationUtilization utilization: ReservationUtilization.values()) {
            reservationMap.put(utilization, Maps.<TagGroup, List<Reservation>>newHashMap());
        }
        hasEc2Reservations = false;
        hasRdsReservations = false;
        hasRedshiftReservations = false;

        for (ReservationKey key: reservationsFromApi.keySet()) {
            CanonicalReservedInstances reservedInstances = reservationsFromApi.get(key);
            if (reservedInstances.getInstanceCount() <= 0) {
            	//logger.info("Reservation: " + reservedInstances.getReservationId() + ", type: " + reservedInstances.getInstanceType() + " has no instances");
                continue;
            }

            Account account = accountService.getAccountById(key.account);

            ReservationUtilization utilization = ReservationUtilization.get(reservedInstances.getOfferingType());
            
            // AWS reservations start at the beginning of the hour in which the reservation was purchased.
            // Likewise, they end at the start of the hour as well.
            long startTime = getEffectiveReservationTime(reservedInstances.getStart());
            long endTime = getEffectiveReservationTime(reservedInstances.getEnd());
            endTime = Math.min(endTime, startTime + reservedInstances.getDuration() * 1000);
            if (endTime <= startMillis) {
            	//logger.info("Reservation: " + reservedInstances.getReservationId() + ", type: " + reservedInstances.getInstanceType() + " has expired");
                continue;
            }
            
            // usage price is the sum of the usage price and the recurring hourly charge
            double usagePrice = reservedInstances.getUsagePrice() + reservedInstances.getRecurringHourlyCharges();
            double fixedPrice = reservedInstances.getFixedPrice();

            // logger.info("Reservation: " + reservedInstances.getReservationId() + ", type: " + reservedInstances.getInstanceType() + ", fixedPrice: " + fixedPrice);
            
            double hourlyFixedPrice = fixedPrice / (reservedInstances.getDuration() / 3600); // duration is in seconds, we need hours
            Reservation reservation = new Reservation(reservedInstances.getInstanceCount(), startTime, endTime, utilization, hourlyFixedPrice, usagePrice);

            UsageType usageType;
            Product product;
            Zone zone = null;
            Region region = Region.getRegionByName(reservedInstances.getRegion());
            
            if (reservedInstances.isEC2()) {
            	if (reservedInstances.getScope().equals("Availability Zone")) {
	                zone = Zone.getZone(reservedInstances.getAvailabilityZone());
	                if (zone == null)
	                    logger.error("Not able to find zone for EC2 reserved instances " + reservedInstances.getAvailabilityZone());
            	}
            	else if (!reservedInstances.getScope().equals("Region")) {
            		logger.error("Unknown scope value for reservation: " + reservedInstances.getReservationId() + ", scope: " + reservedInstances.getScope());
            	}
                String osStr = reservedInstances.getProductDescription();
                InstanceOs os = InstanceOs.withDescription(osStr);
                usageType = UsageType.getUsageType(reservedInstances.getInstanceType() + os.usageType, "hours");
                product = productService.getProductByName(Product.ec2Instance);
                hasEc2Reservations = true;
            }
            else if (reservedInstances.isRDS()) {
            	InstanceDb db = InstanceDb.withDescription(reservedInstances.getProductDescription());
            	String multiAZ = reservedInstances.getMultiAZ() ? ".multiaz" : "";
            	usageType = UsageType.getUsageType(reservedInstances.getInstanceType() + multiAZ + db.usageType, "hours");
            	product = productService.getProductByName(Product.rdsInstance);
            	hasRdsReservations = true;
            }
            else if (reservedInstances.isRedshift()){
            	usageType = UsageType.getUsageType(reservedInstances.getInstanceType(), "hours");
            	product = productService.getProductByName(Product.redshift);
            	hasRedshiftReservations = true;
            }
            else {
            	logger.error("Unknown reserved instance type: " + reservedInstances.getProduct() + ", " + reservedInstances.toString());
            	continue;
            }

            TagGroup reservationKey = new TagGroup(account, region, zone, product, Operation.getReservedInstances(utilization), usageType, null);

            List<Reservation> reservations = reservationMap.get(utilization).get(reservationKey);
            if (reservations == null) {
                reservationMap.get(utilization).put(reservationKey, Lists.<Reservation>newArrayList(reservation));
            }
            else {
                reservations.add(reservation);
            }
            //logger.info("Add reservation " + utilization.name() + " for key " + reservationKey.toString());

        }

        this.reservations = reservationMap;
    }
}
