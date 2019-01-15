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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.AmazonEC2Exception;
import com.amazonaws.services.ec2.model.DescribeReservedInstancesModificationsRequest;
import com.amazonaws.services.ec2.model.DescribeReservedInstancesModificationsResult;
import com.amazonaws.services.ec2.model.DescribeReservedInstancesResult;
import com.amazonaws.services.ec2.model.ReservedInstances;
import com.amazonaws.services.ec2.model.ReservedInstancesModification;
import com.amazonaws.services.ec2.model.ReservedInstancesModificationResult;
import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.AmazonRDSClientBuilder;
import com.amazonaws.services.rds.model.AmazonRDSException;
import com.amazonaws.services.rds.model.DescribeReservedDBInstancesResult;
import com.amazonaws.services.rds.model.ReservedDBInstance;
import com.amazonaws.services.redshift.AmazonRedshift;
import com.amazonaws.services.redshift.AmazonRedshiftClientBuilder;
import com.amazonaws.services.redshift.model.AmazonRedshiftException;
import com.amazonaws.services.redshift.model.DescribeReservedNodesResult;
import com.amazonaws.services.redshift.model.ReservedNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.Poller;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.pricelist.InstancePrices;
import com.netflix.ice.processor.pricelist.InstancePrices.LeaseContractLength;
import com.netflix.ice.processor.pricelist.InstancePrices.OfferingClass;
import com.netflix.ice.processor.pricelist.InstancePrices.PurchaseOption;
import com.netflix.ice.processor.pricelist.InstancePrices.Rate;
import com.netflix.ice.processor.pricelist.InstancePrices.ServiceCode;
import com.netflix.ice.processor.pricelist.PriceListService;
import com.netflix.ice.processor.CanonicalReservedInstances;
import com.netflix.ice.processor.ProcessorConfig;
import com.netflix.ice.processor.ReservationService;
import com.netflix.ice.tag.*;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.*;
import java.util.Map.Entry;

public class BasicReservationService extends Poller implements ReservationService {
    protected ProcessorConfig config;
    protected boolean runCapacityPoller;
    protected Map<ReservationUtilization, Map<TagGroup, List<Reservation>>> reservations;
    protected Map<String, Reservation> reservationsById;
    protected ReservationPeriod term;
    protected ReservationUtilization defaultUtilization;
    protected Long futureMillis = new DateTime().withYearOfCentury(99).getMillis();
    private boolean hasEc2Reservations;
    private boolean hasRdsReservations;
    private boolean hasRedshiftReservations;

    private static final String ec2 		= "ec2";
    private static final String rds 		= "rds";
    private static final String redshift 	= "redshift";
    
    private static final String archiveFilename = "reservation_capacity.csv";
    private long archiveLastModified = 0;
    
	static Map<String, Double> instanceSizeMap = Maps.newHashMap();
	static String[] sizes = new String[]{
		"nano",
		"micro",
		"small",
		"medium",
		"large",
		"xlarge",
	};
	{
		// Load the multiplier map
		for (int i = 0; i < sizes.length; i++)
			instanceSizeMap.put(sizes[i], (double) (1 << i));
	}

    public BasicReservationService(ReservationPeriod term, ReservationUtilization defaultUtilization, boolean runCapacityPoller) {
        this.term = term;
        this.defaultUtilization = defaultUtilization;
        this.runCapacityPoller = runCapacityPoller;

        reservations = Maps.newHashMap();
        for (ReservationUtilization utilization: ReservationUtilization.values()) {
            reservations.put(utilization, Maps.<TagGroup, List<Reservation>>newHashMap());
        }
        reservationsById = Maps.newHashMap();
    }

    public void init() throws Exception {
        this.config = ProcessorConfig.getInstance();
        if (runCapacityPoller) {
        	poll();
        	start(3600, 3600, false);
        }
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

    public static class Reservation {
    	final String id;
    	final TagGroup tagGroup;
        final int count;
        final long start; // Reservation start time rounded down to starting hour mark where it takes effect
        final long end; // Reservation end time rounded down to ending hour mark where reservation actually ends
        final ReservationUtilization utilization;
        final double hourlyFixedPrice; // The hourly fixed price - used to compute amortization
        final double usagePrice; // usage price plus the recurring hourly charge

        public Reservation(
        		String id,
        		TagGroup tagGroup,
                int count,
                long start,
                long end,
                ReservationUtilization utilization,
                double hourlyFixedPrice,
                double usagePrice) {
        	this.id = id;
        	this.tagGroup = tagGroup;
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

    public Collection<TagGroup> getTagGroups(ReservationUtilization utilization, Long startMilli) {
    	// Only return tagGroups with active reservations for the requested start time
    	Set<TagGroup> tagGroups = Sets.newHashSet();
    	for (TagGroup t: reservations.get(utilization).keySet()) {
    		List<Reservation> resList = reservations.get(utilization).get(t);
    		for (Reservation r: resList) {
	            if (startMilli >= r.start && startMilli < r.end) {
	            	tagGroups.add(t);
	            	break;
	            }
    		}
    	}
        return tagGroups;
    }

    public ReservationUtilization getDefaultReservationUtilization(long time) {
        return defaultUtilization;
    }

    public double getLatestHourlyTotalPrice(
            long time,
            Region region,
            UsageType usageType,
            PurchaseOption purchaseOption,
            ServiceCode serviceCode,
            InstancePrices prices) {
    	
		try {
			if (prices == null) {
				logger.error("No prices for " + serviceCode + " at " + AwsUtils.dateFormatter.print(time));
				return 0.0;
			}
			LeaseContractLength lcl = LeaseContractLength.getByYears(term.years);
			Rate rate = prices.getReservationRate(region, usageType, lcl, purchaseOption, OfferingClass.standard);
			double hourlyFixed = rate.getHourlyUpfrontAmortized(lcl);
	        return hourlyFixed + rate.hourly;
		} catch (Exception e) {
			logger.error("No reservation rate for " + purchaseOption + " " + usageType + " in " + region + " at " + AwsUtils.dateFormatter.print(time) + " " + serviceCode);
			e.printStackTrace();
		}
        return 0.0;
    }
    
    /*
     * Get ReservationInfo for the given reservation id
     */
    public ReservationInfo getReservation(String id) {
    	Reservation reservation = reservationsById.get(id);
	    return new ReservationInfo(reservation.tagGroup, reservation.count, reservation.hourlyFixedPrice, reservation.usagePrice);
    }
    
    /*
     * Get the set of reservation IDs that are active for the given time.
     */
    public Set<String> getReservations(long time, Product product) {
    	Set<String> ids = Sets.newHashSet();
    	for (Reservation r: reservationsById.values()) {
    		if (time >= r.start && time < r.end && (product == null || r.tagGroup.product == product))
    			ids.add(r.id);
    	}
    	return ids;
    }
    
    public ReservationInfo getReservation(
        long time,
        TagGroup tagGroup,
        ReservationUtilization utilization,
        InstancePrices instancePrices) {

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
	    	// for this usage. Pull the prices from the price list.
	        if (tagGroup.product.isEc2Instance()) {
				try {
					Rate rate = instancePrices.getReservationRate(tagGroup.region, tagGroup.usageType, LeaseContractLength.getByYears(term.years), utilization.getPurchaseOption(), OfferingClass.standard);
					upfrontAmortized = rate.getHourlyUpfrontAmortized(LeaseContractLength.getByYears(term.years));
					hourlyCost = rate.hourly;
				} catch (Exception e) {
		            logger.error("Not able to find EC2 reservation price for " + utilization.name() + " " + tagGroup.usageType + " in " + tagGroup.region);
				}
	    	}
	    }
	    else {
	        upfrontAmortized = upfrontAmortized / count;
	        hourlyCost = hourlyCost / count;
	    }
	    
	    return new ReservationInfo(tagGroup, count, upfrontAmortized, hourlyCost);
	}
    
    private long getEffectiveReservationTime(Date d) {
    	Calendar c = new GregorianCalendar();
    	c.setTime(d);
    	c.set(Calendar.MINUTE, 0);
    	c.set(Calendar.SECOND, 0);
    	c.set(Calendar.MILLISECOND, 0);
    	return c.getTime().getTime();
    }

    @Override
    protected void poll() throws Exception {
        Map<ReservationKey, CanonicalReservedInstances> reservations = readArchive(config);
        
        if (archiveLastModified < DateTime.now().minusHours(6).getMillis()) {
        	pullReservations(reservations);
        }
        updateReservations(reservations, config.accountService, config.startDate.getMillis(), config.productService);
    }
    
    private void pullReservations(Map<ReservationKey, CanonicalReservedInstances> reservations) {
        
        AmazonEC2ClientBuilder ec2Builder = AmazonEC2ClientBuilder.standard().withClientConfiguration(AwsUtils.clientConfig);
        AmazonRDSClientBuilder rdsBuilder = AmazonRDSClientBuilder.standard().withClientConfiguration(AwsUtils.clientConfig);
        AmazonRedshiftClientBuilder redshiftBuilder = AmazonRedshiftClientBuilder.standard().withClientConfiguration(AwsUtils.clientConfig);       
        
        for (Entry<Account, Set<String>> entry: config.accountService.getReservationAccounts().entrySet()) {
            Account account = entry.getKey();
            Set<String> products = entry.getValue();
            logger.info("Get reservations for account: " + account.name);
            
        	try {
                String assumeRole = config.accountService.getReservationAccessRoles().get(account);
                if (assumeRole != null && assumeRole.isEmpty())
                	assumeRole = null;
                
                AWSCredentialsProvider credentialsProvider = AwsUtils.awsCredentialsProvider;
                if (assumeRole != null) {
                    String externalId = config.accountService.getReservationAccessExternalIds().get(account);
                    credentialsProvider = AwsUtils.getAssumedCredentialsProvider(account.id, assumeRole, externalId);
                }
                
                for (Region region: Region.getAllRegions()) {

            	   
                   if (products.contains(ec2)) {
                	   AmazonEC2 ec2 = ec2Builder.withRegion(region.name).withCredentials(credentialsProvider).build();
                	   Map<ReservationKey, CanonicalReservedInstances> ec2Reservations = Maps.newTreeMap();
                	   
                	   // Start by getting any reservation modifications so that we can later use them to track down
                	   // the fixed price of modified Partial Upfront or All Upfront reservations. AWS doesn't carry
                	   // the fixed price to the modified reservation, but we need that to compute amortization.
                	   List<ReservedInstancesModification> modifications = Lists.newArrayList();
                	   try {
                		   DescribeReservedInstancesModificationsResult modResult = ec2.describeReservedInstancesModifications();
                		   
                		   modifications.addAll(modResult.getReservedInstancesModifications());
                		   while (modResult.getNextToken() != null) {
                			   modResult = ec2.describeReservedInstancesModifications(new DescribeReservedInstancesModificationsRequest().withNextToken(modResult.getNextToken()));
                    		   modifications.addAll(modResult.getReservedInstancesModifications());
                		   }
                	   }
                       catch(AmazonEC2Exception e) {
                    	   logger.info("could not get EC2 reservation modifications for " + region + " " + account.name + ", " + e.getErrorMessage());
                       }
                       catch (Exception e) {
                           logger.error("error in describeReservedInstancesModifications for " + region.name + " " + account.name, e);
                       }
                	   Ec2Mods mods = new Ec2Mods(modifications);
                       try {
                    	   
                           DescribeReservedInstancesResult result = ec2.describeReservedInstances();
                           for (ReservedInstances reservation: result.getReservedInstances()) {
                        	   //logger.info("*** Reservation: " + reservation.getReservedInstancesId());
                               ReservationKey key = new ReservationKey(account.id, region.name, reservation.getReservedInstancesId());
                               
                               CanonicalReservedInstances cri = new CanonicalReservedInstances(
                            		   account.id, region.name, reservation, 
                            		   mods.getModResId(reservation.getReservedInstancesId()));
                               ec2Reservations.put(key, cri);
                           }
                       }
                       catch(AmazonEC2Exception e) {
                    	   logger.info("could not get EC2 reservations for " + region + " " + account.name + ", " + e.getErrorMessage());
                       }
                       catch (Exception e) {
                           logger.error("error in describeReservedInstances for " + region.name + " " + account.name, e);
                       }
                       ec2.shutdown();
                       handleEC2Modifications(ec2Reservations, mods, region, config.priceListService);
                       reservations.putAll(ec2Reservations);
                   }
               	
   
                   if (products.contains(rds)) {
                       AmazonRDS rds = rdsBuilder.withRegion(region.name).withCredentials(credentialsProvider).build();
                       try {
                           DescribeReservedDBInstancesResult result = rds.describeReservedDBInstances();
                           for (ReservedDBInstance reservation: result.getReservedDBInstances()) {
                        	   ReservationKey key = new ReservationKey(account.id, region.name, reservation.getReservedDBInstanceId());
                               CanonicalReservedInstances cri = new CanonicalReservedInstances(account.id, region.name, reservation);
                               reservations.put(key, cri);
                           }
                       }
                       catch(AmazonRDSException e) {
                    	   logger.info("could not get RDS reservations for " + region + " " + account.name + ", " + e.getErrorMessage());
                       }
                       catch (Exception e) {
                           logger.error("error in describeReservedDBInstances for " + region.name + " " + account.name, e);
                       }
                       rds.shutdown();
                   }
            	   
                   if (products.contains(redshift)) {
                       AmazonRedshift redshift = redshiftBuilder.withRegion(region.name).withCredentials(credentialsProvider).build();
                       	
	                   try {
	                        DescribeReservedNodesResult result = redshift.describeReservedNodes();
	                        for (ReservedNode reservation: result.getReservedNodes()) {
	                            ReservationKey key = new ReservationKey(account.id, region.name, reservation.getReservedNodeId());
	                            CanonicalReservedInstances cri = new CanonicalReservedInstances(account.id, region.name, reservation);
	                            reservations.put(key, cri);
	                        }
	                   }
                       catch(AmazonRedshiftException e) {
                    	   logger.info("could not get Redshift reservations for " + region + " " + account.name + ", " + e.getErrorMessage());
                       }
	                   catch (Exception e) {
	                        logger.error("error in describeReservedNodes for " + region.name + " " + account.name, e);
	                   }
	                   redshift.shutdown();
                   }
	           }

            }
            catch (Exception e) {
                logger.error("Error in describeReservedInstances for " + account.name, e);
            }
        }
        archive(config, reservations);
    }

    protected void handleEC2Modifications(Map<ReservationKey, CanonicalReservedInstances> ec2Reservations, Ec2Mods mods, Region region, PriceListService pls) {
    	for (ReservationKey key: ec2Reservations.keySet()) {
    		CanonicalReservedInstances reservedInstances = ec2Reservations.get(key);
    		
	        double fixedPrice = reservedInstances.getFixedPrice();
	        ReservationUtilization utilization = ReservationUtilization.get(reservedInstances.getOfferingType());
	
	        logger.debug("Reservation: " + reservedInstances.getReservationId() + ", type: " + reservedInstances.getInstanceType() + ", fixedPrice: " + fixedPrice);
	        
	        if (fixedPrice == 0.0 && 
	        		(utilization == ReservationUtilization.FIXED ||
	        		 utilization == ReservationUtilization.PARTIAL))  {
	        	// Reservation was likely modified and AWS doesn't carry forward the fixed price from the parent reservation
	        	// Get the reservation modification for this RI
	        	String parentReservationId = mods.getOriginalReservationId(reservedInstances.getReservationId());
	        	if (parentReservationId.equals(reservedInstances.getReservationId())) {
	        		if (reservedInstances.getOfferingClass().equals("standard")) {
	        			logger.error("Reservation: " + reservedInstances.getReservationId() + ", type: " + reservedInstances.getInstanceType() + ", No parent reservation found " +
	        					mods.getMod(reservedInstances.getReservationId()) + ", getting fixed price from price list");
	        		}
	        		double newFixedPrice = getFixedPrice(reservedInstances, region, pls);
	        		reservedInstances.setFixedPrice(newFixedPrice);
	        		continue;
	        	}
	        	ReservationKey parentKey = new ReservationKey(key.account, key.region, parentReservationId);
	        	CanonicalReservedInstances parentRI = ec2Reservations.get(parentKey);
	        	if (parentRI == null) {
	        		logger.error("Reservation: " + parentKey + " Not found in reservation list");
	        		continue;
	        	}
	        	/*
	        	 * If reservation instance size changed, adjust the price accordingly.
	        	 */
	        	double adjustedPrice = adjustPrice(parentRI.getInstanceType(), reservedInstances.getInstanceType(), parentRI.getFixedPrice());
	        	reservedInstances.setFixedPrice(adjustedPrice);
	        }
    	}
    }
    
    protected double getFixedPrice(CanonicalReservedInstances ri, Region region, PriceListService pls) {
    	double fixedPrice = 0.0;
		try {
			InstancePrices prices = pls.getPrices(new DateTime(ri.getStart()), ServiceCode.AmazonEC2);
			LeaseContractLength lcl = ri.getDuration() > 24 * 365 ? LeaseContractLength.threeyear : LeaseContractLength.oneyear;
			PurchaseOption po = PurchaseOption.getByName(ri.getOfferingType());
			OfferingClass oc = OfferingClass.valueOf(ri.getOfferingClass());
			UsageType ut = UsageType.getUsageType(ri.getInstanceType(), "hours");
			Rate rate = prices.getReservationRate(region, ut, lcl, po, oc);
			if (rate != null) {
				fixedPrice = rate.fixed;
			}
			else {
				logger.error("No rate for " + region + "," + ut + "," + lcl + "," + po + "," + oc + " in AmazonEC2 pricelist " + prices.getVersionId());
			}
		} catch (Exception e) {
			logger.error("Error trying to get pricing for reservation:" + e);
			e.printStackTrace();
		}
		return fixedPrice;
    }
    
    private double adjustPrice(String fromInstanceType, String toInstanceType, double price) {
    	String fromSize = fromInstanceType.split("\\.")[1];
    	String toSize = toInstanceType.split("\\.")[1];
    	return price * multiplier(toSize) / multiplier(fromSize);
    }
    
	double multiplier(String size) {
		Double m = instanceSizeMap.get(size);
		if (m == null) {
			if (size.endsWith("xlarge")) {
				double mult = Double.parseDouble(size.substring(0, size.lastIndexOf("xlarge")));
				return instanceSizeMap.get("xlarge") * mult;
			}
			logger.error("Cannot find multiplier for size: %s", size);
			return 1.0;
		}
		return m;
	}
    
    private Map<ReservationKey, CanonicalReservedInstances> readArchive(ProcessorConfig config) {
        File file = new File(config.localDir, archiveFilename);
        
        // read from s3 if not exists
        if (!file.exists()) {
            logger.info("downloading " + file + "...");
            AwsUtils.downloadFileIfNotExist(config.workS3BucketName, config.workS3BucketPrefix, file);
            logger.info("downloaded " + file);
        }
        
        archiveLastModified = file.lastModified();

        Map<ReservationKey, CanonicalReservedInstances> reservations = readReservations(file);
        logger.info("read " + reservations.size() + " reservations.");
        return reservations;
    }
    
    public static Map<ReservationKey, CanonicalReservedInstances> readReservations(File file) {
        // read from file
        Map<ReservationKey, CanonicalReservedInstances> reservations = Maps.newTreeMap();
        if (file.exists()) {
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new FileReader(file));
                String line;
                
                // skip the header
                reader.readLine();

                while ((line = reader.readLine()) != null) {
                	CanonicalReservedInstances reservation = new CanonicalReservedInstances(line);
                	
                    reservations.put(new ReservationKey(reservation.getAccountId(), reservation.getRegion(), reservation.getReservationId()), reservation);
                }
            }
            catch (Exception e) {
            	Logger logger = LoggerFactory.getLogger(ReservationService.class);
            	logger.error("error in reading " + file, e);
            }
            finally {
                if (reader != null)
                    try {reader.close();} catch (Exception e) {}
            }
        }
        return reservations;
    }
    
    private void archive(ProcessorConfig config, Map<ReservationKey, CanonicalReservedInstances> reservations) {
        File file = new File(config.localDir, archiveFilename);

        // archive to disk
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(file));
            writer.write(CanonicalReservedInstances.header());
            writer.newLine();
            for (ReservationKey key: reservations.keySet()) {
                CanonicalReservedInstances reservation = reservations.get(key);
                writer.write(reservation.toString());
                writer.newLine();
            }
        }
        catch (Exception e) {
            logger.error("",  e);
        }
        finally {
            if (writer != null)
                try {writer.close();} catch (Exception e) {}
        }
        logger.info("archived " + reservations.size() + " reservations.");

        // archive to s3
        logger.info("uploading " + file + "...");
        AwsUtils.upload(config.workS3BucketName, config.workS3BucketPrefix, config.localDir, file.getName());
        logger.info("uploaded " + file);
    }

    public void updateReservations(Map<ReservationKey, CanonicalReservedInstances> reservationsFromApi, AccountService accountService, long startMillis, ProductService productService) {
        Map<ReservationUtilization, Map<TagGroup, List<Reservation>>> reservationMap = Maps.newTreeMap();
        for (ReservationUtilization utilization: ReservationUtilization.values()) {
            reservationMap.put(utilization, Maps.<TagGroup, List<Reservation>>newHashMap());
        }
        Map<String, Reservation> reservationsByIdMap = Maps.newHashMap();

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
            
            long startTime = 0;
            long endTime = 0;
        	// AWS Reservations start being applied at the beginning of the hour in which the reservation was purchased.
            // The reservations stop being applied at the beginning of the hour in which the reservation expires.
            startTime = getEffectiveReservationTime(reservedInstances.getStart());
            endTime = getEffectiveReservationTime(reservedInstances.getEnd());
            
            // Modified reservations don't have updated durations
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

            TagGroup reservationKey = TagGroup.getTagGroup(account, region, zone, product, Operation.getReservedInstances(utilization), usageType, null);
            Reservation reservation = new Reservation(reservedInstances.getReservationId(), reservationKey, reservedInstances.getInstanceCount(), startTime, endTime, utilization, hourlyFixedPrice, usagePrice);

            List<Reservation> reservations = reservationMap.get(utilization).get(reservationKey);
            if (reservations == null) {
                reservationMap.get(utilization).put(reservationKey, Lists.<Reservation>newArrayList(reservation));
            }
            else {
                reservations.add(reservation);
            }
            reservationsByIdMap.put(reservation.id, reservation);

            //logger.info("Add reservation " + utilization.name() + " for key " + reservationKey.toString());

        }

        this.reservations = reservationMap;
        this.reservationsById = reservationsByIdMap;
    }
    
	public class Ec2Mods {
		private List<ReservedInstancesModification> mods;
		
		Ec2Mods(List<ReservedInstancesModification> mods) {
			this.mods = mods;
		}
		
		public ReservedInstancesModification getMod(String reservationId) {
			for (ReservedInstancesModification mod: mods) {
				if (mod.getStatus().equals("failed"))
					continue;
				
				for (ReservedInstancesModificationResult result: mod.getModificationResults()) {
					if (result.getReservedInstancesId() == null) {
						logger.error("getReservedInstancesId for " + mod.getReservedInstancesIds() + " return null: " + result);
						continue;
					}
					if (result.getReservedInstancesId().equals(reservationId))
						return mod;
				}
			}
			return null;
		}
		
		public String getModResId(String reservationId) {
			ReservedInstancesModification mod = getMod(reservationId);
			return mod == null ? "" : mod.getReservedInstancesIds().get(0).getReservedInstancesId();
		}
		
		public String getOriginalReservationId(String reservationId) {
			String childId = reservationId;
			for (ReservedInstancesModification modification = getMod(childId); modification != null; modification = getMod(childId)) {
				childId = modification.getReservedInstancesIds().get(0).getReservedInstancesId();
				logger.debug("Reservation " + reservationId + " has parent " + childId);
			}
			return childId;
		}
		
		public String info() {
			int fulfilled = 0;
			int failed = 0;
			
			for (ReservedInstancesModification mod: mods) {
				if (mod.getStatus().equals("failed"))
					failed++;
				if (mod.getStatus().equals("fulfilled"))
					fulfilled++;
			}
			return "Modifications: " + fulfilled + " fulfilled, " + failed + " failed, " + mods.size();
		}
	}

}
