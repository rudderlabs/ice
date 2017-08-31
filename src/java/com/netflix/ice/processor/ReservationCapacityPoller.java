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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DescribeReservedInstancesModificationsRequest;
import com.amazonaws.services.ec2.model.DescribeReservedInstancesModificationsResult;
import com.amazonaws.services.ec2.model.DescribeReservedInstancesResult;
import com.amazonaws.services.ec2.model.ReservedInstances;
import com.amazonaws.services.ec2.model.ReservedInstancesModification;
import com.amazonaws.services.ec2.model.ReservedInstancesModificationResult;
import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.AmazonRDSClientBuilder;
import com.amazonaws.services.rds.model.DescribeReservedDBInstancesResult;
import com.amazonaws.services.rds.model.ReservedDBInstance;
import com.amazonaws.services.redshift.AmazonRedshift;
import com.amazonaws.services.redshift.AmazonRedshiftClientBuilder;
import com.amazonaws.services.redshift.model.DescribeReservedNodesResult;
import com.amazonaws.services.redshift.model.ReservedNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.Poller;
import com.netflix.ice.processor.Ec2InstanceReservationPrice.ReservationUtilization;
import com.netflix.ice.processor.ReservationService.ReservationKey;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Region;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to poll reservation capacities.
 */
public class ReservationCapacityPoller extends Poller {
    private boolean updatedConfig = false;
    
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


    public boolean updatedConfig() {
        return updatedConfig;
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

    @Override
    protected void poll() throws Exception {
        ProcessorConfig config = ProcessorConfig.getInstance();

        Map<ReservationKey, CanonicalReservedInstances> reservations = readArchive(config);
        
        if (archiveLastModified < DateTime.now().minusHours(6).getMillis()) {
        
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
	                       catch (Exception e) {
	                           logger.error("error in describeReservedInstances for " + region.name + " " + account.name, e);
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
	                       catch (Exception e) {
	                           logger.error("error in describeReservedInstances for " + region.name + " " + account.name, e);
	                       }
	                       ec2.shutdown();
	                       handleEC2Modifications(ec2Reservations, mods);
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

        config.reservationService.updateReservations(reservations, config.accountService, config.startDate.getMillis(), config.productService);
        updatedConfig = true;        
    }
    
    protected void handleEC2Modifications(Map<ReservationKey, CanonicalReservedInstances> ec2Reservations, Ec2Mods mods) {
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
	        		logger.error("Reservation: " + reservedInstances.getReservationId() + ", type: " + reservedInstances.getInstanceType() + ", No parent reservation found" +
	        					mods.getMod(reservedInstances.getReservationId()));
	        		logger.error(mods.info());
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
            	Logger logger = LoggerFactory.getLogger(ReservationCapacityPoller.class);
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
}
