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

import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeReservedInstancesResult;
import com.amazonaws.services.ec2.model.ReservedInstances;
import com.amazonaws.services.rds.AmazonRDSClient;
import com.amazonaws.services.rds.model.DescribeReservedDBInstancesResult;
import com.amazonaws.services.rds.model.ReservedDBInstance;
import com.amazonaws.services.redshift.AmazonRedshiftClient;
import com.amazonaws.services.redshift.model.DescribeReservedNodesResult;
import com.amazonaws.services.redshift.model.ReservedNode;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.Poller;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Region;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Class to poll reservation capacities.
 */
public class ReservationCapacityPoller extends Poller {
    private boolean updatedConfig = false;
    
    private static final String ec2 		= "ec2";
    private static final String rds 		= "rds";
    private static final String redshift 	= "redshift";

    public boolean updatedConfig() {
        return updatedConfig;
    }

    @Override
    protected void poll() throws Exception {
        ProcessorConfig config = ProcessorConfig.getInstance();

        Map<String, CanonicalReservedInstances> reservations = readArchive(config);
        
        for (Entry<Account, Set<String>> entry: config.accountService.getReservationProducts().entrySet()) {
            Account account = entry.getKey();
            Set<String> products = entry.getValue();
            logger.info("Get reservations for account: " + account.name);
            
        	try {
                String assumeRole = config.accountService.getReservationAccessRoles().get(account);
                if (assumeRole != null && assumeRole.isEmpty())
                	assumeRole = null;
                
                AWSSessionCredentials sessionCredentials = null;
                if (assumeRole != null) {
                    String externalId = config.accountService.getReservationAccessExternalIds().get(account);
                    final Credentials credentials = AwsUtils.getAssumedCredentials(account.id, assumeRole, externalId);
                	sessionCredentials = new AWSSessionCredentials() {
                        public String getAWSAccessKeyId() {
                            return credentials.getAccessKeyId();
                        }

                        public String getAWSSecretKey() {
                            return credentials.getSecretAccessKey();
                        }

                        public String getSessionToken() {
                            return credentials.getSessionToken();
                        }
                    };
                }
                
                for (Region region: Region.getAllRegions()) {

            	   
                   if (products.contains(ec2)) {
                       AmazonEC2Client ec2Client;
                       if (assumeRole != null) {
                           ec2Client = new AmazonEC2Client(sessionCredentials);
                       }
                       else {
                           ec2Client = new AmazonEC2Client(AwsUtils.awsCredentialsProvider.getCredentials(), AwsUtils.clientConfig);
                       }
                       ec2Client.setEndpoint("ec2." + region.name + ".amazonaws.com");

                       try {
                           DescribeReservedInstancesResult result = ec2Client.describeReservedInstances();
                           for (ReservedInstances reservation: result.getReservedInstances()) {
                        	   //logger.info("*** Reservation: " + reservation.getReservedInstancesId());
                               String key = account.id + "," + region.name + "," + reservation.getReservedInstancesId();
                               CanonicalReservedInstances cri = new CanonicalReservedInstances(account.id, region.name, reservation);
                               reservations.put(key, cri);
                           }
                       }
                       catch (Exception e) {
                           logger.error("error in describeReservedInstances for " + region.name + " " + account.name, e);
                       }
                       ec2Client.shutdown();
                   }
               	
   
                   if (products.contains(rds)) {
                       AmazonRDSClient rdsClient;
                       
                       if (assumeRole != null) {
                           rdsClient = new AmazonRDSClient(sessionCredentials);
                       }
                       else {
                           rdsClient = new AmazonRDSClient(AwsUtils.awsCredentialsProvider.getCredentials(), AwsUtils.clientConfig);
                       }
     
                       rdsClient.setEndpoint("rds." + region.name + ".amazonaws.com");

                       try {
                           DescribeReservedDBInstancesResult result = rdsClient.describeReservedDBInstances();
                           for (ReservedDBInstance reservation: result.getReservedDBInstances()) {
                               String key = account.id + "," + region.name + "," + reservation.getReservedDBInstanceId();
                               CanonicalReservedInstances cri = new CanonicalReservedInstances(account.id, region.name, reservation);
                               reservations.put(key, cri);
                           }
                       }
                       catch (Exception e) {
                           logger.error("error in describeReservedDBInstances for " + region.name + " " + account.name, e);
                       }
                       rdsClient.shutdown();
                   }
            	   
                   if (products.contains(redshift)) {
                       AmazonRedshiftClient redshiftClient;
                       
                       if (assumeRole != null) {
                           redshiftClient = new AmazonRedshiftClient(sessionCredentials);
                       }
                       else {
                           redshiftClient = new AmazonRedshiftClient(AwsUtils.awsCredentialsProvider.getCredentials(), AwsUtils.clientConfig);
                       }

	                   redshiftClient.setEndpoint("redshift." + region.name + ".amazonaws.com");
	
	                   try {
	                        DescribeReservedNodesResult result = redshiftClient.describeReservedNodes();
	                        for (ReservedNode reservation: result.getReservedNodes()) {
	                            String key = account.id + "," + region.name + "," + reservation.getReservedNodeId();
	                            CanonicalReservedInstances cri = new CanonicalReservedInstances(account.id, region.name, reservation);
	                            reservations.put(key, cri);
	                        }
	                   }
	                   catch (Exception e) {
	                        logger.error("error in describeReservedNodes for " + region.name + " " + account.name, e);
	                   }
	                   redshiftClient.shutdown();
                   }
	           }

            }
            catch (Exception e) {
                logger.error("Error in describeReservedInstances for " + account.name, e);
            }
        }

        config.reservationService.updateReservations(reservations, config.accountService, config.startDate.getMillis());
        updatedConfig = true;
        
        archive(config, reservations);
    }
    
    private Map<String, CanonicalReservedInstances> readArchive(ProcessorConfig config) {
        File file = new File(config.localDir, "reservation_capacity.txt");
        
        // read from s3 if not exists
        if (!file.exists()) {
            logger.info("downloading " + file + "...");
            AwsUtils.downloadFileIfNotExist(config.workS3BucketName, config.workS3BucketPrefix, file);
            logger.info("downloaded " + file);
        }

        // read from file
        Map<String, CanonicalReservedInstances> reservations = Maps.newTreeMap();
        if (file.exists()) {
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new FileReader(file));
                String line;

                while ((line = reader.readLine()) != null) {
                	CanonicalReservedInstances reservation = new CanonicalReservedInstances(line);
                	
                    reservations.put(reservation.getAccountId() + "," + reservation.getRegion() + "," + reservation.getReservationId(), reservation);
                }
            }
            catch (Exception e) {
                logger.error("error in reading " + file, e);
            }
            finally {
                if (reader != null)
                    try {reader.close();} catch (Exception e) {}
            }
        }
        logger.info("read " + reservations.size() + " reservations.");
        return reservations;
    }
    
    private void archive(ProcessorConfig config, Map<String, CanonicalReservedInstances> reservations) {
        File file = new File(config.localDir, "reservation_capacity.txt");

        // archive to disk
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(file));
            for (String key: reservations.keySet()) {
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
