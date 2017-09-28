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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.ReservationService.ReservationUtilization;
import com.netflix.ice.processor.pricelist.InstancePrices;
import com.netflix.ice.processor.pricelist.InstancePrices.ServiceCode;
import com.netflix.ice.processor.pricelist.PriceListService;
import com.netflix.ice.reader.InstanceMetrics;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.Zone;

public class ReservationProcessor {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    private final Map<Account, List<Account>> reservationBorrowers;
    
    // hour of data to print debug statements. Set to -1 to turn off all debug logging.
    private int debugHour = -1;
    private String debugFamily = "db";
    // debugAccounts - will print all accounts if null
    private String[] debugAccounts = null; // { "AccountName" };
    // debugUtilization - will print all utilization if null
    private ReservationUtilization debugUtilization = ReservationUtilization.PARTIAL;
    // debugRegion - will print all regions if null
    private Region[] debugRegions = { Region.AP_SOUTHEAST_2 };
    
    private ProductService productService;
    private PriceListService priceListService;
    
    // The following are initialzed on each call to process()
    private InstanceMetrics instanceMetrics = null;
    private Map<Product, InstancePrices> prices;

    public ReservationProcessor(Map<Account, List<Account>> payerAccounts, Set<Account> reservationOwners, ProductService productService, PriceListService priceListService) throws IOException {
    	this.productService = productService;
    	this.priceListService = priceListService;
    	
        // Initialize the reservation owner and borrower account lists
        reservationBorrowers = Maps.newHashMap();
        // Associate all the accounts in a consolidated billing group with the reservation owner
        Set<Account> payers = payerAccounts.keySet();
        for (Account owner: reservationOwners) {
        	// Find the owner account in the payerAccounts
        	for (Account payer: payers) {
        		if (payer.name.equals(owner.name)) {
            		// Owner is a payer account. Add all the linked accounts as reservationBorrowers
                    List<Account> list = payerAccounts.get(payer);
                    for (Account borrower: list) {
                        if (borrower.name.equals(owner.name))
                            continue;
                        addBorrower(owner, borrower);
                    }
                    break;
        		}
    			// Look for the owner in the linked account lists
    			boolean found = false;
    			for (Account linked: payerAccounts.get(payer)) {
    				if (linked.name.equals(owner.name)) {
    					found = true;
    					break;
    				}
    			}
    			if (found) {
					// Add the payer and all the other linked accounts to the reservationBorrowers
    				addBorrower(owner, payer);
    				for (Account borrower: payerAccounts.get(payer)) {
    					if (borrower.name.equals(owner.name))
    						continue;
    					addBorrower(owner, borrower);
    				}
    				break;
    			}
        	}
        }
    }
    
    private void addBorrower(Account owner, Account borrower) {
        List<Account> from = reservationBorrowers.get(borrower);
        if (from == null) {
            from = Lists.newArrayList();
            reservationBorrowers.put(borrower, from);
        }
        from.add(owner);
    }
    
    public void setDebugHour(int i) {
    	debugHour = i;
    }
    
    public int getDebugHour() {
    	return debugHour;
    }
    
    public void setDebugFamily(String family) {
    	debugFamily = family;
    }
    
    public String getDebugFamily() {
    	return debugFamily;
    }
    
    public void setDebugAccounts(String[] accounts) {
    	debugAccounts = accounts;
    }
    
    public String[] getDebugAccounts() {
    	return debugAccounts;
    }
    
    public void setDebugRegions(Region[] regions) {
    	debugRegions = regions;
    }
    
    public Region[] getDebugRegions() {
    	return debugRegions;
    }
    
	private boolean debugReservations(int i, TagGroup tg, ReservationUtilization utilization) {
		// Must have match on debugHour
		if (i != debugHour)
			return false;
		
		// Must have match on debugUtilization if it isn't null
		if (debugUtilization != null && utilization != debugUtilization)
			return false;
		
		// Must have match on region if debugRegions isn't null
		if (debugRegions != null) {
			boolean match = false;
			for (Region r: debugRegions) {
				if (r == tg.region) {
					match = true;
					break;
				}
			}
			if (!match)
				return false;
		}
		
		// Must have match on account if debugAccounts isn't null
		if (debugAccounts != null) {
			boolean match = false;
			for (String a: debugAccounts) {
				if (a.equals(tg.account.name)) {
					match = true;
					break;
				}
			}
			if (!match)
				return false;
		}
		
		if (debugFamily.isEmpty()) {
			return true;
		}
		String family = tg.usageType.name.split("\\.")[0];
		if (family.equals(debugFamily)) {
			return true;
		}
		return false;
	}
   
    private void borrow(int i, long time,
            Map<TagGroup, Double> usageMap,
            Map<TagGroup, Double> costMap,
            List<Account> fromAccounts,
            TagGroup tagGroup,
            ReservationUtilization utilization,
            ReservationService reservationService,
            Set<TagGroup> reservationTagGroups) {

	    boolean debug = debugReservations(i, tagGroup, utilization);
		Double existing = usageMap.get(tagGroup);

		if (debug)
			logger.info("      borrow from accounts: " + fromAccounts + ", existing: " + existing + " from tagGroup " + tagGroup);
		if (existing != null && fromAccounts != null) {
		
			// process borrowing of matching usage type
			for (Account from: fromAccounts) {
			    if (existing <= 0)
			        break;
			    
			    TagGroup unusedTagGroup = new TagGroup(from, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getUnusedInstances(utilization), tagGroup.usageType, null);
			    Double unused = usageMap.get(unusedTagGroup);
			
			    if (unused != null && unused > 0) {
			        if (debug) {
			        	logger.info("** borrow(" + i + ") up to: " + existing + " for: " + tagGroup);
			        	logger.info("       from: " + from + ", unused: " + unused + ", " + unusedTagGroup);
			        }
			        
			        Double resHourlyCost = costMap.get(unusedTagGroup);
			        double hourlyCost = resHourlyCost == null ? 0.0 : resHourlyCost / unused;
			
			        double reservedBorrowed = Math.min(existing, unused);
			        double reservedUnused = unused - reservedBorrowed;
			
			        existing -= reservedBorrowed;
			
			        TagGroup borrowedTagGroup = new TagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getBorrowedInstances(utilization), tagGroup.usageType, null);
			        TagGroup lentTagGroup = new TagGroup(from, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getLentInstances(utilization), tagGroup.usageType, null);
			
			        Double existingLent = usageMap.get(lentTagGroup);
			        double reservedLent = existingLent == null ? reservedBorrowed : reservedBorrowed + existingLent;
			        Double existingBorrowed = usageMap.get(borrowedTagGroup);
			        reservedBorrowed = existingBorrowed == null ? reservedBorrowed : reservedBorrowed + existingBorrowed;
			
			        usageMap.put(borrowedTagGroup, reservedBorrowed);
			        usageMap.put(lentTagGroup, reservedLent);
			        usageMap.put(tagGroup, existing);
			        
			        costMap.put(borrowedTagGroup, reservedBorrowed * hourlyCost);
			        costMap.put(lentTagGroup, reservedLent * hourlyCost);
			        costMap.put(tagGroup, existing * hourlyCost);			        			
			        
			        if (reservedUnused > 0) {
			        	usageMap.put(unusedTagGroup, reservedUnused);
			        	costMap.put(unusedTagGroup, reservedUnused * hourlyCost);
			        }
			        else {
			        	usageMap.remove(unusedTagGroup);
			        	costMap.remove(unusedTagGroup);
			        }
			        
			        if (debug) {
			        	logger.info("      borrowed  quantity: " + reservedBorrowed + ", tag: " + borrowedTagGroup);
			        	logger.info("      lent      quantity: " + reservedLent + ", tag: " + lentTagGroup);
			        	logger.info("      remaining quantity: " + existing + ", tag: " + tagGroup);
			            logger.info("      unused    quantity: " + reservedUnused + ", tag: " + unusedTagGroup);
			        }
			    }
			}
			
			// Now process family-based borrowing
			for (Account from: fromAccounts) {
			    if (existing <= 0)
			        break;
			    
			    // Scan all the regional reservations looking for matching account, region, and family with unused reservations
			    for (TagGroup rtg: reservationTagGroups) {
			    	if (rtg.zone != null || rtg.account != from || rtg.region != tagGroup.region || !sameFamily(rtg, tagGroup))
			    		continue;
			    	
			        TagGroup unusedRegionalTagGroup = new TagGroup(from, rtg.region, null, rtg.product, Operation.getUnusedInstances(utilization), rtg.usageType, null);
			        Double unused = usageMap.get(unusedRegionalTagGroup);
			
			        if (unused != null && unused > 0) {
			            if (debug) {
			               	logger.info("** family borrow(" + i + ") up to: " + existing + " for: " + tagGroup);
			            	logger.info("       from: " + from + ", unused: " + unused + ", " + rtg);
			            }
			            
				        Double resHourlyCost = costMap.get(unusedRegionalTagGroup);
				        double hourlyCost = resHourlyCost == null ? 0.0 : resHourlyCost / unused;
			
			            double adjustedUnused = convertFamilyUnits(unused, rtg.usageType, tagGroup.usageType);
			            double adjustedReservedBorrowed = Math.min(existing, adjustedUnused);
			            double reservedUnused = convertFamilyUnits(adjustedUnused - adjustedReservedBorrowed, tagGroup.usageType, rtg.usageType);
			            double reservedBorrowed = unused - reservedUnused;
			            
			            existing -= adjustedReservedBorrowed;
			           
			            TagGroup borrowedTagGroup = new TagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getBorrowedInstances(utilization), tagGroup.usageType, null);
			            TagGroup lentTagGroup = new TagGroup(from, rtg.region, rtg.zone, rtg.product, Operation.getLentInstances(utilization), rtg.usageType, null);
			            
			            // Lent is in reservation units
			            Double existingLent = usageMap.get(lentTagGroup);
			            double reservedLent = existingLent == null ? reservedBorrowed : reservedBorrowed + existingLent;
			
			
			            Double existingBorrowed = usageMap.get(borrowedTagGroup);
			            adjustedReservedBorrowed = existingBorrowed == null ? adjustedReservedBorrowed : adjustedReservedBorrowed + existingBorrowed;			            
			
			            usageMap.put(borrowedTagGroup, adjustedReservedBorrowed);			            
			            usageMap.put(lentTagGroup, reservedLent);
			            usageMap.put(tagGroup, existing);

			            // Borrowed is in actual usage units
			            double curBorrowedCost = reservedBorrowed * hourlyCost;
			            Double existingBorrowedCost = costMap.get(borrowedTagGroup);
			            double borrowedCost = existingBorrowedCost == null ? curBorrowedCost : curBorrowedCost + existingBorrowedCost;

			            costMap.put(lentTagGroup, reservedLent * hourlyCost);
			            costMap.put(borrowedTagGroup, borrowedCost);
			            costMap.put(tagGroup, existing * hourlyCost);
			            			

			            if (reservedUnused > 0) {
			            	usageMap.put(unusedRegionalTagGroup, reservedUnused);
			            	costMap.put(unusedRegionalTagGroup, reservedUnused * hourlyCost);
			            }
			            else {
			            	usageMap.remove(unusedRegionalTagGroup);
			            	costMap.remove(unusedRegionalTagGroup);
			            }
			            
			            if (debug) {
			            	logger.info("      borrowed  quantity: " + adjustedReservedBorrowed + ", tag: " + borrowedTagGroup);
			            	logger.info("      lent      quantity: " + reservedLent + ", tag: " + lentTagGroup);
			            	logger.info("      remaining quantity: " + existing + ", tag: " + tagGroup);
			                logger.info("      unused    quantity: " + reservedUnused + ", tag: " + unusedRegionalTagGroup);
			            }
			        }
			    }
			}
		}

		// the rest is bonus
		if (existing != null && existing > 0) {
	        TagGroup resTagGroup = new TagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getReservedInstances(utilization), tagGroup.usageType, null);
			ReservationService.ReservationInfo reservation = reservationService.getReservation(time, resTagGroup, utilization);
			TagGroup bonusTagGroup = new TagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getBonusReservedInstances(utilization), tagGroup.usageType, null);
			if (debug) {
				logger.info("** bonus(" + i + ") **   bonus     quantity: " + existing + ", tag: " + bonusTagGroup);
			}
			usageMap.put(bonusTagGroup, existing);
			if (reservation.reservationHourlyCost > 0)
				costMap.put(bonusTagGroup, existing * reservation.reservationHourlyCost);				
		}
		else {
			usageMap.remove(tagGroup);
			costMap.remove(tagGroup);
		}
	}

	private boolean sameFamily(TagGroup a, TagGroup b) {
		// True if both tags are ec2_instances and have the same usage type prefix
		return a.product.isEc2Instance() &&
			a.product == b.product &&
			a.usageType.name.split("\\.")[0].equals(b.usageType.name.split("\\.")[0]);
	}
	
	private double convertFamilyUnits(double units, UsageType from, UsageType to) {
		return units * instanceMetrics.getNormalizationFactor(from) / instanceMetrics.getNormalizationFactor(to);
	}

	private void family(int i, long time,
		Map<TagGroup, Double> usageMap,
		Map<TagGroup, Double> costMap,
		TagGroup tagGroup,
		ReservationUtilization utilization,
		Set<TagGroup> bonusTags) {
	
	    boolean debug = debugReservations(i, tagGroup, utilization);
		TagGroup unusedTagGroup = new TagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getUnusedInstances(utilization), tagGroup.usageType, null);
		Double unused = usageMap.get(unusedTagGroup);

		if (debug)
			logger.info("      family - " + bonusTags.size() + " bonus tags, unused: " + unused + " for tagGroup " + tagGroup);
		
		if (unused != null && unused > 0) {
	        Double resHourlyCost = costMap.get(unusedTagGroup);
	        double hourlyCost = resHourlyCost == null ? 0.0 : resHourlyCost / unused;
			
			if (debug) {
				logger.info("----- family(" + i + ")** unused: " + unused + ", tagGroup: " + unusedTagGroup);
			}
			
			// Scan bonus reservations for this account
			for (TagGroup tg: bonusTags) {
				// only look within the same account and region
				if (tg.account != tagGroup.account || tg.region != tagGroup.region)
					continue;
				
				// Don't process equivalent instance types within the owner account. That will have
				// already been done.
				if (tg.usageType == tagGroup.usageType)
					continue;
				
				if (sameFamily(tg, tagGroup)) {
			        if (debug) {
			        	logger.info("      found bonus: " + usageMap.get(tg) + ", tag: " + tg);
			        }
					// found a reservation that uses the unused portion
					Double used = usageMap.get(tg);
					if (used != null && used > 0) {
						double adjustedUsed = convertFamilyUnits(used, tg.usageType, tagGroup.usageType);
						double reservedUsed = Math.min(unused, adjustedUsed);
						double familyUsed = convertFamilyUnits(reservedUsed, tagGroup.usageType, tg.usageType);
						unused -= reservedUsed;
						
						used -= familyUsed;
						
				        TagGroup familyTagGroup = new TagGroup(tg.account, tg.region, tg.zone, tg.product, Operation.getFamilyReservedInstances(utilization), tg.usageType, null);
				        
				        // Allocated usage as a family reservation
				        
			            Double existingFamilyUsage = usageMap.get(familyTagGroup);
			            double totalFamilyUsage = existingFamilyUsage == null ? familyUsed : familyUsed + existingFamilyUsage;
			            
				        usageMap.put(familyTagGroup, totalFamilyUsage);			            
			            
			            Double existingFamilyCost = costMap.get(familyTagGroup);
			            double familyCost = reservedUsed * hourlyCost;
			            double totalFamilyCost = existingFamilyCost == null ? familyCost : familyCost + existingFamilyCost;
		
			        	costMap.put(familyTagGroup, totalFamilyCost);
				        
				        // What's left of bonus if any
				        if (used > 0) {
							usageMap.put(tg, used);
							costMap.put(tg, (adjustedUsed - reservedUsed) * hourlyCost);
				        }
				        else {
				        	usageMap.remove(tg);
				        	costMap.remove(tg);
				        }
						
			            if (debug) {
			            	logger.info("** family(" + i + ")** ");
			            	logger.info("      family    quantity: " + totalFamilyUsage + ", tag: " + familyTagGroup);
			            	logger.info("      bonus     quantity: " + used + ", tag: " + tg);
			                logger.info("      unused    quantity: " + unused + ", tag: " + unusedTagGroup);
			            }
					}
				}
			}
			if (debug)
				logger.info("      family - update unused: " + unused + " for unused tagGroup " + unusedTagGroup);
			// Updated whatever remains unused if any
			if (unused > 0) {
				usageMap.put(unusedTagGroup, unused);
				costMap.put(unusedTagGroup, unused * hourlyCost);
			}
			else {
				usageMap.remove(unusedTagGroup);
				costMap.remove(unusedTagGroup);
			}
		}
	}
	
	private class UsedUnused {
		public double used;
		public double unused;
		
		public UsedUnused(double used, double unused) {
			this.used = used;
			this.unused = unused;
		}
	}
	
	private UsedUnused processUsage(ReservationUtilization utilization,
			TagGroup tagGroup,
			Zone zone,
			Map<TagGroup, Double> usageMap,
			Map<TagGroup, Double> costMap,
			UsedUnused uu,
			double reservationHourlyCost) {
		
		TagGroup bonusTagGroup = new TagGroup(tagGroup.account, tagGroup.region, zone, tagGroup.product, Operation.getBonusReservedInstances(utilization), tagGroup.usageType, null);
		Double existing = usageMap.get(bonusTagGroup);
		double value = existing == null ? 0 : existing;
		
		double reservedUsedZone = Math.min(value, uu.unused);
	    if (reservedUsedZone > 0) {
    		uu.used += reservedUsedZone;
    		uu.unused -= reservedUsedZone;

    		TagGroup usedTagGroup = new TagGroup(tagGroup.account, tagGroup.region, zone, tagGroup.product, Operation.getReservedInstances(utilization), tagGroup.usageType, null);
    		Double usedExisting = usageMap.get(usedTagGroup);
    		double usedTotal = usedExisting == null ? reservedUsedZone : usedExisting + reservedUsedZone;
    		
	        usageMap.put(usedTagGroup, usedTotal);				        
	        costMap.put(usedTagGroup, usedTotal * reservationHourlyCost);
	        
	        // Now decrement the bonus
	        double bonus = value - reservedUsedZone;
	        if (bonus > 0) {
	        	usageMap.put(bonusTagGroup, bonus);
	        	costMap.put(bonusTagGroup, bonus * reservationHourlyCost);
	        }
	        else {
	        	usageMap.remove(bonusTagGroup);
	        	costMap.remove(bonusTagGroup);
	        }				        
	    }
	    return uu;
	}
	
	/*
	 * process() will run through all the usage data looking for reservation usage and
	 * associate it with the appropriate reservations found in the reservation owner
	 * accounts. It handles both AZ and Regional scoped reservations including borrowing
	 * across accounts linked through consolidated billing and sharing of instance reservations
	 * among instance types in the same family.
	 * 
	 * The order of processing is as follows:
	 *  1. AZ-scoped reservations used within the owner account.
	 *  2. AZ-scoped reservations borrowed by other accounts within the consolidated group.
	 *  3. Region-scoped reservations used within the owner account.
	 *  4. Region-scoped reservations used within the owner account but from a different instance type within the same family.
	 *  5. Region-scoped reservations borrowed by other accounts
	 *  6. Region-scoped reservations borrowed by other accounts but from a different instance type within the same family.
	 * 
	 * When called, all usage data is flagged as bonusReserved. The job of this method is to
	 * walk through all the bonus tags and convert them to the proper reservation usage type
	 * based on the association of the actual reservations. Since the detailed billing reports
	 * don't tell us which reservation was actually used for each line item, we mimic the AWS rules
	 * as best we can. The actual usage in AWS may be slightly different. Only the AWS Cost and Usage
	 * reports fully provide the reservation ARN that is associated with each usage, but ICE doesn't
	 * use those reports. (Added support recently for processing based on cost and usage reports, but
	 * haven't done the work to grab and use the reservation ARNs.)
	 * 
	 * Amortization of upfront costs is assigned to the reservations instance type in the account that purchased
	 * the reservation regarless of how it was used - either by a different family type or borrowed by another account.
	 * 
	 * Savings values, like amortization are assigned to the reservation instance type in the account that purchased
	 * the reservation. Savings is computed as the difference between the on-demand cost and the sum of the upfront amortization
	 * and hourly rate. Any unused instance costs are subtracted from the savings.
	 * 
	 * We initially calculate the max savings possible:
	 * 
	 * 	max_savings = reservation.capacity * (onDemand_rate - hourly_rate - hourly_amortization)
	 * 
	 * We then subtract off the onDemand cost of unused reservations to get actual savings
	 * 
	 * actual_savings = max_savings - (onDemand_rate * unusedCount)
	 */
	public void process(ReservationUtilization utilization,
			ReservationService reservationService,
			ReadWriteData usageData,
			ReadWriteData costData,
			DateTime start) throws Exception {
		
		if (reservationService.getTagGroups(utilization).size() == 0)
			return;
	
    	logger.info("---------- Process " + reservationService.getTagGroups(utilization).size() + " reservations for utilization: " + utilization);

		if (debugHour >= 0)
			printUsage("before", usageData, costData);
		
		// Initialize the price lists
    	prices = Maps.newHashMap();
    	prices.put(productService.getProductByName(Product.ec2Instance), priceListService.getPrices(start, ServiceCode.AmazonEC2));
    	if (reservationService.hasRdsReservations())
    		prices.put(productService.getProductByName(Product.rdsInstance), priceListService.getPrices(start, ServiceCode.AmazonRDS));
    	if (reservationService.hasRedshiftReservations())
    		prices.put(productService.getProductByName(Product.redshift), priceListService.getPrices(start, ServiceCode.AmazonRedshift));
		
    	instanceMetrics = priceListService.getInstanceMetrics();
		
		long startMilli = start.getMillis();
		processAvailabilityZoneReservations(utilization, reservationService, usageData, costData, startMilli);
		if (debugHour >= 0)
			printUsage("between AZ and Regional", usageData, costData);		
		processRegionalReservations(utilization, reservationService, usageData, costData, startMilli);
		removeUnusedFromSavings(utilization, usageData, costData);
		
		if (debugHour >= 0)
			printUsage("after", usageData, costData);		
	}
		
	private void processAvailabilityZoneReservations(ReservationUtilization utilization,
			ReservationService reservationService,
			ReadWriteData usageData,
			ReadWriteData costData,
			Long startMilli) {

		// first mark owner accounts
		// The reservationTagGroups set will contain all the tagGroups for reservation purchases.
		// The account tag is the owner of the reservation, the zone is null for regionally scoped RIs.
		// It does NOT have anything to do with usage.
		// Usage is saved in the usageData maps.
		// reservationTagGroups therefore, does not include any reserved instance usage for borrowed reservations or reservation usage by members of the same
		// family of instance. Family reservation usage will appear as bonus reservations if the account owns other RIs for
		// that same usage type.
		SortedSet<TagGroup> reservationTagGroups = Sets.newTreeSet();
		SortedSet<TagGroup> reservations = Sets.newTreeSet();
		reservations.addAll(reservationService.getTagGroups(utilization));
		
		if (debugHour >= 0)
			logger.info("--------------- processAvailabilityZoneReservations ----------- " + reservations.size() + " reservations");
		
		for (TagGroup tagGroup: reservations) {
			// For each of the owner AZ reservation tag groups...
			if (tagGroup.zone == null)
				continue;
			
		    TagGroup bonusTagGroup = new TagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getBonusReservedInstances(utilization), tagGroup.usageType, null);
		    TagGroup savingsTagGroup = new TagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getSavings(utilization), tagGroup.usageType, null);
	        TagGroup unusedTagGroup = new TagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getUnusedInstances(utilization), tagGroup.usageType, null);
	        TagGroup upfrontTagGroup = new TagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getUpfrontAmortized(utilization), tagGroup.usageType, null);
	        
	        double onDemandRate = prices.get(tagGroup.product).getOnDemandRate(tagGroup.region, tagGroup.usageType);
		    
			for (int i = 0; i < usageData.getNum(); i++) {
				// For each hour of usage...
			
			    Map<TagGroup, Double> usageMap = usageData.getData(i);
			    Map<TagGroup, Double> costMap = costData.getData(i);
			
			    // Get the reservation info for the utilization and tagGroup in the current hour
			    ReservationService.ReservationInfo reservation = reservationService.getReservation(startMilli + i * AwsUtils.hourMillis, tagGroup, utilization);
			    boolean debug = debugReservations(i, tagGroup, utilization);

			    // Do we have any usage from the current reservation?
			    // Usage is initially tagged as Bonus, then we work through the allocations.
			    Double existing = usageMap.get(bonusTagGroup);
			    double bonusReserved = existing == null ? 0 : existing;
			    
			    double reservedUnused = reservation.capacity;			    
			    double reservedUsed = Math.min(bonusReserved, reservedUnused);
			    reservedUnused -= reservedUsed;
			    bonusReserved -= reservedUsed;

			    if (reservedUsed > 0) {
			        usageMap.put(tagGroup, reservedUsed);
			        costMap.put(tagGroup, reservedUsed * reservation.reservationHourlyCost);
			    }
			    
			    if (debug) {
			    	logger.info("**** AZ reservation **** hour: " + i + ", existing: " + existing + ", bonusReserved: " + bonusReserved + ", used: " + reservedUsed + ", unused: " + reservedUnused + ", capacity: " + reservation.capacity + ", tagGroup: " + tagGroup);
			    }
			    
			    if (reservedUnused > 0) {
			        usageMap.put(unusedTagGroup, reservedUnused);
			        costMap.put(unusedTagGroup, reservedUnused * reservation.reservationHourlyCost);
			        if (debug) {
			        	logger.info("  ** Unused instances **** hour: " + i + ", used: " + reservedUsed + ", unused: " + reservedUnused + ", tag: " + unusedTagGroup);
			        }
			    }
			
		        usageMap.put(bonusTagGroup, bonusReserved);
		        costMap.put(bonusTagGroup, bonusReserved * reservation.reservationHourlyCost);
		        if (debug) {
		        	logger.info("  ** Bonus instances **** hour: " + i + ", bonus: " + bonusReserved + ", tag: " + bonusTagGroup);
		        }
			
			    if (reservation.capacity > 0 && reservation.upfrontAmortized > 0) {
			        costMap.put(upfrontTagGroup, reservation.capacity * reservation.upfrontAmortized);
			        
			        double savingsRate = onDemandRate - reservation.reservationHourlyCost - reservation.upfrontAmortized;
				    costMap.put(savingsTagGroup, reservation.capacity * savingsRate);
			    }
			}
			
			reservationTagGroups.add(new TagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getReservedInstances(utilization), tagGroup.usageType, null));
		}
		processFamilySharingAndBorrowing(utilization, reservationService, usageData, costData, startMilli, reservationTagGroups, false);
	}
	
	private void processFamilySharingAndBorrowing(ReservationUtilization utilization,
			ReservationService reservationService,
			ReadWriteData usageData,
			ReadWriteData costData,
			Long startMilli,
			Set<TagGroup> reservationTagGroups,
			boolean regional) {

		Operation bonusOperation = Operation.getBonusReservedInstances(utilization);
		
		if (regional) {
			Set<TagGroup> unassignedUsage = getUnassignedUsage(usageData, bonusOperation);
					
			if (debugHour >= 0)
				logger.info("--------------- process family-based sharing within same account ----------- " + unassignedUsage.size() + " unassigned tags");
			
			// Scan bonus reservations and handle non-zone-specific and family-based usage of regionally-scoped reservations
			// within each owner account (region-scoped reservations new as of 11/1/2016, family-based credits are new as of 3/1/2017)
			for (TagGroup tagGroup: reservationTagGroups) {
				// only process regional reservations
				if (tagGroup.zone != null)
					continue;
				
				for (int i = 0; i < usageData.getNum(); i++) {
				
				    Map<TagGroup, Double> usageMap = usageData.getData(i);
				    Map<TagGroup, Double> costMap = costData.getData(i);
				    
				    family(i, startMilli + i * AwsUtils.hourMillis, usageMap, costMap, tagGroup, utilization, unassignedUsage);
				}
			}
		}
		
		if (debugHour >= 0)
			printUsage("before processing family-based borrowing across accounts", usageData, costData);
			
		Set<TagGroup> unassignedUsage = getUnassignedUsage(usageData, bonusOperation);
		if (debugHour >= 0)
			logger.info("--------------- process family-based borrowing across accounts ----------- " + unassignedUsage.size() + " unassigned tags");
		
		for (TagGroup tagGroup: unassignedUsage) {
			for (int i = 0; i < usageData.getNum(); i++) {
			
			    Map<TagGroup, Double> usageMap = usageData.getData(i);
			    Map<TagGroup, Double> costMap = costData.getData(i);
			
			    borrow(i, startMilli + i * AwsUtils.hourMillis,
			    		usageMap, costMap,
			           reservationBorrowers.get(tagGroup.account),
			           tagGroup,
			           utilization,
			           reservationService,
			           reservationTagGroups);
			}
		}		
	}
		
	private void processRegionalReservations(ReservationUtilization utilization,
			ReservationService reservationService,
			ReadWriteData usageData,
			ReadWriteData costData,
			Long startMilli) {
		// Now spin through all the bonus reservations and allocate them to any regional reservations in the owner account.
		// Regional reservations include RDS and Redshift products.
		SortedSet<TagGroup> reservationTagGroups = Sets.newTreeSet();
		SortedSet<TagGroup> reservations = Sets.newTreeSet();
		reservations.addAll(reservationService.getTagGroups(utilization));
		if (debugHour >= 0)
			logger.info("--------------- processRegionalReservations ----------- " + reservations.size() + " reservations");
		for (TagGroup tagGroup: reservations) {
			// For each of the owner Region reservation tag groups...
			if (tagGroup.zone != null)
				continue;
			
	        TagGroup upfrontTagGroup = new TagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getUpfrontAmortized(utilization), tagGroup.usageType, null);
	        TagGroup savingsTagGroup = new TagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getSavings(utilization), tagGroup.usageType, null);
	        TagGroup unusedTagGroup = new TagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getUnusedInstances(utilization), tagGroup.usageType, null);
	        
	        double onDemandRate = prices.get(tagGroup.product).getOnDemandRate(tagGroup.region, tagGroup.usageType);
			
	        for (int i = 0; i < usageData.getNum(); i++) {
				// For each hour of usage...
			
			    Map<TagGroup, Double> usageMap = usageData.getData(i);
			    Map<TagGroup, Double> costMap = costData.getData(i);
			
			    // Get the reservation info for the utilization and tagGroup in the current hour
			    ReservationService.ReservationInfo reservation = reservationService.getReservation(startMilli + i * AwsUtils.hourMillis, tagGroup, utilization);
			    boolean debug = debugReservations(i, tagGroup, utilization);

			    UsedUnused uu = new UsedUnused(0.0, reservation.capacity);
			    
			    if (uu.unused > 0) {
				    // Do we have any usage from the current reservation?
				    // First check for region-based usage
				    uu = processUsage(utilization, tagGroup, null, usageMap, costMap, uu, reservation.reservationHourlyCost);

			    	// Check each of the AZs in the region
			    	for (Zone zone: tagGroup.region.getZones()) {
			    		if (uu.unused <= 0)
			    			break;
			    		
			    		uu = processUsage(utilization, tagGroup, zone, usageMap, costMap, uu, reservation.reservationHourlyCost);
				    }
			    }
			    if (debug) {
			    	logger.info("**** Region reservation **** hour: " + i + ", used: " + uu.used + ", capacity: " + reservation.capacity + ", tagGroup: " + tagGroup);
			    }
			    
			    if (uu.unused > 0) {
			        usageMap.put(unusedTagGroup, uu.unused);
			        costMap.put(unusedTagGroup, uu.unused * reservation.reservationHourlyCost);
			        if (debug) {
			        	logger.info("  ** Unused instances **** hour: " + i + ", used: " + uu.used + ", unused: " + uu.unused + ", tag: " + unusedTagGroup);
			        }
			    }
			    if (reservation.capacity > 0 && reservation.upfrontAmortized > 0) {
			        costMap.put(upfrontTagGroup, reservation.capacity * reservation.upfrontAmortized);
			        double savingsRate = onDemandRate - reservation.reservationHourlyCost - reservation.upfrontAmortized;
			        costMap.put(savingsTagGroup, reservation.capacity * savingsRate);
			    }			
			}
			reservationTagGroups.add(new TagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getReservedInstances(utilization), tagGroup.usageType, null));
		}
		
		if (debugHour >= 0)
			printUsage("before regional family sharing and borrowing", usageData, costData);
		processFamilySharingAndBorrowing(utilization, reservationService, usageData, costData, startMilli, reservationTagGroups, true);
	}
	
	private void removeUnusedFromSavings(
			ReservationUtilization utilization,
			ReadWriteData usageData,
			ReadWriteData costData) {
		
		Operation unusedOp = Operation.getUnusedInstances(utilization);
		
		for (TagGroup tagGroup: usageData.getTagGroups()) {
			if (tagGroup.operation != unusedOp)
				continue;
			
	        TagGroup savingsTagGroup = new TagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getSavings(utilization), tagGroup.usageType, null);
			
	        double onDemandRate = prices.get(tagGroup.product).getOnDemandRate(tagGroup.region, tagGroup.usageType);

	        for (int i = 0; i < usageData.getNum(); i++) {
				// For each hour of usage...
			
			    Map<TagGroup, Double> usageMap = usageData.getData(i);
			    Map<TagGroup, Double> costMap = costData.getData(i);
			    
			    Double unused = usageMap.get(tagGroup);
	        	if (unused != null && unused > 0.0) {
		        	Double savings = costMap.get(savingsTagGroup);
		        	if (savings == null) {
		        		logger.error("Savings record not found for " + tagGroup);
		        	}
		        	else {
		        		savings -= unused * onDemandRate;
		        		costMap.put(savingsTagGroup, savings);
		        	}
	        	}
	        }
		}
	}
	
	private Set<TagGroup> getUnassignedUsage(ReadWriteData usageData, Operation bonusOperation) {
		// Collect all tag groups for reserved instances not yet associated with a reservation.
		// They will appear as BonusReservedInstances.
		SortedSet<TagGroup> unassignedUsage = Sets.newTreeSet();
		SortedSet<TagGroup> usageTags = Sets.newTreeSet();
		usageTags.addAll(usageData.getTagGroups());
		for (TagGroup tagGroup: usageTags) {
			if (tagGroup.resourceGroup == null &&
			    tagGroup.product.isEc2Instance() &&
			    tagGroup.operation == bonusOperation) {
			
				unassignedUsage.add(tagGroup);
			}
		}
		return unassignedUsage;
	}
	
	
	private void printUsage(String when, ReadWriteData usageData, ReadWriteData costData) {
		logger.info("---------- usage and cost for hour " + debugHour + " " + when + " processing ----------------");
	    Map<TagGroup, Double> usageMap = usageData.getData(debugHour);
		
		SortedSet<TagGroup> usageTags = Sets.newTreeSet();
		usageTags.addAll(usageData.getTagGroups());
		for (TagGroup tagGroup: usageTags) {
			if (tagGroup.operation == Operation.ondemandInstances || tagGroup.operation == Operation.spotInstances)
				continue;
			if ((tagGroup.product.isEc2Instance() || tagGroup.product.isRdsInstance() || tagGroup.product.isRedshift())
					&& usageMap.get(tagGroup) != null 
					&& debugReservations(debugHour, tagGroup, ((Operation.ReservationOperation) tagGroup.operation).getUtilization())) {
				logger.info("usage " + usageMap.get(tagGroup) + " for tagGroup: " + tagGroup);
			}
		}

		Map<TagGroup, Double> costMap = costData.getData(debugHour);

		SortedSet<TagGroup> costTags = Sets.newTreeSet();
		costTags.addAll(costData.getTagGroups());
		for (TagGroup tagGroup: costTags) {
			if (tagGroup.operation == Operation.ondemandInstances || tagGroup.operation == Operation.spotInstances)
				continue;
			if ((tagGroup.product.isEc2Instance() || tagGroup.product.isRdsInstance() || tagGroup.product.isRedshift())
					&& costMap.get(tagGroup) != null 
					&& debugReservations(debugHour, tagGroup, ((Operation.ReservationOperation) tagGroup.operation).getUtilization())) {
				logger.info("cost " + costMap.get(tagGroup) + " for tagGroup: " + tagGroup);
			}
		}
		logger.info("---------- end of usage and cost report ----------------");
	}
}

