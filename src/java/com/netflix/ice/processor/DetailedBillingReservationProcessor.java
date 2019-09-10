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
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.ReservationService.ReservationUtilization;
import com.netflix.ice.processor.pricelist.InstancePrices;
import com.netflix.ice.processor.pricelist.PriceListService;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.Zone;

public class DetailedBillingReservationProcessor extends ReservationProcessor {
   protected Logger logger = LoggerFactory.getLogger(getClass());

   protected final Set<Account> reservationBorrowers;
   
   public DetailedBillingReservationProcessor(Set<Account> reservationOwners,
		   ProductService productService, PriceListService priceListService, boolean familyBreakout) throws IOException {
	   super(reservationOwners, productService, priceListService, familyBreakout);

		// Initialize the reservation owner and borrower account lists
		reservationBorrowers = Sets.newHashSet();
	}
   
   public void addBorrower(Account account) {
       reservationBorrowers.add(account);
   }
   
   public void clearBorrowers() {
		reservationBorrowers.clear();
   }
   
   private void borrow(int i, long time,
           Map<TagGroup, Double> usageMap,
           Map<TagGroup, Double> costMap,
           TagGroup tagGroup,
           ReservationUtilization utilization,
           ReservationService reservationService,
           Set<TagGroup> reservationTagGroups,
           InstancePrices instancePrices) {

	    boolean debug = debugReservations(i, tagGroup, utilization);
		Double existing = usageMap.get(tagGroup);

		if (debug)
			logger.info("      borrow from accounts: " + reservationBorrowers + ", existing: " + existing + " from tagGroup " + tagGroup);
		if (existing != null && !reservationBorrowers.isEmpty()) {
		
			// process borrowing of matching usage type
			for (Account from: reservationBorrowers) {
			    if (existing <= 0)
			        break;
			    
			    TagGroup unusedTagGroup = TagGroup.getTagGroup(from, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getUnusedInstances(utilization), tagGroup.usageType, tagGroup.resourceGroup);
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
			
			        TagGroup borrowedTagGroup = TagGroup.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getBorrowedInstances(utilization), tagGroup.usageType, tagGroup.resourceGroup);
			        
			        Double existingBorrowed = usageMap.get(borrowedTagGroup);
			        reservedBorrowed = existingBorrowed == null ? reservedBorrowed : reservedBorrowed + existingBorrowed;
			
			        usageMap.put(borrowedTagGroup, reservedBorrowed);
			        usageMap.put(tagGroup, existing);
			        
			        costMap.put(borrowedTagGroup, reservedBorrowed * hourlyCost);
			        costMap.put(tagGroup, existing * hourlyCost);
			        
			        if (reservedUnused > 0) {
			        	usageMap.put(unusedTagGroup, reservedUnused);
			        	costMap.put(unusedTagGroup, reservedUnused * hourlyCost);
			        }
			        else {
			        	usageMap.remove(unusedTagGroup);
			        	costMap.remove(unusedTagGroup);
			        }
			        
			        if (product == null) {
			        	TagGroup lentTagGroup = TagGroup.getTagGroup(from, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getLentInstances(utilization), tagGroup.usageType, tagGroup.resourceGroup);
				        Double existingLent = usageMap.get(lentTagGroup);
				        double reservedLent = existingLent == null ? reservedBorrowed : reservedBorrowed + existingLent;
				        usageMap.put(lentTagGroup, reservedLent);
				        costMap.put(lentTagGroup, reservedLent * hourlyCost);
				        if (debug)
				        	logger.info("      lent      quantity: " + reservedLent + ", tag: " + lentTagGroup);
			        }						        
			        if (debug) {
			        	logger.info("      borrowed  quantity: " + reservedBorrowed + ", tag: " + borrowedTagGroup);
			        	logger.info("      remaining quantity: " + existing + ", tag: " + tagGroup);
			            logger.info("      unused    quantity: " + reservedUnused + ", tag: " + unusedTagGroup);
			        }
			    }
			}
			
			// Now process family-based borrowing
			for (Account from: reservationBorrowers) {
			    if (existing <= 0)
			        break;
			    
			    // Scan all the regional reservations looking for matching account, region, and family with unused reservations
			    for (TagGroup rtg: reservationTagGroups) {
			    	ResourceGroup riResourceGroup = product == null ? null : rtg.resourceGroup;
			    	if (rtg.zone != null || rtg.account != from || rtg.region != tagGroup.region || !sameFamily(rtg, tagGroup))
			    		continue;
			    	
			        TagGroup unusedRegionalTagGroup = TagGroup.getTagGroup(from, rtg.region, null, rtg.product, Operation.getUnusedInstances(utilization), rtg.usageType, riResourceGroup);
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
			           
			            TagGroup borrowedTagGroup = TagGroup.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getBorrowedInstances(utilization), tagGroup.usageType, tagGroup.resourceGroup);
			
			            Double existingBorrowed = usageMap.get(borrowedTagGroup);
			            adjustedReservedBorrowed = existingBorrowed == null ? adjustedReservedBorrowed : adjustedReservedBorrowed + existingBorrowed;			            
			
			            usageMap.put(borrowedTagGroup, adjustedReservedBorrowed);			            
			            usageMap.put(tagGroup, existing);

			            // Borrowed is in actual usage units
			            double curBorrowedCost = reservedBorrowed * hourlyCost;
			            Double existingBorrowedCost = costMap.get(borrowedTagGroup);
			            double borrowedCost = existingBorrowedCost == null ? curBorrowedCost : curBorrowedCost + existingBorrowedCost;

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
			            
			            if (product == null) {
				            TagGroup lentTagGroup = TagGroup.getTagGroup(from, rtg.region, rtg.zone, rtg.product, Operation.getLentInstances(utilization), rtg.usageType, riResourceGroup);
				            
				            // Lent is in reservation units
				            Double existingLent = usageMap.get(lentTagGroup);
				            double reservedLent = existingLent == null ? reservedBorrowed : reservedBorrowed + existingLent;
				            usageMap.put(lentTagGroup, reservedLent);
				            costMap.put(lentTagGroup, reservedLent * hourlyCost);
				            if (debug)
				            	logger.info("      lent      quantity: " + reservedLent + ", tag: " + lentTagGroup);
			            }
			            
			            if (debug) {
			            	logger.info("      borrowed  quantity: " + adjustedReservedBorrowed + ", tag: " + borrowedTagGroup);
			            	logger.info("      remaining quantity: " + existing + ", tag: " + tagGroup);
			                logger.info("      unused    quantity: " + reservedUnused + ", tag: " + unusedRegionalTagGroup);
			            }
			        }
			    }
			}
		}

		// the rest is bonus
		if (existing != null && existing > 0) {
	        TagGroup resTagGroup = TagGroup.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getReservedInstances(utilization), tagGroup.usageType, tagGroup.resourceGroup);
			ReservationService.ReservationInfo reservation = reservationService.getReservation(time, resTagGroup, utilization, instancePrices);
			TagGroup bonusTagGroup = TagGroup.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getBonusReservedInstances(utilization), tagGroup.usageType, tagGroup.resourceGroup);
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
	
	private void family(int i, long time,
		Map<TagGroup, Double> usageMap,
		Map<TagGroup, Double> costMap,
		TagGroup tagGroup,
		ReservationUtilization utilization,
		Set<TagGroup> bonusTags) {
	
	    boolean debug = debugReservations(i, tagGroup, utilization);
	    ResourceGroup riResourceGroup = product == null ? null : tagGroup.resourceGroup;
		TagGroup unusedTagGroup = TagGroup.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getUnusedInstances(utilization), tagGroup.usageType, riResourceGroup);
		Double unused = usageMap.get(unusedTagGroup);
		Operation op = familyBreakout ? Operation.getFamilyReservedInstances(utilization) : Operation.getReservedInstances(utilization);

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
						
				        TagGroup familyTagGroup = TagGroup.getTagGroup(tg.account, tg.region, tg.zone, tg.product, op, tg.usageType, tg.resourceGroup);
				        
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
	
	private void processUsage(ReservationUtilization utilization,
			TagGroup tagGroup,
			Zone zone,
			Map<TagGroup, Double> usageMap,
			Map<TagGroup, Double> costMap,
			UsedUnused uu,
			double reservationHourlyCost) {
				
		Operation bonusOperation = Operation.getBonusReservedInstances(utilization);
		
		if (product == null) {
			TagGroup bonusTagGroup = TagGroup.getTagGroup(tagGroup.account, tagGroup.region, zone, tagGroup.product, bonusOperation, tagGroup.usageType, null);
			applyUsage(utilization, bonusTagGroup, usageMap, costMap, uu, reservationHourlyCost);
		}
		else {
			// Handle resource groups - make a copy of the keys so we can update the usageMap as we process
			Set<TagGroup> usageMapKeys = Sets.newHashSet();
			usageMapKeys.addAll(usageMap.keySet());
			for (TagGroup usageTg: usageMapKeys) {
				if (usageTg.account != tagGroup.account || usageTg.region != tagGroup.region || usageTg.zone != zone ||
						usageTg.product != tagGroup.product || usageTg.operation != bonusOperation || usageTg.usageType != tagGroup.usageType) {
					continue;
				}
				TagGroup bonusTagGroup = TagGroup.getTagGroup(usageTg.account, usageTg.region, usageTg.zone, usageTg.product, bonusOperation, usageTg.usageType, usageTg.resourceGroup);
				applyUsage(utilization, bonusTagGroup, usageMap, costMap, uu, reservationHourlyCost);
				if (uu.unused <= 0.0)
					break;
			}
		}
	}
	
	private void applyUsage(ReservationUtilization utilization,
			TagGroup bonusTagGroup,
			Map<TagGroup, Double> usageMap,
			Map<TagGroup, Double> costMap,
			UsedUnused uu,
			double reservationHourlyCost) {
		Double existing = usageMap.get(bonusTagGroup);
		double value = existing == null ? 0 : existing;
		double reservedUsed = Math.min(value, uu.unused);
	    if (reservedUsed > 0) {
	   		uu.used += reservedUsed;
	   		uu.unused -= reservedUsed;
	
	   		TagGroup usedTagGroup = TagGroup.getTagGroup(bonusTagGroup.account, bonusTagGroup.region, bonusTagGroup.zone, bonusTagGroup.product, Operation.getReservedInstances(utilization), bonusTagGroup.usageType, bonusTagGroup.resourceGroup);
	   		Double usedExisting = usageMap.get(usedTagGroup);
	   		double usedTotal = usedExisting == null ? reservedUsed : usedExisting + reservedUsed;
   		
	        usageMap.put(usedTagGroup, usedTotal);				        
	        costMap.put(usedTagGroup, usedTotal * reservationHourlyCost);
	        
	        // Now decrement the bonus
	        double bonus = value - reservedUsed;
	        if (bonus > 0) {
	        	usageMap.put(bonusTagGroup, bonus);
	        	costMap.put(bonusTagGroup, bonus * reservationHourlyCost);
	        }
	        else {
	        	usageMap.remove(bonusTagGroup);
	        	costMap.remove(bonusTagGroup);
	        }				        
	    }
	}
	
	@Override
	protected void processReservations(
			ReservationService reservationService,
			CostAndUsageData data,
			Long startMilli) {		

		ReadWriteData usageData = data.getUsage(product);
		ReadWriteData costData = data.getCost(product);
		
        for (ReservationUtilization utilization: ReservationUtilization.values()) {
			if (reservationService.getTagGroups(utilization, startMilli).size() == 0)
				continue;
			
	    	logger.info("---------- Process " + (product == null ? "Non-resource" : product) + " data with " + reservationService.getTagGroups(utilization, startMilli).size() + " reservations for utilization: " + utilization);
	
	    	processAvailabilityZoneReservations(utilization, reservationService, usageData, costData, startMilli);
			if (debugHour >= 0)
				printUsage("between AZ and Regional", usageData, costData);		
			processRegionalReservations(utilization, reservationService, usageData, costData, startMilli);
    		removeUnusedFromSavings(utilization, usageData, costData);
        }	
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
		reservations.addAll(reservationService.getTagGroups(utilization, startMilli));
		
		if (debugHour >= 0)
			logger.info("--------------- processAvailabilityZoneReservations ----------- " + reservations.size() + " reservations");
		
		for (TagGroup reservationTagGroup: reservations) {
			// Get the tagGroup without the resource if we're processing non-product report
			ResourceGroup riResourceGroup = product == null ? null : reservationTagGroup.resourceGroup;
			TagGroup tagGroup = TagGroup.getTagGroup(reservationTagGroup.account, reservationTagGroup.region, reservationTagGroup.zone, reservationTagGroup.product, reservationTagGroup.operation, reservationTagGroup.usageType, riResourceGroup);
			// For each of the owner AZ reservation tag groups...
			if (tagGroup.zone == null)
				continue;
			
		    TagGroup bonusTagGroup = TagGroup.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getBonusReservedInstances(utilization), tagGroup.usageType, null);
		    TagGroup savingsTagGroup = TagGroup.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getSavings(utilization), tagGroup.usageType, null);
	        TagGroup unusedTagGroup = TagGroup.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getUnusedInstances(utilization), tagGroup.usageType, riResourceGroup);
	        
	        InstancePrices instancePrices = prices.get(tagGroup.product);
	        double onDemandRate = instancePrices.getOnDemandRate(tagGroup.region, tagGroup.usageType);
		    
			for (int i = 0; i < usageData.getNum(); i++) {
				// For each hour of usage...
			
			    Map<TagGroup, Double> usageMap = usageData.getData(i);
			    Map<TagGroup, Double> costMap = costData.getData(i);
			
			    // Get the reservation info for the utilization and tagGroup in the current hour
			    ReservationService.ReservationInfo reservation = reservationService.getReservation(startMilli + i * AwsUtils.hourMillis, reservationTagGroup, utilization, instancePrices);
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
			
			    if (reservation.capacity > 0) {
			    	if (reservation.upfrontAmortized > 0) {
				        TagGroup upfrontTagGroup = TagGroup.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getUpfrontAmortized(utilization), tagGroup.usageType, null);
			    		costMap.put(upfrontTagGroup, reservation.capacity * reservation.upfrontAmortized);
			    	}
			        double savingsRate = onDemandRate - reservation.reservationHourlyCost - reservation.upfrontAmortized;
				    costMap.put(savingsTagGroup, reservation.capacity * savingsRate);
			    }
			}
			
			reservationTagGroups.add(TagGroup.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getReservedInstances(utilization), tagGroup.usageType, tagGroup.resourceGroup));
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
			InstancePrices instancePrices = prices.get(tagGroup.product);
			for (int i = 0; i < usageData.getNum(); i++) {
			
			    Map<TagGroup, Double> usageMap = usageData.getData(i);
			    Map<TagGroup, Double> costMap = costData.getData(i);
			
			    borrow(i, startMilli + i * AwsUtils.hourMillis,
			    		usageMap, costMap,
			           tagGroup,
			           utilization,
			           reservationService,
			           reservationTagGroups,
			           instancePrices);
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
		reservations.addAll(reservationService.getTagGroups(utilization, startMilli));
		if (debugHour >= 0)
			logger.info("--------------- processRegionalReservations ----------- " + reservations.size() + " reservations");
		for (TagGroup tagGroup: reservations) {
			// For each of the owner Region reservation tag groups...
			if (tagGroup.zone != null)
				continue;
			
			ResourceGroup riResourceGroup = product == null ? null : tagGroup.resourceGroup;
	        TagGroup savingsTagGroup = TagGroup.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getSavings(utilization), tagGroup.usageType, null);
	        TagGroup unusedTagGroup = TagGroup.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getUnusedInstances(utilization), tagGroup.usageType, riResourceGroup);
	        
	        InstancePrices instancePrices = prices.get(tagGroup.product);
	        double onDemandRate = instancePrices.getOnDemandRate(tagGroup.region, tagGroup.usageType);
			
	        for (int i = 0; i < usageData.getNum(); i++) {
				// For each hour of usage...
			
			    Map<TagGroup, Double> usageMap = usageData.getData(i);
			    Map<TagGroup, Double> costMap = costData.getData(i);
			
			    // Get the reservation info for the utilization and tagGroup in the current hour
			    ReservationService.ReservationInfo reservation = reservationService.getReservation(startMilli + i * AwsUtils.hourMillis, tagGroup, utilization, instancePrices);
			    boolean debug = debugReservations(i, tagGroup, utilization);

			    UsedUnused uu = new UsedUnused(0.0, reservation.capacity);
			    
			    if (uu.unused > 0) {
				    // Do we have any usage from the current reservation?
				    // First check for region-based usage
				    processUsage(utilization, tagGroup, null, usageMap, costMap, uu, reservation.reservationHourlyCost);

			    	// Check each of the AZs in the region
			    	for (Zone zone: tagGroup.region.getZones()) {
			    		if (uu.unused <= 0)
			    			break;
			    		
			    		processUsage(utilization, tagGroup, zone, usageMap, costMap, uu, reservation.reservationHourlyCost);
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
			    if (reservation.capacity > 0) {
			    	if (reservation.upfrontAmortized > 0) {
				        TagGroup upfrontTagGroup = TagGroup.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getUpfrontAmortized(utilization), tagGroup.usageType, null);
			    		costMap.put(upfrontTagGroup, reservation.capacity * reservation.upfrontAmortized);
			    	}
			        double savingsRate = onDemandRate - reservation.reservationHourlyCost - reservation.upfrontAmortized;
			        costMap.put(savingsTagGroup, reservation.capacity * savingsRate);
			    }			
			}
			reservationTagGroups.add(TagGroup.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getReservedInstances(utilization), tagGroup.usageType, tagGroup.resourceGroup));
		}
		
		if (debugHour >= 0)
			printUsage("before regional family sharing and borrowing", usageData, costData);
		processFamilySharingAndBorrowing(utilization, reservationService, usageData, costData, startMilli, reservationTagGroups, true);
	}
	
	private Set<TagGroup> getUnassignedUsage(ReadWriteData usageData, Operation bonusOperation) {
		// Collect all tag groups for reserved instances not yet associated with a reservation.
		// They will appear as BonusReservedInstances.
		SortedSet<TagGroup> unassignedUsage = Sets.newTreeSet();
		SortedSet<TagGroup> usageTags = Sets.newTreeSet();
		usageTags.addAll(usageData.getTagGroups());
		for (TagGroup tagGroup: usageTags) {
			if (tagGroup.product.isEc2Instance() &&
			    tagGroup.operation == bonusOperation) {
			
				unassignedUsage.add(tagGroup);
			}
		}
		return unassignedUsage;
	}
	
	private void removeUnusedFromSavings(
			ReservationUtilization utilization,
			ReadWriteData usageData,
			ReadWriteData costData) {
		
		Operation unusedOp = Operation.getUnusedInstances(utilization);
		
		for (TagGroup tagGroup: usageData.getTagGroups()) {
			if (tagGroup.operation != unusedOp)
				continue;
			
	        TagGroup savingsTagGroup = TagGroup.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getSavings(utilization), tagGroup.usageType, null);
			
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
}

