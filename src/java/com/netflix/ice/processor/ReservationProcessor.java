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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.reader.InstanceMetrics;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.Zone;

public class ReservationProcessor {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    private final Set<Account> reservationOwners;
    private final Map<Account, List<Account>> reservationBorrowers;
    
    // hour of data to print debug statements. Set to -1 to turn off.
    private int debugHour = -1;
    private String debugFamily;

    public ReservationProcessor(Map<Account, List<Account>> reservationAccounts) {        
        // Initialize the reservation owner and borrower account lists
        reservationOwners = reservationAccounts.keySet();
        reservationBorrowers = Maps.newHashMap();
        for (Account account: reservationAccounts.keySet()) {
            List<Account> list = reservationAccounts.get(account);
            for (Account borrowingAccount: list) {
                if (borrowingAccount.name.equals(account.name))
                    continue;
                List<Account> from = reservationBorrowers.get(borrowingAccount);
                if (from == null) {
                    from = Lists.newArrayList();
                    reservationBorrowers.put(borrowingAccount, from);
                }
                from.add(account);
            }
        }
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
    
	private boolean debugReservations(int i, UsageType ut) {
		if (i != debugHour)
			return false;
		if (debugFamily.isEmpty())
			return true;
		String family = ut.name.split("\\.")[0];
		return family.equals(debugFamily);
	}
   
    private void borrow(int i, long time,
            Map<TagGroup, Double> usageMap,
            Map<TagGroup, Double> costMap,
            List<Account> fromAccounts,
            TagGroup tagGroup,
            Ec2InstanceReservationPrice.ReservationUtilization utilization,
            boolean forBonus,
            ReservationService reservationService,
            Set<TagGroup> reservationTagGroups,
            boolean debug) {

		Double existing = usageMap.get(tagGroup);
		
		if (existing != null && fromAccounts != null) {
		
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
			if (!forBonus) {
				ReservationService.ReservationInfo reservation = reservationService.getReservation(time, tagGroup, utilization);
				TagGroup bonusTagGroup = new TagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getBonusReservedInstances(utilization), tagGroup.usageType, null);
				if (debug) {
					logger.info("** borrow(" + i + ") **   bonus     quantity: " + existing + ", tag: " + bonusTagGroup);
				}
				usageMap.put(bonusTagGroup, existing);
				if (reservation.reservationHourlyCost > 0)
					costMap.put(bonusTagGroup, existing * reservation.reservationHourlyCost);
				
				usageMap.remove(tagGroup);
				costMap.remove(tagGroup);
			}
		}
		else {
			usageMap.remove(tagGroup);
			costMap.remove(tagGroup);
		}
	}

	private boolean sameFamily(TagGroup a, TagGroup b) {
		// True if both tags are ec2_instances and have the same usage type prefix
		return a.product == Product.ec2_instance &&
			a.product == b.product &&
			a.usageType.name.split("\\.")[0].equals(b.usageType.name.split("\\.")[0]);
	}
	
	private double convertFamilyUnits(double units, UsageType from, UsageType to) {
		return units * InstanceMetrics.getCostMultiplier(from) / InstanceMetrics.getCostMultiplier(to);
	}

	private void family(int i, long time,
		Map<TagGroup, Double> usageMap,
		Map<TagGroup, Double> costMap,
		TagGroup tagGroup,
		Ec2InstanceReservationPrice.ReservationUtilization utilization,
		Set<TagGroup> bonusTags,
		boolean debug) {
	
		TagGroup unusedTagGroup = new TagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getUnusedInstances(utilization), tagGroup.usageType, null);
		Double unused = usageMap.get(unusedTagGroup);
		
		if (unused != null && unused > 0) {
	        Double resHourlyCost = costMap.get(unusedTagGroup);
	        double hourlyCost = resHourlyCost == null ? 0.0 : resHourlyCost / unused;
			
			if (debug) {
				logger.info("----- family(" + i + ")** unused: " + unused + ", tagGroup: " + unusedTagGroup);
			}
			
			// Scan bonus reservations for this account
			for (TagGroup tg: bonusTags) {
				// only look within the same account
				if (tg.account != tagGroup.account)
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
	
	public void process(Ec2InstanceReservationPrice.ReservationUtilization utilization,
			ReservationService reservationService,
			ReadWriteData usageData,
			ReadWriteData costData,
			Long startMilli) {
	
		if (reservationService.getTagGroups(utilization).size() == 0)
			return;
	
    	logger.info("---------- Process " + reservationService.getTagGroups(utilization).size() + " reservations for utilization: " + utilization);

		// first mark owner accounts
		// The reservationTagGroups set will contain all the tagGroups for reservation purchases.
		// The account tag is the owner of the reservation, the zone is null for regionally scoped RIs.
		// It does NOT have anything to do with usage.
		// Usage is saved in the usageData maps.
		// reservationTagGroups therefore, does not include any reserved instance usage for borrowed reservations or reservation usage by members of the same
		// family of instance. Family reservervation usage will appear as bonus reservations if the account owns other RIs for
		// that same usage type.
		Set<TagGroup> reservationTagGroups = Sets.newTreeSet();
		for (TagGroup tagGroup: reservationService.getTagGroups(utilization)) {
			// For each of the owner AZ reservation tag groups...
			if (tagGroup.zone == null)
				continue;
			
			for (int i = 0; i < usageData.getNum(); i++) {
				// For each hour of usage...
			
			    Map<TagGroup, Double> usageMap = usageData.getData(i);
			    Map<TagGroup, Double> costMap = costData.getData(i);
			
			    // Get the reservation info for the utilization and tagGroup in the current hour
			    ReservationService.ReservationInfo reservation = reservationService.getReservation(startMilli + i * AwsUtils.hourMillis, tagGroup, utilization);
			    boolean debug = debugReservations(i, tagGroup.usageType);

			    // Do we have any usage from the current reservation?
			    // Usage is initially tagged as Bonus, then we work through the allocations.
			    TagGroup bonusTagGroup = new TagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getBonusReservedInstances(utilization), tagGroup.usageType, null);
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
			        TagGroup unusedTagGroup = new TagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getUnusedInstances(utilization), tagGroup.usageType, null);
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
			        TagGroup upfrontTagGroup = new TagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getUpfrontAmortized(utilization), tagGroup.usageType, null);
			        costMap.put(upfrontTagGroup, reservation.capacity * reservation.upfrontAmortized);
			    }
			}
			
			reservationTagGroups.add(new TagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getReservedInstances(utilization), tagGroup.usageType, null));
		}
		
		// Now spin through all the bonus reservations and allocate them to any regional reservations in the owner account
		for (TagGroup tagGroup: reservationService.getTagGroups(utilization)) {
			// For each of the owner Region reservation tag groups...
			if (tagGroup.zone != null)
				continue;
			for (int i = 0; i < usageData.getNum(); i++) {
				// For each hour of usage...
			
			    Map<TagGroup, Double> usageMap = usageData.getData(i);
			    Map<TagGroup, Double> costMap = costData.getData(i);
			
			    // Get the reservation info for the utilization and tagGroup in the current hour
			    ReservationService.ReservationInfo reservation = reservationService.getReservation(startMilli + i * AwsUtils.hourMillis, tagGroup, utilization);
			    boolean debug = debugReservations(i, tagGroup.usageType);

			    double reservedUsed = 0;
			    double reservedUnused = reservation.capacity;
			    
			    // Do we have any usage from the current reservation?

		    	// Check each of the AZs in the region
		    	for (Zone zone: tagGroup.region.getZones()) {
		    		TagGroup bonusTagGroup = new TagGroup(tagGroup.account, tagGroup.region, zone, tagGroup.product, Operation.getBonusReservedInstances(utilization), tagGroup.usageType, null);
		    		Double existing = usageMap.get(bonusTagGroup);
		    		double value = existing == null ? 0 : existing;
		    		
		    		double reservedUsedZone = Math.min(value, reservedUnused);
				    if (reservedUsedZone > 0) {
			    		reservedUsed += reservedUsedZone;
			    		reservedUnused -= reservedUsedZone;

			    		TagGroup usedTagGroup = new TagGroup(tagGroup.account, tagGroup.region, zone, tagGroup.product, Operation.getReservedInstances(utilization), tagGroup.usageType, null);
			    		Double usedExisting = usageMap.get(usedTagGroup);
			    		double usedTotal = usedExisting == null ? reservedUsedZone : usedExisting + reservedUsedZone;
			    		
				        usageMap.put(usedTagGroup, usedTotal);				        
				        costMap.put(usedTagGroup, usedTotal * reservation.reservationHourlyCost);
				        
				        // Now decrement the bonus
				        double bonus = value - reservedUsedZone;
				        if (bonus > 0) {
				        	usageMap.put(bonusTagGroup, bonus);
				        	costMap.put(bonusTagGroup, bonus * reservation.reservationHourlyCost);
				        }
				        else {
				        	usageMap.remove(bonusTagGroup);
				        	costMap.remove(bonusTagGroup);
				        }				        
				    }				
			    }
			    if (debug) {
			    	logger.info("**** Region reservation **** hour: " + i + ", used: " + reservedUsed + ", capacity: " + reservation.capacity + ", tagGroup: " + tagGroup);
			    }
			    
			    if (reservedUnused > 0) {
			        TagGroup unusedTagGroup = new TagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getUnusedInstances(utilization), tagGroup.usageType, null);
			        usageMap.put(unusedTagGroup, reservedUnused);
			        costMap.put(unusedTagGroup, reservedUnused * reservation.reservationHourlyCost);
			        if (debug) {
			        	logger.info("  ** Unused instances **** hour: " + i + ", used: " + reservedUsed + ", unused: " + reservedUnused + ", tag: " + unusedTagGroup);
			        }
			    }
			    if (reservation.capacity > 0 && reservation.upfrontAmortized > 0) {
			        TagGroup upfrontTagGroup = new TagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getUpfrontAmortized(utilization), tagGroup.usageType, null);
			        costMap.put(upfrontTagGroup, reservation.capacity * reservation.upfrontAmortized);
			    }			
			}
			reservationTagGroups.add(new TagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getReservedInstances(utilization), tagGroup.usageType, null));
		}
		
		// now mark borrowing accounts
		// mark all tag groups for reserved instances not used by the owner account and any bonus reservations.
		// Reserved instance usage that has other reservations in the owner account for that usage type will be flagged as BonusReservedInstances.
		// Reserved instance usage that doesn't have any reservations in the owner account will be flagged as ReservedInstances
		Set<TagGroup> toMarkBorrowing = Sets.newTreeSet();
		for (TagGroup tagGroup: usageData.getTagGroups()) {
			if (tagGroup.resourceGroup == null &&
			    tagGroup.product == Product.ec2_instance &&
			    (tagGroup.operation == Operation.getReservedInstances(utilization) && !reservationTagGroups.contains(tagGroup) ||
			     tagGroup.operation == Operation.getBonusReservedInstances(utilization))) {
			
			    toMarkBorrowing.add(tagGroup);
			}
		}
		
		// Scan bonus reservations and handle non-zone-specific and family-based usage of regionally-scoped reservations
		// within each owner account (region-scoped reservations new as of 11/1/2016, family-based credits are new as of 3/1/2017)
		for (TagGroup tagGroup: reservationTagGroups) {
			// only process regional reservations
			if (tagGroup.zone != null)
				continue;
			
			for (int i = 0; i < usageData.getNum(); i++) {
			
			    Map<TagGroup, Double> usageMap = usageData.getData(i);
			    Map<TagGroup, Double> costMap = costData.getData(i);
			    
			    boolean debug = debugReservations(i, tagGroup.usageType);
			    family(i, startMilli + i * AwsUtils.hourMillis, usageMap, costMap, tagGroup, utilization, toMarkBorrowing, debug);
			}
		}
			
			
		for (TagGroup tagGroup: toMarkBorrowing) {
			for (int i = 0; i < usageData.getNum(); i++) {
			
			    Map<TagGroup, Double> usageMap = usageData.getData(i);
			    Map<TagGroup, Double> costMap = costData.getData(i);
			
			    boolean debug = debugReservations(i, tagGroup.usageType);
			    borrow(i, startMilli + i * AwsUtils.hourMillis,
			    		usageMap, costMap,
			           reservationBorrowers.get(tagGroup.account),
			           tagGroup,
			           utilization,
			           reservationOwners.contains(tagGroup.account),
			           reservationService,
			           reservationTagGroups,
			           debug);
			}
		}
		
		if (debugHour >= 0) {
			logger.info("---------- usage for hour " + debugHour + " after processing ----------------");
		    Map<TagGroup, Double> usageMap = usageData.getData(debugHour);
			
			for (TagGroup tagGroup: usageData.getTagGroups()) {
				if (debugReservations(debugHour, tagGroup.usageType))
					logger.info("usage " + usageMap.get(tagGroup) + " for tagGroup: " + tagGroup);
			}

			Map<TagGroup, Double> costMap = costData.getData(debugHour);

			for (TagGroup tagGroup: costData.getTagGroups()) {
				if (debugReservations(debugHour, tagGroup.usageType))
					logger.info("cost " + costMap.get(tagGroup) + " for tagGroup: " + tagGroup);
			}
		}
	}
}
