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
import java.util.Set;
import java.util.SortedSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.PurchaseOption;
import com.netflix.ice.common.TagGroup;
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
		   ProductService productService, PriceListService priceListService) throws IOException {
	   super(reservationOwners, productService, priceListService);

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
           ReadWriteData usageData,
           ReadWriteData costData,
           TagGroup tagGroup,
           PurchaseOption purchaseOption,
           ReservationService reservationService,
           Set<TagGroup> reservationTagGroups,
           InstancePrices instancePrices) {

	    boolean debug = debugReservations(i, tagGroup, purchaseOption);
		Double existing = usageData.get(i, tagGroup);

		if (debug)
			logger.info("      borrow from accounts: " + reservationBorrowers + ", existing: " + existing + " from tagGroup " + tagGroup);
		if (existing != null && !reservationBorrowers.isEmpty()) {
		
			// process borrowing of matching usage type
			for (Account from: reservationBorrowers) {
			    if (existing <= 0)
			        break;
			    
			    TagGroup unusedTagGroup = TagGroup.getTagGroup(from, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getUnusedInstances(purchaseOption), tagGroup.usageType, tagGroup.resourceGroup);
			    Double unused = usageData.get(i, unusedTagGroup);
			
			    if (unused != null && unused > 0) {
			        if (debug) {
			        	logger.info("** borrow(" + i + ") up to: " + existing + " for: " + tagGroup);
			        	logger.info("       from: " + from + ", unused: " + unused + ", " + unusedTagGroup);
			        }
			        
			        Double resHourlyCost = costData.get(i, unusedTagGroup);
			        double hourlyCost = resHourlyCost == null ? 0.0 : resHourlyCost / unused;
			
			        double reservedBorrowed = Math.min(existing, unused);
			        double reservedUnused = unused - reservedBorrowed;
			
			        existing -= reservedBorrowed;
			
			        TagGroup borrowedTagGroup = TagGroup.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getBorrowedInstances(purchaseOption), tagGroup.usageType, tagGroup.resourceGroup);
			        
			        Double existingBorrowed = usageData.get(i, borrowedTagGroup);
			        reservedBorrowed = existingBorrowed == null ? reservedBorrowed : reservedBorrowed + existingBorrowed;
			
			        usageData.put(i, borrowedTagGroup, reservedBorrowed);
			        usageData.put(i, tagGroup, existing);
			        
			        costData.put(i, borrowedTagGroup, reservedBorrowed * hourlyCost);
			        costData.put(i, tagGroup, existing * hourlyCost);
			        
			        if (reservedUnused > 0) {
			        	usageData.put(i, unusedTagGroup, reservedUnused);
			        	costData.put(i, unusedTagGroup, reservedUnused * hourlyCost);
			        }
			        else {
			        	usageData.remove(i, unusedTagGroup);
			        	costData.remove(i, unusedTagGroup);
			        }
			        
			        if (product == null) {
			        	TagGroup lentTagGroup = TagGroup.getTagGroup(from, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getLentInstances(purchaseOption), tagGroup.usageType, tagGroup.resourceGroup);
				        Double existingLent = usageData.get(i, lentTagGroup);
				        double reservedLent = existingLent == null ? reservedBorrowed : reservedBorrowed + existingLent;
				        usageData.put(i, lentTagGroup, reservedLent);
				        costData.put(i, lentTagGroup, reservedLent * hourlyCost);
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
			    	
			        TagGroup unusedRegionalTagGroup = TagGroup.getTagGroup(from, rtg.region, null, rtg.product, Operation.getUnusedInstances(purchaseOption), rtg.usageType, riResourceGroup);
			        Double unused = usageData.get(i, unusedRegionalTagGroup);
			
			        if (unused != null && unused > 0) {
			            if (debug) {
			               	logger.info("** family borrow(" + i + ") up to: " + existing + " for: " + tagGroup);
			            	logger.info("       from: " + from + ", unused: " + unused + ", " + rtg);
			            }
			            
				        Double resHourlyCost = costData.get(i, unusedRegionalTagGroup);
				        double hourlyCost = resHourlyCost == null ? 0.0 : resHourlyCost / unused;
			
			            double adjustedUnused = convertFamilyUnits(unused, rtg.usageType, tagGroup.usageType);
			            double adjustedReservedBorrowed = Math.min(existing, adjustedUnused);
			            double reservedUnused = convertFamilyUnits(adjustedUnused - adjustedReservedBorrowed, tagGroup.usageType, rtg.usageType);
			            double reservedBorrowed = unused - reservedUnused;
			            
			            existing -= adjustedReservedBorrowed;
			           
			            TagGroup borrowedTagGroup = TagGroup.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getBorrowedInstances(purchaseOption), tagGroup.usageType, tagGroup.resourceGroup);
			
			            Double existingBorrowed = usageData.get(i, borrowedTagGroup);
			            adjustedReservedBorrowed = existingBorrowed == null ? adjustedReservedBorrowed : adjustedReservedBorrowed + existingBorrowed;			            
			
			            usageData.put(i, borrowedTagGroup, adjustedReservedBorrowed);			            
			            usageData.put(i, tagGroup, existing);

			            // Borrowed is in actual usage units
			            double curBorrowedCost = reservedBorrowed * hourlyCost;
			            Double existingBorrowedCost = costData.get(i, borrowedTagGroup);
			            double borrowedCost = existingBorrowedCost == null ? curBorrowedCost : curBorrowedCost + existingBorrowedCost;

			            costData.put(i, borrowedTagGroup, borrowedCost);
			            costData.put(i, tagGroup, existing * hourlyCost);
			            			

			            if (reservedUnused > 0) {
			            	usageData.put(i, unusedRegionalTagGroup, reservedUnused);
			            	costData.put(i, unusedRegionalTagGroup, reservedUnused * hourlyCost);
			            }
			            else {
			            	usageData.remove(i, unusedRegionalTagGroup);
			            	costData.remove(i, unusedRegionalTagGroup);
			            }
			            
			            if (product == null) {
				            TagGroup lentTagGroup = TagGroup.getTagGroup(from, rtg.region, rtg.zone, rtg.product, Operation.getLentInstances(purchaseOption), rtg.usageType, riResourceGroup);
				            
				            // Lent is in reservation units
				            Double existingLent = usageData.get(i, lentTagGroup);
				            double reservedLent = existingLent == null ? reservedBorrowed : reservedBorrowed + existingLent;
				            usageData.put(i, lentTagGroup, reservedLent);
				            costData.put(i, lentTagGroup, reservedLent * hourlyCost);
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
	        TagGroup resTagGroup = TagGroup.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getReservedInstances(purchaseOption), tagGroup.usageType, tagGroup.resourceGroup);
			ReservationService.ReservationInfo reservation = reservationService.getReservation(time, resTagGroup, purchaseOption, instancePrices);
			TagGroup bonusTagGroup = TagGroup.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getBonusReservedInstances(purchaseOption), tagGroup.usageType, tagGroup.resourceGroup);
			if (debug) {
				logger.info("** bonus(" + i + ") **   bonus     quantity: " + existing + ", tag: " + bonusTagGroup);
			}
			usageData.put(i, bonusTagGroup, existing);
			if (reservation.reservationHourlyCost > 0)
				costData.put(i, bonusTagGroup, existing * reservation.reservationHourlyCost);				
		}
		else {
			usageData.remove(i, tagGroup);
			costData.remove(i, tagGroup);
		}
	}

	private boolean sameFamily(TagGroup a, TagGroup b) {
		// True if both tags are ec2_instances and have the same usage type prefix
		return a.product.isEc2Instance() &&
			a.product == b.product &&
			a.usageType.name.split("\\.")[0].equals(b.usageType.name.split("\\.")[0]);
	}
	
	private void family(int i, long time,
		ReadWriteData usageData,
		ReadWriteData costData,
		TagGroup tagGroup,
		PurchaseOption purchaseOption,
		Set<TagGroup> bonusTags) {
	
	    boolean debug = debugReservations(i, tagGroup, purchaseOption);
	    ResourceGroup riResourceGroup = product == null ? null : tagGroup.resourceGroup;
		TagGroup unusedTagGroup = TagGroup.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getUnusedInstances(purchaseOption), tagGroup.usageType, riResourceGroup);
		Double unused = usageData.get(i, unusedTagGroup);
		Operation op = Operation.getReservedInstances(purchaseOption);

		if (debug)
			logger.info("      family - " + bonusTags.size() + " bonus tags, unused: " + unused + " for tagGroup " + tagGroup);
		
		if (unused != null && unused > 0) {
	        Double resHourlyCost = costData.get(i, unusedTagGroup);
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
			        	logger.info("      found bonus: " + usageData.get(i, tg) + ", tag: " + tg);
			        }
					// found a reservation that uses the unused portion
					Double used = usageData.get(i, tg);
					if (used != null && used > 0) {
						double adjustedUsed = convertFamilyUnits(used, tg.usageType, tagGroup.usageType);
						double reservedUsed = Math.min(unused, adjustedUsed);
						double familyUsed = convertFamilyUnits(reservedUsed, tagGroup.usageType, tg.usageType);
						unused -= reservedUsed;
						
						used -= familyUsed;
						
				        TagGroup familyTagGroup = TagGroup.getTagGroup(tg.account, tg.region, tg.zone, tg.product, op, tg.usageType, tg.resourceGroup);
				        
				        // Allocated usage as a family reservation
				        
			            Double existingFamilyUsage = usageData.get(i, familyTagGroup);
			            double totalFamilyUsage = existingFamilyUsage == null ? familyUsed : familyUsed + existingFamilyUsage;
			            
				        usageData.put(i, familyTagGroup, totalFamilyUsage);			            
			            
			            Double existingFamilyCost = costData.get(i, familyTagGroup);
			            double familyCost = reservedUsed * hourlyCost;
			            double totalFamilyCost = existingFamilyCost == null ? familyCost : familyCost + existingFamilyCost;
		
			        	costData.put(i, familyTagGroup, totalFamilyCost);
				        
				        // What's left of bonus if any
				        if (used > 0) {
							usageData.put(i, tg, used);
							costData.put(i, tg, (adjustedUsed - reservedUsed) * hourlyCost);
				        }
				        else {
				        	usageData.remove(i, tg);
				        	costData.remove(i, tg);
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
				usageData.put(i, unusedTagGroup, unused);
				costData.put(i, unusedTagGroup, unused * hourlyCost);
			}
			else {
				usageData.remove(i, unusedTagGroup);
				costData.remove(i, unusedTagGroup);
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
	
	private void processUsage(
			int hour,
			PurchaseOption purchaseOption,
			TagGroup tagGroup,
			Zone zone,
			ReadWriteData usageData,
			ReadWriteData costData,
			UsedUnused uu,
			double reservationHourlyCost,
			boolean debug) {
				
		Operation bonusOperation = Operation.getBonusReservedInstances(purchaseOption);
		
		if (product == null) {
			TagGroup bonusTagGroup = TagGroup.getTagGroup(tagGroup.account, tagGroup.region, zone, tagGroup.product, bonusOperation, tagGroup.usageType, null);
			applyUsage(hour, purchaseOption, bonusTagGroup, usageData, costData, uu, reservationHourlyCost, debug);
		}
		else {
			// Handle resource groups - make a copy of the keys so we can update the usageData as we process
			Set<TagGroup> usageMapKeys = Sets.newHashSet();
			usageMapKeys.addAll(usageData.getTagGroups(hour));
			for (TagGroup usageTg: usageMapKeys) {
				if (usageTg.account != tagGroup.account || usageTg.region != tagGroup.region || usageTg.zone != zone ||
						usageTg.product != tagGroup.product || usageTg.operation != bonusOperation || usageTg.usageType != tagGroup.usageType) {
					continue;
				}
				TagGroup bonusTagGroup = TagGroup.getTagGroup(usageTg.account, usageTg.region, usageTg.zone, usageTg.product, bonusOperation, usageTg.usageType, usageTg.resourceGroup);
				applyUsage(hour, purchaseOption, bonusTagGroup, usageData, costData, uu, reservationHourlyCost, debug);
				if (uu.unused <= 0.0)
					break;
			}
		}
	}
	
	private void applyUsage(
			int hour,
			PurchaseOption purchaseOption,
			TagGroup bonusTagGroup,
			ReadWriteData usageData,
			ReadWriteData costData,
			UsedUnused uu,
			double reservationHourlyCost, 
			boolean debug) {
		Double existing = usageData.get(hour, bonusTagGroup);
		double value = existing == null ? 0 : existing;
		double reservedUsed = Math.min(value, uu.unused);
		if (debug)
			logger.info("  ---- zone=" + bonusTagGroup.zone + ", existing=" + existing + ", value=" + value + ", reservedUsed=" + reservedUsed);
	    if (reservedUsed > 0) {
	   		uu.used += reservedUsed;
	   		uu.unused -= reservedUsed;
	
	   		TagGroup usedTagGroup = TagGroup.getTagGroup(bonusTagGroup.account, bonusTagGroup.region, bonusTagGroup.zone, bonusTagGroup.product, Operation.getReservedInstances(purchaseOption), bonusTagGroup.usageType, bonusTagGroup.resourceGroup);
	   		Double usedExisting = usageData.get(hour, usedTagGroup);
	   		double usedTotal = usedExisting == null ? reservedUsed : usedExisting + reservedUsed;
   		
	   		usageData.put(hour, usedTagGroup, usedTotal);				        
	        costData.put(hour, usedTagGroup, usedTotal * reservationHourlyCost);
	        
	        // Now decrement the bonus
	        double bonus = value - reservedUsed;
	        if (bonus > 0) {
	        	usageData.put(hour, bonusTagGroup, bonus);
	        	costData.put(hour, bonusTagGroup, bonus * reservationHourlyCost);
	        }
	        else {
	        	usageData.remove(hour, bonusTagGroup);
	        	costData.remove(hour, bonusTagGroup);
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
		
        for (PurchaseOption purchaseOption: PurchaseOption.values()) {
			if (reservationService.getTagGroups(purchaseOption, startMilli, product).size() == 0)
				continue;
			
	    	logger.info("---------- Process " + (product == null ? "Non-resource" : product) + " data with " + reservationService.getTagGroups(purchaseOption, startMilli, product).size() + " reservations for utilization: " + purchaseOption);
	
	    	processAvailabilityZoneReservations(purchaseOption, reservationService, usageData, costData, startMilli);
			if (debugHour >= 0)
				printUsage("between AZ and Regional", usageData, costData);		
			processRegionalReservations(purchaseOption, reservationService, usageData, costData, startMilli);
    		removeUnusedFromSavings(purchaseOption, usageData, costData);
        }	
	}
		
	private void processAvailabilityZoneReservations(PurchaseOption purchaseOption,
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
		reservations.addAll(reservationService.getTagGroups(purchaseOption, startMilli, product));
		
		if (debugHour >= 0)
			logger.info("--------------- processAvailabilityZoneReservations ----------- " + reservations.size() + " reservations");
		
		for (TagGroup reservationTagGroup: reservations) {
			// Get the tagGroup without the resource if we're processing non-product report
			ResourceGroup riResourceGroup = product == null ? null : reservationTagGroup.resourceGroup;
			TagGroup tagGroup = TagGroup.getTagGroup(reservationTagGroup.account, reservationTagGroup.region, reservationTagGroup.zone, reservationTagGroup.product, reservationTagGroup.operation, reservationTagGroup.usageType, riResourceGroup);
			// For each of the owner AZ reservation tag groups...
			if (tagGroup.zone == null)
				continue;
			
		    TagGroup bonusTagGroup = TagGroup.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getBonusReservedInstances(purchaseOption), tagGroup.usageType, null);
		    TagGroup savingsTagGroup = TagGroup.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getSavings(purchaseOption), tagGroup.usageType, null);
	        TagGroup unusedTagGroup = TagGroup.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getUnusedInstances(purchaseOption), tagGroup.usageType, riResourceGroup);
	        
	        InstancePrices instancePrices = prices.get(tagGroup.product);
	        double onDemandRate = instancePrices.getOnDemandRate(tagGroup.region, tagGroup.usageType);
		    
			for (int i = 0; i < usageData.getNum(); i++) {
				// For each hour of usage...
			
			    // Get the reservation info for the utilization and tagGroup in the current hour
			    ReservationService.ReservationInfo reservation = reservationService.getReservation(startMilli + i * AwsUtils.hourMillis, reservationTagGroup, purchaseOption, instancePrices);
			    boolean debug = debugReservations(i, tagGroup, purchaseOption);

			    // Do we have any usage from the current reservation?
			    // Usage is initially tagged as Bonus, then we work through the allocations.
			    Double existing = usageData.get(i, bonusTagGroup);
			    double bonusReserved = existing == null ? 0 : existing;
			    
			    double reservedUnused = reservation.capacity;			    
			    double reservedUsed = Math.min(bonusReserved, reservedUnused);
			    reservedUnused -= reservedUsed;
			    bonusReserved -= reservedUsed;

			    if (reservedUsed > 0) {
			    	addToExisting(usageData, i, tagGroup, reservedUsed);
			    	addToExisting(costData, i, tagGroup, reservedUsed * reservation.reservationHourlyCost);
			    }
			    
			    if (debug) {
			    	logger.info("**** AZ reservation **** hour: " + i + ", existing: " + existing + ", bonusReserved: " + bonusReserved + ", used: " + reservedUsed + ", unused: " + reservedUnused + ", capacity: " + reservation.capacity + ", tagGroup: " + tagGroup);
			    }
			    
			    if (reservedUnused > 0) {
			        addToExisting(usageData, i, unusedTagGroup, reservedUnused);
			        addToExisting(costData, i, unusedTagGroup, reservedUnused * reservation.reservationHourlyCost);
			        if (debug) {
			        	logger.info("  ** Unused instances **** hour: " + i + ", used: " + reservedUsed + ", unused: " + reservedUnused + ", tag: " + unusedTagGroup);
			        }
			    }
			
			    usageData.put(i, bonusTagGroup, bonusReserved);
			    costData.put(i, bonusTagGroup, bonusReserved * reservation.reservationHourlyCost);
		        if (debug) {
		        	logger.info("  ** Bonus instances **** hour: " + i + ", bonus: " + bonusReserved + ", tag: " + bonusTagGroup);
		        }
			
			    if (reservation.capacity > 0) {
			    	if (reservation.upfrontAmortized > 0) {
				        TagGroup upfrontTagGroup = TagGroup.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getAmortized(purchaseOption), tagGroup.usageType, null);
			    		addToExisting(costData, i, upfrontTagGroup, reservation.capacity * reservation.upfrontAmortized);
			    	}
			        double savingsRate = onDemandRate - reservation.reservationHourlyCost - reservation.upfrontAmortized;
				    addToExisting(costData, i, savingsTagGroup, reservation.capacity * savingsRate);
			    }
			}
			
			reservationTagGroups.add(TagGroup.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getReservedInstances(purchaseOption), tagGroup.usageType, tagGroup.resourceGroup));
		}
		processFamilySharingAndBorrowing(purchaseOption, reservationService, usageData, costData, startMilli, reservationTagGroups, false);
	}
	
	private void addToExisting(ReadWriteData data, int hour, TagGroup tagGroup, double amount) {
    	Double existing = data.get(hour, tagGroup);
    	double total = (existing == null ? 0.0 : existing) + amount;
        data.put(hour, tagGroup, total);
	}
	
	private void processFamilySharingAndBorrowing(PurchaseOption purchaseOption,
			ReservationService reservationService,
			ReadWriteData usageData,
			ReadWriteData costData,
			Long startMilli,
			Set<TagGroup> reservationTagGroups,
			boolean regional) {
		
		if (product != null && !product.isEc2Instance())
			return;

		Operation bonusOperation = Operation.getBonusReservedInstances(purchaseOption);
		
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
				
				    family(i, startMilli + i * AwsUtils.hourMillis, usageData, costData, tagGroup, purchaseOption, unassignedUsage);
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
						
			    borrow(i, startMilli + i * AwsUtils.hourMillis,
			    		usageData, costData,
			           tagGroup,
			           purchaseOption,
			           reservationService,
			           reservationTagGroups,
			           instancePrices);
			}
		}		
	}
		
	private void processRegionalReservations(PurchaseOption purchaseOption,
			ReservationService reservationService,
			ReadWriteData usageData,
			ReadWriteData costData,
			Long startMilli) {
		// Now spin through all the bonus reservations and allocate them to any regional reservations in the owner account.
		// Regional reservations include RDS and Redshift products.
		SortedSet<TagGroup> reservationTagGroups = Sets.newTreeSet();
		SortedSet<TagGroup> reservations = Sets.newTreeSet();
		reservations.addAll(reservationService.getTagGroups(purchaseOption, startMilli, product));
		if (debugHour >= 0)
			logger.info("--------------- processRegionalReservations ----------- " + reservations.size() + " reservations");
		for (TagGroup tagGroup: reservations) {
			// For each of the owner Region reservation tag groups...
			if (tagGroup.zone != null)
				continue;
			
			ResourceGroup riResourceGroup = product == null ? null : tagGroup.resourceGroup;
	        TagGroup savingsTagGroup = TagGroup.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getSavings(purchaseOption), tagGroup.usageType, null);
	        TagGroup unusedTagGroup = TagGroup.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getUnusedInstances(purchaseOption), tagGroup.usageType, riResourceGroup);
	        
	        InstancePrices instancePrices = prices.get(tagGroup.product);
	        if (instancePrices == null)
	        	logger.error("No prices for product: " + tagGroup.product + ", have prices for: " + prices.keySet().toString());
	        double onDemandRate = instancePrices == null ? 0 : instancePrices.getOnDemandRate(tagGroup.region, tagGroup.usageType);
			
	        for (int i = 0; i < usageData.getNum(); i++) {
				// For each hour of usage...
						
			    // Get the reservation info for the utilization and tagGroup in the current hour
			    ReservationService.ReservationInfo reservation = reservationService.getReservation(startMilli + i * AwsUtils.hourMillis, tagGroup, purchaseOption, instancePrices);
			    boolean debug = debugReservations(i, tagGroup, purchaseOption);

			    UsedUnused uu = new UsedUnused(0.0, reservation.capacity);
			    
			    if (uu.unused > 0) {
				    // Do we have any usage from the current reservation?
				    // First check for region-based usage
				    processUsage(i, purchaseOption, tagGroup, null, usageData, costData, uu, reservation.reservationHourlyCost, debug);

			    	// Check each of the AZs in the region
			    	for (Zone zone: tagGroup.region.getZones()) {
			    		if (uu.unused <= 0)
			    			break;
			    		
			    		processUsage(i, purchaseOption, tagGroup, zone, usageData, costData, uu, reservation.reservationHourlyCost, debug);
				    }
			    }
			    if (debug) {
			    	logger.info("**** Region reservation **** hour: " + i + ", used: " + uu.used + ", capacity: " + reservation.capacity + ", tagGroup: " + tagGroup);
			    	logger.info("    zones = " + tagGroup.region.getZones());
			    }
			    
			    if (uu.unused > 0) {
			    	usageData.put(i, unusedTagGroup, uu.unused);
			    	costData.put(i, unusedTagGroup, uu.unused * reservation.reservationHourlyCost);
			        if (debug) {
			        	logger.info("  ** Unused instances **** hour: " + i + ", used: " + uu.used + ", unused: " + uu.unused + ", tag: " + unusedTagGroup);
			        }
			    }
			    if (reservation.capacity > 0) {
			    	if (reservation.upfrontAmortized > 0) {
				        TagGroup upfrontTagGroup = TagGroup.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getAmortized(purchaseOption), tagGroup.usageType, null);
				        addToExisting(costData, i, upfrontTagGroup, reservation.capacity * reservation.upfrontAmortized);
			    	}
			        double savingsRate = onDemandRate - reservation.reservationHourlyCost - reservation.upfrontAmortized;
			        addToExisting(costData, i, savingsTagGroup, reservation.capacity * savingsRate);
			    }			
			}
			reservationTagGroups.add(TagGroup.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getReservedInstances(purchaseOption), tagGroup.usageType, tagGroup.resourceGroup));
		}
		
		if (debugHour >= 0)
			printUsage("before regional family sharing and borrowing", usageData, costData);
		processFamilySharingAndBorrowing(purchaseOption, reservationService, usageData, costData, startMilli, reservationTagGroups, true);
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
			PurchaseOption purchaseOption,
			ReadWriteData usageData,
			ReadWriteData costData) {
		
		Operation unusedOp = Operation.getUnusedInstances(purchaseOption);
		
		for (TagGroup tagGroup: usageData.getTagGroups()) {
			if (tagGroup.operation != unusedOp)
				continue;
			
	        TagGroup savingsTagGroup = TagGroup.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, Operation.getSavings(purchaseOption), tagGroup.usageType, null);
			
	        double onDemandRate = prices.get(tagGroup.product).getOnDemandRate(tagGroup.region, tagGroup.usageType);

	        for (int i = 0; i < usageData.getNum(); i++) {
				// For each hour of usage...
			
			    Double unused = usageData.get(i, tagGroup);
	        	if (unused != null && unused > 0.0) {
		        	Double savings = costData.get(i, savingsTagGroup);
		        	if (savings == null) {
		        		logger.error("Savings record not found for " + tagGroup);
		        	}
		        	else {
		        		savings -= unused * onDemandRate;
		        		costData.put(i, savingsTagGroup, savings);
		        	}
	        	}
	        }
		}
	}
}

