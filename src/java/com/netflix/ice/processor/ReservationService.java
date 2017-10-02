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

import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.pricelist.InstancePrices;
import com.netflix.ice.processor.pricelist.InstancePrices.PurchaseOption;
import com.netflix.ice.processor.pricelist.InstancePrices.ServiceCode;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.UsageType;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Interface for reservations.
 */
public interface ReservationService {

    void init() throws Exception;
    public void shutdown();

    /**
     * Get all tag groups with reservations
     * @param utilization
     * @return
     */
    Collection<TagGroup> getTagGroups(ReservationUtilization utilization);

    /**
     *
     * @return
     */
    ReservationUtilization getDefaultReservationUtilization(long time);

    /**
     * Get reservation info.
     * @param time
     * @param tagGroup
     * @param utilization
     * @return
     */
    ReservationInfo getReservation(
            long time,
            TagGroup tagGroup,
            ReservationUtilization utilization,
            InstancePrices instancePrices);

    /**
     * Some companies may get different price tiers at different times depending on reservation cost.
     * This method is to get the latest hourly price including amortized upfront for given, time, region and usage type.
     * @param time
     * @param region
     * @param usageType
     * @param utilization
     * @return
     */
    double getLatestHourlyTotalPrice(
            long time,
            Region region,
            UsageType usageType,
            PurchaseOption purchaseOption,
            ServiceCode serviceCode,
            InstancePrices prices);

    /**
     * Methods to indicate that we have reservations for each corresponding service.
     */
    boolean hasEc2Reservations();
    boolean hasRdsReservations();
    boolean hasRedshiftReservations();

    public static class ReservationInfo {
        public final int capacity;
        public final double upfrontAmortized;		// Per-hour amortization of any up-front cost per instance
        public final double reservationHourlyCost;	// Per-hour cost for each reserved instance

        public ReservationInfo(int capacity, double upfrontAmortized, double reservationHourlyCost) {
            this.capacity = capacity;
            this.upfrontAmortized = upfrontAmortized;
            this.reservationHourlyCost = reservationHourlyCost;
        }
    }
    
    public class ReservationKey implements Comparable<ReservationKey> {
    	public String account;
    	public String region;
    	public String reservationId;
    	
    	public ReservationKey(String account, String region, String reservationId) {
    		this.account = account;
    		this.region = region;
    		this.reservationId = reservationId;
    	}

		@Override
		public int compareTo(ReservationKey o) {
	        int result = account.compareTo(o.account);
	        if (result != 0)
	            return result;
	        result = region.compareTo(o.region);
	        if (result != 0)
	            return result;
			return reservationId.compareTo(o.reservationId);
		}
		
		@Override
		public boolean equals(Object o) {
			ReservationKey rk = (ReservationKey) o;
			return rk == null ? false : compareTo(rk) == 0;
		}
		
		@Override
		public int hashCode() {
			return account.hashCode() * region.hashCode() * reservationId.hashCode();
		}
    }

    public static enum ReservationPeriod {
        oneyear(1, "yrTerm1"),
        threeyear(3, "yrTerm3");

        public final int years;
        public final String jsonTag;

        private ReservationPeriod(int years, String jsonTag) {
            this.years = years;
            this.jsonTag = jsonTag;
        }

    }

    public static enum ReservationUtilization {
        HEAVY, 		// The new "No Upfront" Reservations
        PARTIAL,	// The new "Partial Upfront" Reservations
        FIXED;		// The new "Full Upfront" Reservations

        static final Map<String, String> reservationTypeMap = new HashMap<String, String>();
        static {
            reservationTypeMap.put("ALL", "FIXED");
            reservationTypeMap.put("PARTIAL", "PARTIAL");
            reservationTypeMap.put("NO", "HEAVY");
        }

        public static ReservationUtilization get(String offeringType) {
            int idx = offeringType.indexOf(" ");
            if (idx > 0) {
            	// We've been called with a reservationInstance record offering type field
                offeringType = offeringType.substring(0, idx).toUpperCase();
                String mappedValue = reservationTypeMap.get(offeringType);
                if (mappedValue != null)
                    offeringType = mappedValue;
                return valueOf(offeringType);
            }
            else {
            	// We've been called with a billing report line item usage type field
                for (ReservationUtilization utilization: values()) {
                    if (offeringType.toUpperCase().startsWith(utilization.name()))
                        return utilization;
                }
                throw new RuntimeException("Unknown ReservationUtilization " + offeringType);
            }
        }
        
        public PurchaseOption getPurchaseOption() {
            switch (this) {
            case HEAVY:
            	return PurchaseOption.noUpfront;
            case PARTIAL:
            	return PurchaseOption.partialUpfront;
            case FIXED:
            default:
            	return PurchaseOption.allUpfront;
            }
        }
    }

}
