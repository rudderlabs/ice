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

import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.UsageType;

import java.util.Collection;
import java.util.Map;

/**
 * Interface for reservations.
 */
public interface ReservationService {

    void init();

    /**
     * Get all tag groups with reservations
     * @param utilization
     * @return
     */
    Collection<TagGroup> getTagGroups(Ec2InstanceReservationPrice.ReservationUtilization utilization);

    /**
     *
     * @return
     */
    Ec2InstanceReservationPrice.ReservationUtilization getDefaultReservationUtilization(long time);

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
            Ec2InstanceReservationPrice.ReservationUtilization utilization);

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
            Ec2InstanceReservationPrice.ReservationUtilization utilization);

    /**
     * Called by ReservationCapacityPoller to update reservations.
     * @param reservations
     */
    void updateReservations(Map<ReservationKey, CanonicalReservedInstances> reservationsFromApi, AccountService accountService, long startMillis, ProductService productService);


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
    	
    	ReservationKey(String account, String region, String reservationId) {
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

}
