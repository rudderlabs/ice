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

import com.netflix.ice.basic.BasicReservationService.Reservation;
import com.netflix.ice.common.PurchaseOption;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.common.TagGroupRI;
import com.netflix.ice.processor.pricelist.InstancePrices;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.ReservationArn;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Interface for reservations.
 */
public interface ReservationService {

    /**
     * Get all tag groups with reservations (Used only for Detailed Billing Report Processing)
     * @param purchaseOption
     * @return
     */
    Collection<TagGroup> getTagGroups(PurchaseOption purchaseOption, Long startMilli, Product product);

    /**
     *
     * @return
     */
    PurchaseOption getDefaultPurchaseOption(long time);
    
    /*
     * Get ReservationInfo for the given reservation ARN
     */
    ReservationInfo getReservation(ReservationArn arn);
    
    /*
     * Get the set of reservation IDs that are active for the given time.
     */
    Set<ReservationArn> getReservations(long time, Product product);

    /**
     * Get reservation info. (Used only for Detailed Billing Report Processing)
     * @param time
     * @param tagGroup
     * @param utilization
     * @return
     */
    ReservationInfo getReservation(
            long time,
            TagGroup tagGroup,
            PurchaseOption purchaseOption,
            InstancePrices instancePrices);

    /**
     * Methods to indicate that we have reservations for each corresponding service.
     */
    boolean hasReservations(Product product);
    
    public void setReservations(Map<PurchaseOption, Map<TagGroup, List<Reservation>>> reservations, Map<ReservationArn, Reservation> reservationsById);

    public static class ReservationInfo {
    	public final TagGroupRI tagGroup;
        public final int capacity;
        public final long start;					// only set when fetching reservations by ARN
        public final long end;						// only set when fetching reservations by ARN
        public final double upfrontAmortized;		// Per-hour amortization of any up-front cost per instance
        public final double reservationHourlyCost;	// Per-hour cost for each reserved instance

        public ReservationInfo(TagGroupRI tagGroup, int capacity, long start, long end, double upfrontAmortized, double reservationHourlyCost) {
        	this.tagGroup = tagGroup;
            this.capacity = capacity;
            this.start = start;
            this.end = end;
            this.upfrontAmortized = upfrontAmortized;
            this.reservationHourlyCost = reservationHourlyCost;
        }

        public static final DateTimeFormatter dateTimeFormatISO = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ").withZone(DateTimeZone.UTC);

        public String toString() {
        	return "tagGroup:{" + tagGroup + "}, capacity:" + capacity +
        			", start:" + new DateTime(start).toString(dateTimeFormatISO) +
        			", end:" + new DateTime(end).toString(dateTimeFormatISO) +
        			", amortized:" + upfrontAmortized +
        			", hourly:" + reservationHourlyCost;
        }
    }
    
    public class ReservationKey implements Comparable<ReservationKey> {
    	public String account;
    	public String region;
    	public String reservationArn;
    	
    	public ReservationKey(String account, String region, String reservationArn) {
    		this.account = account;
    		this.region = region;
    		this.reservationArn = reservationArn;
    	}

		@Override
		public int compareTo(ReservationKey o) {
	        int result = account.compareTo(o.account);
	        if (result != 0)
	            return result;
	        result = region.compareTo(o.region);
	        if (result != 0)
	            return result;
			return reservationArn.compareTo(o.reservationArn);
		}
		
		@Override
		public boolean equals(Object o) {
			ReservationKey rk = (ReservationKey) o;
			return rk == null ? false : compareTo(rk) == 0;
		}
		
		@Override
		public int hashCode() {
			return account.hashCode() * region.hashCode() * reservationArn.hashCode();
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
}
