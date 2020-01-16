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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.common.TagGroupRI;
import com.netflix.ice.processor.pricelist.InstancePrices;
import com.netflix.ice.processor.pricelist.InstancePrices.LeaseContractLength;
import com.netflix.ice.processor.pricelist.InstancePrices.OfferingClass;
import com.netflix.ice.processor.pricelist.InstancePrices.Rate;
import com.netflix.ice.processor.ReservationService;
import com.netflix.ice.tag.*;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class BasicReservationService implements ReservationService {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    
    final protected ReservationPeriod term;
    final protected ReservationUtilization defaultUtilization;
    protected Map<ReservationArn, Reservation> reservationsByArn;
    protected Long futureMillis = new DateTime().withYearOfCentury(99).getMillis();
    private Set<Product> hasReservations;
    // Following map used only for DBR processing
    protected Map<ReservationUtilization, Map<TagGroup, List<Reservation>>> reservations;

    public BasicReservationService(ReservationPeriod term, ReservationUtilization defaultUtilization) {
        this.term = term;
        this.defaultUtilization = defaultUtilization;

        reservations = Maps.newHashMap();
        for (ReservationUtilization utilization: ReservationUtilization.values()) {
            reservations.put(utilization, Maps.<TagGroup, List<Reservation>>newHashMap());
        }
        reservationsByArn = Maps.newHashMap();
        hasReservations = Sets.newHashSet();
    }
    
    public BasicReservationService(Map<ReservationArn, Reservation> reservations) {
    	this.term = null;
    	this.defaultUtilization = null;
    	this.reservationsByArn = reservations;
    	updateHasSet();
    }

    // For testing
    public void injectReservation(Reservation res) {
    	reservationsByArn.put(res.tagGroup.arn, res);
    }
    
    public void setReservations(Map<ReservationUtilization, Map<TagGroup, List<Reservation>>> reservations, Map<ReservationArn, Reservation> reservationsByArn) {
    	this.reservations = reservations;
    	this.reservationsByArn = reservationsByArn;
    	updateHasSet();    	
    }
    
    private void updateHasSet() {
    	this.hasReservations = Sets.newHashSet();
    	for (Reservation r: reservationsByArn.values()) {
    		hasReservations.add(r.tagGroup.product);
    	}
    }
    
    /**
     * Methods to indicate that we have reservations for each corresponding service.
     */
    public boolean hasReservations(Product product) {
    	return this.hasReservations.contains(product);
    }

    public static class Reservation {
    	final public TagGroupRI tagGroup;
    	final public int count;
    	final public long start; // Reservation start time rounded down to starting hour mark where it takes effect
    	final public long end; // Reservation end time rounded down to ending hour mark where reservation actually ends
    	final public ReservationUtilization utilization;
    	final public double hourlyFixedPrice; // Per-hour amortization of any up-front cost per instance
    	final public double usagePrice; // Per-hour cost for each reserved instance

        public Reservation(
        		TagGroupRI tagGroup,
                int count,
                long start,
                long end,
                ReservationUtilization utilization,
                double hourlyFixedPrice,
                double usagePrice) {
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

    public ReservationUtilization getDefaultReservationUtilization(long time) {
        return defaultUtilization;
    }
    
    /*
     * Get ReservationInfo for the given reservation ARN
     */
    public ReservationInfo getReservation(ReservationArn arn) {
    	Reservation reservation = reservationsByArn.get(arn);
    	if (reservation == null)
    		return null;
	    return new ReservationInfo(reservation.tagGroup, reservation.count, reservation.hourlyFixedPrice, reservation.usagePrice);
    }
    
    /*
     * Get the set of reservation IDs that are active for the given time.
     */
    public Set<ReservationArn> getReservations(long time, Product product) {
    	Set<ReservationArn> arns = Sets.newHashSet();
    	for (Reservation r: reservationsByArn.values()) {
    		if (time >= r.start && time < r.end && (product == null || r.tagGroup.product == product))
    			arns.add(r.tagGroup.arn);
    	}
    	return arns;
    }
    
    public Map<ReservationArn, Reservation> getReservations() {
    	return reservationsByArn;
    }
    
    /*
     * The following methods are used exclusively by Detailed Billing Report Reservation Processor
     */
    public Collection<TagGroup> getTagGroups(ReservationUtilization utilization, Long startMilli, Product product) {
    	// Only return tagGroups with active reservations for the requested start time
    	Set<TagGroup> tagGroups = Sets.newHashSet();
    	for (TagGroup t: reservations.get(utilization).keySet()) {
    		List<Reservation> resList = reservations.get(utilization).get(t);
    		for (Reservation r: resList) {
	            if (startMilli >= r.start && startMilli < r.end && (product == null || r.tagGroup.product == product)) {
	            	tagGroups.add(t);
	            	break;
	            }
    		}
    	}
        return tagGroups;
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
	    
	    return new ReservationInfo(TagGroupRI.get(tagGroup), count, upfrontAmortized, hourlyCost);
	}

}
