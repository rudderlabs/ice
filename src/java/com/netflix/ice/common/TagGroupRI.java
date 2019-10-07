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
package com.netflix.ice.common;

import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.Maps;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.ReservationArn;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.Zone;
import com.netflix.ice.tag.Zone.BadZone;

/*
 * TagGroupRI adds the reservationArn for usage of a reserved instance to a TagGroup.
 * The reservationArn is used by the ReservationProcessor to properly associate usage
 * of an RI to the reservation so that lending/borrowing, amortization, reassignment of usage
 * and calculation of unused RIs can be done correctly.
 * 
 * reservationArns are only available when processing the new Cost and Usage Reports, not the old
 * Detailed Billing Reports. TagGroupRIs are converted to TagGroups by the ReservationProcessor
 * once RI usage calculations are done, so only TagGroups are serialized to external data files.
 */
public class TagGroupRI extends TagGroup {
	private static final long serialVersionUID = 1L;
	
	public final ReservationArn reservationArn;

	private TagGroupRI(Account account, Region region, Zone zone,
			Product product, Operation operation, UsageType usageType,
			ResourceGroup resourceGroup, ReservationArn reservationArn) {
		super(account, region, zone, product, operation, usageType,
				resourceGroup);
		this.reservationArn = reservationArn;
	}

    @Override
    public String toString() {
        return super.toString() + ",\"" + reservationArn + "\"";
    }

    @Override
    public String compareKey() {
    	return reservationArn.name;
    }
    
    @Override
    public boolean equals(Object o) {
    	if (this == o)
    		return true;
        if (o == null)
            return false;
        if (!super.equals(o))
        	return false;
        TagGroupRI other = (TagGroupRI)o;
        return this.reservationArn == other.reservationArn;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((this.reservationArn != null) ? this.reservationArn.hashCode() : 0);
        return result;
    }
    
    private static Map<TagGroupRI, TagGroupRI> tagGroups = Maps.newConcurrentMap();

    public static TagGroupRI get(TagGroup tg) {
    	if (tg instanceof TagGroupRI)
    		return (TagGroupRI) tg;
    	return get(tg.account, tg.region, tg.zone, tg.product, tg.operation, tg.usageType, tg.resourceGroup, null);
    }
    
    public static TagGroupRI get(String account, String region, String zone, String product, String operation, String usageTypeName, String usageTypeUnit, String resourceGroup, String reservationArn, AccountService accountService, ProductService productService) throws BadZone {
        Region r = Region.getRegionByName(region);
        return get(
    		accountService.getAccountByName(account),
        	r, StringUtils.isEmpty(zone) ? null : r.getZone(zone),
        	productService.getProductByName(product),
        	Operation.getOperation(operation),
            UsageType.getUsageType(usageTypeName, usageTypeUnit),
            StringUtils.isEmpty(resourceGroup) ? null : ResourceGroup.getResourceGroup(resourceGroup, resourceGroup.equals(product)),
            ReservationArn.get(reservationArn));   	
    }
    
    public static TagGroupRI get(Account account, Region region, Zone zone, Product product, Operation operation, UsageType usageType, ResourceGroup resourceGroup, ReservationArn reservationArn) {
        TagGroupRI newOne = new TagGroupRI(account, region, zone, product, operation, usageType, resourceGroup, reservationArn);
        TagGroupRI oldOne = tagGroups.get(newOne);
        if (oldOne != null) {
            return oldOne;
        }
        else {
            tagGroups.put(newOne, newOne);
            return newOne;
        }
    }

}
