package com.netflix.ice.common;

import java.util.Map;

import com.google.common.collect.Maps;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.Zone;

/*
 * TagGroupRI adds the reservationId for usage of a reserved instance to a TagGroup.
 * The reservationId is used by the ReservationProcessor to properly associate usage
 * of an RI to the reservation so that lending/borrowing, amortization, reassignment of usage
 * and calculation of unused RIs can be done correctly.
 * 
 * reservationIds are only available when processing the new Cost and Usage Reports, not the old
 * Detailed Billing Reports. TagGroupRIs are converted to TagGroups by the ReservationProcessor
 * once RI usage calculations are done, so only TagGroups are serialized to external data files.
 */
public class TagGroupRI extends TagGroup {
	private static final long serialVersionUID = 1L;
	
	public final String reservationId;

	private TagGroupRI(Account account, Region region, Zone zone,
			Product product, Operation operation, UsageType usageType,
			ResourceGroup resourceGroup, String reservationId) {
		super(account, region, zone, product, operation, usageType,
				resourceGroup);
		this.reservationId = reservationId;
	}

    @Override
    public String toString() {
        return super.toString() + ",\"" + reservationId + "\"";
    }

    public int compareTo(TagGroupRI t) {
        int result = super.compareTo(t);
        if (result != 0)
            return result;
        result = this.reservationId.compareTo(t.reservationId);
        return result;
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
        return this.reservationId.equals(other.reservationId);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + this.reservationId.hashCode();

        return result;
    }
    
    private static Map<TagGroupRI, TagGroupRI> tagGroups = Maps.newConcurrentMap();

    public static TagGroupRI getTagGroup(Account account, Region region, Zone zone, Product product, Operation operation, UsageType usageType, ResourceGroup resourceGroup, String reservationId) {
        TagGroupRI newOne = new TagGroupRI(account, region, zone, product, operation, usageType, resourceGroup, reservationId);
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
