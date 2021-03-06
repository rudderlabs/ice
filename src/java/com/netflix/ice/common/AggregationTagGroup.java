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

import java.util.List;

import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.Tag;
import com.netflix.ice.tag.TagType;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.UserTag;
import com.netflix.ice.tag.Zone;

public class AggregationTagGroup {
	public final List<Tag> tags;
	private final List<TagType> types;
	public final List<UserTag> userTags;
	private final List<Integer> userTagIndeces;
	private final int hashcode;
	
	protected AggregationTagGroup(List<Tag> tags, List<TagType> types, List<UserTag> userTags, List<Integer> userTagIndeces) {
		this.tags = tags;
		this.types = types;
		this.userTags = userTags;
		this.userTagIndeces = userTagIndeces;
		this.hashcode = genHashCode();
	}
	
	public Account getAccount() {
		int index = types.indexOf(TagType.Account);
		return index < 0 ? null : (Account) tags.get(index); 
	}
	
	public Region getRegion() {
		int index = types.indexOf(TagType.Region);
		return index < 0 ? null : (Region) tags.get(index); 
	}
	
	public Zone getZone() {
		int index = types.indexOf(TagType.Zone);
		return index < 0 ? null : (Zone) tags.get(index); 
	}
	
	public Product getProduct() {
		int index = types.indexOf(TagType.Product);
		return index < 0 ? null : (Product) tags.get(index); 
	}
	
	public Operation getOperation() {
		int index = types.indexOf(TagType.Operation);
		return index < 0 ? null : (Operation) tags.get(index); 
	}
	
	public UsageType getUsageType() {
		int index = types.indexOf(TagType.UsageType);
		return index < 0 ? null : (UsageType) tags.get(index); 
	}
	
	public ResourceGroup getResourceGroup(int numUserTags) {
		if (userTags == null)
			return null;
		
		String[] tags = new String[numUserTags];
		for (int i = 0; i < numUserTags; i++) {
			int tagIndex = userTagIndeces.indexOf(i);
			tags[i] = tagIndex < 0 ? null : userTags.get(tagIndex).name;
		}
		return ResourceGroup.getResourceGroup(tags);
	}
	
	public UserTag getUserTag(Integer index) {
		if (userTagIndeces == null || userTags == null)
			return null;
		
		int i = userTagIndeces.indexOf(index);
		return i < 0 ? null : userTags.get(i);
	}
	
    public boolean groupBy(TagType tagType) {
    	return types.contains(tagType);
    }
    
    public boolean groupByUserTag(Integer index) {
    	return userTagIndeces == null ? false : userTagIndeces.contains(index);
    }
	
    @Override
    public String toString() {
        return userTags == null ? tags.toString() : String.join(",", tags.toString(), userTags.toString());
    }

    public int compareTo(AggregationTagGroup other) {
    	for (int i = 0; i < tags.size(); i++) {
    		Tag t = tags.get(i);
    		Tag o = other.tags.get(i);
            int result = t == o ? 0 : (t == null ? 1 : (o == null ? -1 : o.compareTo(t)));
    		if (result != 0)
    			return result;
    	}
    	
    	int result = userTags == other.userTags ? 0 : (userTags == null ? 1 : (other.userTags == null ? -1 : 0));
    	if (result != 0)
    		return result;
    	
    	for (int i = 0; i < userTags.size(); i++) {
    		Tag t = userTags.get(i);
    		Tag o = other.userTags.get(i);
            result = t == o ? 0 : (t == null ? 1 : (o == null ? -1 : o.compareTo(t)));
    		if (result != 0)
    			return result;
    	}
    	return 0;
    }
    
    @Override
    public boolean equals(Object o) {
    	if (this == o)
    		return true;
        if (o == null)
            return false;
        AggregationTagGroup other = (AggregationTagGroup)o;
        
        
    	for (int i = 0; i < tags.size(); i++) {
    		if (tags.get(i) != other.tags.get(i))
    			return false;
    	}
    	if (userTags != null || other.userTags != null) {
    		// at least one is not null
    		if (userTags == null || other.userTags == null)
    			return false; // only one is null
    		
	    	for (int i = 0; i < userTags.size(); i++) {
	    		if (userTags.get(i) != other.userTags.get(i))
	    			return false;
	    	}
    	}
    	return true;
    }

    @Override
    public int hashCode() {
    	return hashcode;
    }
    
    private int genHashCode() {
        final int prime = 31;
        int result = 1;
        
    	for (Tag t: tags) {
    		if (t != null)
    			result = prime * result + t.hashCode();
    	}
    	if (userTags != null) {
	    	for (Tag t: userTags) {
	    		if (t != null)
	    			result = prime * result + t.hashCode();
	    	}
	    }
        
        return result;
    }

}
