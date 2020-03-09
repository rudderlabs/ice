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
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.Tag;
import com.netflix.ice.tag.TagType;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.Zone;

public class Aggregation {
    private Map<AggregationTagGroup, AggregationTagGroup> tagGroups;
    private List<TagType> groupByTags;
    
    public Aggregation(List<TagType> groupByTags) {
    	this.groupByTags = groupByTags;
    	this.tagGroups = Maps.newConcurrentMap();
    }

    public AggregationTagGroup getAggregationTagGroup(TagGroup tagGroup) throws Exception {
    	return getAggregationTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, tagGroup.operation, tagGroup.usageType, tagGroup.resourceGroup);
    }
    
    public AggregationTagGroup getAggregationTagGroup(Account account, Region region, Zone zone, Product product, Operation operation, UsageType usageType, ResourceGroup resourceGroup) throws Exception {
    	List<Tag> tags = Lists.newArrayListWithCapacity(groupByTags.size());
    	for (TagType tt: groupByTags) {
    		switch (tt) {
    		case Account: 		tags.add(account); break;
    		case Region: 		tags.add(region); break;
    		case Zone: 			tags.add(zone); break;
    		case Product: 		tags.add(product); break;
    		case Operation: 	tags.add(operation); break;
    		case UsageType: 	tags.add(usageType); break;
    		case ResourceGroup: tags.add(resourceGroup); break;
			default:
				throw new Exception("Unsupported tag type aggregation");
    		}
    	}
    	AggregationTagGroup newOne = new AggregationTagGroup(tags, groupByTags);
    	AggregationTagGroup oldOne = tagGroups.get(newOne);
        if (oldOne != null) {
            return oldOne;
        }
        else {
            tagGroups.put(newOne, newOne);
            return newOne;
        }
    }
    
    public boolean contains(TagType tagType) {
    	return groupByTags.contains(tagType);
    }
}
