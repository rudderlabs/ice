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
package com.netflix.ice.tag;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang.StringUtils;

public class ResourceGroup extends Tag {
	private static final long serialVersionUID = 1L;
	
	public static final String separator = "|";
	private static final String splitRegex = "\\|";
	
	private UserTag[] resourceTags;
	/**
	 * isProductName indicates that the resourceTags is simply the product name.
	 */
	private final boolean isProductName;
    private static ConcurrentMap<String, ResourceGroup> resourceGroups = Maps.newConcurrentMap();

	protected ResourceGroup(String name, boolean isProductName) {
        super(name);
        this.isProductName = isProductName;
    }
	
	protected ResourceGroup(String[] tags) {
		super(StringUtils.join(tags, separator));
		this.isProductName = false;
		resourceTags = new UserTag[tags.length];
		for (int i = 0; i < tags.length; i++)
			resourceTags[i] = UserTag.get(tags[i]);
	}
	
	public boolean isProductName() {
		return isProductName;
	}
	
	public UserTag[] getUserTags() {
		if (resourceTags == null) {
			String[] tags = name.split(splitRegex, -1);
			resourceTags = new UserTag[tags.length];
			for (int i = 0; i < tags.length; i++)
				resourceTags[i] = UserTag.get(tags[i]);
		}
		return resourceTags;
	}
	
    public static ResourceGroup getResourceGroup(String name, boolean isProductName) {
    	if (name.contains(separator)) {
    		return getResourceGroup(name.split(splitRegex, -1));
    	}
        ResourceGroup resourceGroup = resourceGroups.get(name);
        if (resourceGroup == null)
        	resourceGroup = resourceGroups.putIfAbsent(name, new ResourceGroup(name, isProductName));
        return resourceGroup;
    }

    public static ResourceGroup getResourceGroup(String[] tags) {
    	String name = StringUtils.join(tags, separator);
        ResourceGroup resourceGroup = resourceGroups.get(name);
        if (resourceGroup == null)
        	resourceGroup = resourceGroups.putIfAbsent(name, new ResourceGroup(tags));
        return resourceGroup;
    }

    public static ResourceGroup getResourceGroup(UserTag[] tags) {
    	String[] strings = new String[tags.length];
    	for (int i = 0; i < tags.length; i++)
    		strings[i] = tags[i] == null ? null : tags[i].name;
    	return getResourceGroup(strings);
    }

    public static List<ResourceGroup> getResourceGroups(List<String> names) {
        List<ResourceGroup> result = Lists.newArrayList();
        if (names != null) {
            for (String name: names) {
                ResourceGroup resourceGroup = resourceGroups.get(name);
                if (resourceGroup != null)
                    result.add(resourceGroup);
            }
        }
        return result;
    }
}
