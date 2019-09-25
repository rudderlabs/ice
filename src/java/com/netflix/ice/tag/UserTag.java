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

import java.util.List;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class UserTag extends Tag {
	private static final long serialVersionUID = 1L;
	public static final String none = "(none)";

    private static ConcurrentMap<String, UserTag> tagsByName = Maps.newConcurrentMap();

	private UserTag(String name) {
		super(name);
	}
	
	public static UserTag get(String name) {
		if (name == null)
			return null;
        UserTag tag = tagsByName.get(name);
        if (tag == null) {
        	tagsByName.putIfAbsent(name, new UserTag(name));
        	tag = tagsByName.get(name);
        }
        return tag;
	}
	
	public static List<UserTag> getUserTags(List<String> names) {
		List<UserTag> tags = Lists.newArrayList();
		for (String name: names) {
			tags.add(get(name.equals(none) ? "" : name));
		}
		return tags;
	}
}
