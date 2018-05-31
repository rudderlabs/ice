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
		String key = name.toLowerCase();
        UserTag tag = tagsByName.get(key);
        if (tag == null) {
        	tagsByName.putIfAbsent(key, new UserTag(name));
        	tag = tagsByName.get(key);
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
