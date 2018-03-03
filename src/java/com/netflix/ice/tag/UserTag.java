package com.netflix.ice.tag;

import java.util.List;

import com.google.common.collect.Lists;

public class UserTag extends Tag {
	private static final long serialVersionUID = 1L;
	public static final String none = "<none>";

	public UserTag(String name) {
		super(name);
	}
	
	public static List<UserTag> getUserTags(List<String> names) {
		List<UserTag> tags = Lists.newArrayList();
		for (String name: names) {
			tags.add(new UserTag(name.equals(none) ? "" : name));
		}
		return tags;
	}

}
