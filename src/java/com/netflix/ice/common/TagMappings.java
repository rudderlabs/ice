package com.netflix.ice.common;

import java.util.List;
import java.util.Map;

public class TagMappings {
	public Map<String, Map<String, List<String>>> maps;
	public List<String> include;
	public List<String> exclude;
	
	public Map<String, Map<String, List<String>>> getMaps() {
		return maps;
	}
	public void setMaps(Map<String, Map<String, List<String>>> maps) {
		this.maps = maps;
	}
	public List<String> getInclude() {
		return include;
	}
	public void setInclude(List<String> include) {
		this.include = include;
	}
	public List<String> getExclude() {
		return exclude;
	}
	public void setExclude(List<String> exclude) {
		this.exclude = exclude;
	}
	
}
	
