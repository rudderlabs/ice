package com.netflix.ice.processor.config;

import java.util.List;

public class KubernetesNamespaceMapping {
	private String tag;
	private String value;
	private List<String> patterns;

	public KubernetesNamespaceMapping() {		
	}
	
	public KubernetesNamespaceMapping(String tag, String value, List<String> patterns) {
		this.tag = tag;
		this.value = value;
		this.patterns = patterns;
	}
	
	public String getTag() {
		return tag;
	}
	public void setTag(String tag) {
		this.tag = tag;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public List<String> getPatterns() {
		return patterns;
	}
	public void setPatterns(List<String> patterns) {
		this.patterns = patterns;
	}
}

