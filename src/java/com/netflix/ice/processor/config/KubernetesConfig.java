package com.netflix.ice.processor.config;

import java.util.List;

public class KubernetesConfig {
	private String bucket;
	private String prefix;
	
	private List<String> clusterNameFormulae;
	private String computeTag;
	private String computeValue;
	private String namespaceTag;
	private List<KubernetesNamespaceMapping> namespaceMappings;
	private List<String> tags;
	
	public String getBucket() {
		return bucket;
	}
	public void setBucket(String bucket) {
		this.bucket = bucket;
	}
	public String getPrefix() {
		return prefix;
	}
	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}
	public List<String> getTags() {
		return tags;
	}
	public void setTags(List<String> tags) {
		this.tags = tags;
	}
	public List<String> getClusterNameFormulae() {
		return clusterNameFormulae;
	}
	public void setClusterNameFormulae(List<String> clusterNameFormulae) {
		this.clusterNameFormulae = clusterNameFormulae;
	}
	public String getComputeTag() {
		return computeTag;
	}
	public void setComputeTag(String computeTag) {
		this.computeTag = computeTag;
	}
	public String getComputeValue() {
		return computeValue;
	}
	public void setComputeValue(String computeValue) {
		this.computeValue = computeValue;
	}
	public String getNamespaceTag() {
		return namespaceTag;
	}
	public void setNamespaceTag(String namespaceTag) {
		this.namespaceTag = namespaceTag;
	}
	public List<KubernetesNamespaceMapping> getNamespaceMappings() {
		return namespaceMappings;
	}
	public void setNamespaceMappings(
			List<KubernetesNamespaceMapping> namespaceMappings) {
		this.namespaceMappings = namespaceMappings;
	}
}

