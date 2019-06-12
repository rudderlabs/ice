package com.netflix.ice.processor;

import java.util.List;
import java.util.Map;

public class AccountConfig {
	public String id;
	public String name;
	public String awsName;
	public Map<String, String> tags;
	public List<String> riProducts;
	public String role;
	public String externalId;
	

	public AccountConfig() {
	}
		
	public AccountConfig(String id, String name, String awsName, Map<String, String> tags, List<String> riProducts, String role, String externalId) {
		this.id = id;
		this.name = name;
		this.awsName = awsName;
		this.tags = tags;
		this.riProducts = riProducts;
		this.role = role;
		this.externalId = externalId;
	}
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getAwsName() {
		return awsName;
	}

	public void setAwsName(String awsName) {
		this.awsName = name;
	}

	public Map<String, String> getDefaultTags() {
		return tags;
	}

	public void setDefaultTags(Map<String, String> defaultTags) {
		this.tags = defaultTags;
	}

	public List<String> getRiProducts() {
		return riProducts;
	}

	public void setRiProducts(List<String> riProducts) {
		this.riProducts = riProducts;
	}

	public String getRole() {
		return role;
	}

	public void setRole(String role) {
		this.role = role;
	}

	public String getExternalId() {
		return externalId;
	}

	public void setExternalId(String externalId) {
		this.externalId = externalId;
	}


}
