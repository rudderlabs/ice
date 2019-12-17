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
package com.netflix.ice.processor.postproc;

import java.util.List;

import com.google.common.collect.Lists;

public class OperandConfig {
	public enum OperandType {
		cost,
		usage;
	}
	
	private OperandType type;
	private List<String> accounts;
	private List<String> regions;
	private List<String> zones;
	private String product;
	private String operation;
	private String usageType;
	
	public OperandConfig() {
		accounts = Lists.newArrayList();
		regions = Lists.newArrayList();
		zones = Lists.newArrayList();
	}
	
	public OperandType getType() {
		return type;
	}
	public void setType(OperandType type) {
		this.type = type;
	}
	public List<String> getAccounts() {
		return accounts;
	}
	public void setAccounts(List<String> accounts) {
		this.accounts = accounts;
	}
	public List<String> getRegions() {
		return regions;
	}
	public void setRegions(List<String> regions) {
		this.regions = regions;
	}
	public List<String> getZones() {
		return zones;
	}
	public void setZones(List<String> zones) {
		this.zones = zones;
	}
	public String getProduct() {
		return product;
	}
	public void setProduct(String product) {
		this.product = product;
	}
	public String getOperation() {
		return operation;
	}
	public void setOperation(String operation) {
		this.operation = operation;
	}
	public String getUsageType() {
		return usageType;
	}
	public void setUsgaeType(String usageType) {
		this.usageType = usageType;
	}
	
}
