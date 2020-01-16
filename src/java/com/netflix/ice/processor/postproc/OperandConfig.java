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

/**
 * OperandConfig specifies the set of tags used to filter and aggregate usage or cost data in a
 * post processor results value expression.
 * 
 * The accounts, regions, and zones attributes provide the ability to filter on a list
 * of values. For accounts the account ID is used, not the account name. If the values for these
 * attributes is left blank or not specified, then no filtering is done.
 * 
 * The product, operation, and usageType fields are regex patterns used to both filter and capture
 * substrings which can be used to build result field values.
 * 
 * If the aggregate attribute is provided, any attribute names listed will be merged and not broken
 * out as separate values in the result.
 * 
 * Allowed aggregate values are: Account, Region, Zone, Product, Operation, UsageType
 * 
 * If the exclude attribute is provided, then for each list of attributes, the specified values
 * for each of that attribute are excluded from the aggregation.
 * 
 * Allowed exclude values are: Account, Region, Zone
 */
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
	private List<String> aggregate;
	private List<String> exclude;
	
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

	public List<String> getAggregate() {
		return aggregate;
	}

	public void setAggregate(List<String> aggregate) {
		this.aggregate = aggregate;
	}

	public List<String> getExclude() {
		return exclude;
	}

	public void setExclude(List<String> exclude) {
		this.exclude = exclude;
	}
	
}
