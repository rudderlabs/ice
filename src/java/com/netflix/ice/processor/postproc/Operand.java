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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.processor.postproc.OperandConfig.OperandType;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.Zone;

public abstract class Operand {
	protected final OperandType type;
	protected final List<Account> accounts;
	protected final List<Region> regions;
	protected final List<Zone> zones;
	protected final TagFilter productTagFilter;
	protected final TagFilter operationTagFilter;
	protected final TagFilter usageTypeTagFilter;
	
	public Operand(OperandConfig opConfig, AccountService accountService) {
		this.type = opConfig.getType();
		this.accounts = accountService.getAccounts(opConfig.getAccounts());
		this.regions = Region.getRegions(opConfig.getRegions());
		this.zones = Zone.getZones(opConfig.getZones());
		this.productTagFilter = opConfig.getProduct() == null ? null : new TagFilter(opConfig.getProduct());
		this.operationTagFilter = opConfig.getOperation() == null ? null : new TagFilter(opConfig.getOperation());
		this.usageTypeTagFilter = opConfig.getUsageType() == null ? null : new TagFilter(opConfig.getUsageType());
	}
	
	public OperandType getType() {
		return type;
	}
	
	/**
	 * Get the products that match the product regex group
	 * @param productService
	 * @return List of Product
	 */
	public List<Product> getProducts(ProductService productService) {
		List<Product> products = Lists.newArrayList();
		for (Product p: productService.getProducts()) {
			if (productTagFilter == null || productTagFilter.matches(p.getServiceCode()))
				products.add(p);
		}
		return products;
	}
	
	public Product getProduct(ProductService productService) {
		// Result operands just use the regex directly.
		return productService.getProductByServiceCode(productTagFilter.regex);
	}
		
	public class TagFilter {
		String regex;
		private Pattern pattern;
		
		public TagFilter(String regex) {
			this.regex = regex;
		}
		
		public String getGroup(String name) {
			if (pattern == null)
				pattern = Pattern.compile(regex);
			Matcher matcher = pattern.matcher(name);
			if (matcher.matches()) {
				// If no group specified in the regex, return group 0 (the whole match)
				return matcher.groupCount() == 0 ? matcher.group(0) : matcher.group(1);
			}
			return null;
		}
		
		public boolean matches(String name) {
			if (pattern == null)
				pattern = Pattern.compile(regex);
			Matcher matcher = pattern.matcher(name);
			return matcher.matches();
		}
		
		/**
		 * Build the out tag by replacing any group reference with the group value provided.
		 */
		public String getTag(String group) {
			return regex.replace("${group}", group);
		}
		
		public String toString() {
			return regex;
		}
	}
}