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
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.postproc.OperandConfig.OperandType;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.Zone;

public class Rule {
    protected Logger logger = LoggerFactory.getLogger(getClass());

	private RuleConfig config;
	private Map<String, Operand> operands;
	private Operand in;
	private List<Operand> results;
	
	public Rule(RuleConfig config, AccountService accountService, ProductService productService) throws Exception {
		this.config = config;
		
		// Check for mandatory values in the config
		if (StringUtils.isEmpty(config.getName()) ||
				StringUtils.isEmpty(config.getStart()) ||
				StringUtils.isEmpty(config.getEnd()) ||
				config.getIn() == null ||
				config.getResults() == null) {
			String err = "Missing required parameters in post processor rule config for " + config.getName() + ". Must have: name, start, end, in, and results";
			logger.error(err);
			throw new Exception(err);
		}
		
		operands = Maps.newHashMap();
		for (String oc: config.getOperands().keySet()) {
			operands.put(oc, new Operand(config.getOperand(oc), accountService, productService));
		}
		
		in = new Operand(config.getIn(), accountService, productService);
		results = Lists.newArrayList();
		for (ResultConfig rc: config.getResults()) {
			results.add(new Operand(rc.getResult(), accountService, productService));
		}
	}
	
	public Operand getOperand(String name) {
		return operands.get(name);
	}
	
	public Map<String, Operand> getOperands() {
		return operands;
	}
	
	public Operand getIn() {
		return in;
	}
	
	public List<Operand> getResults() {
		return results;
	}
	
	public Operand getResult(int index) {
		return results.get(index);
	}
	
	public String getResultValue(int index) {
		return config.getResults().get(index).getValue();
	}
	
	public class Operand {
		private final OperandType type;
		private final List<Account> accounts;
		private final List<Region> regions;
		private final List<Zone> zones;
		private final Product product;
		private final TagFilter operationTagFilter;
		private final TagFilter usageTypeTagFilter;
		
		public Operand(OperandConfig opConfig, AccountService accountService, ProductService productService) {
			this.type = opConfig.getType();
			this.accounts = accountService.getAccounts(opConfig.getAccounts());
			this.regions = Region.getRegions(opConfig.getRegions());
			this.zones = Zone.getZones(opConfig.getZones());
			this.product = opConfig.getProduct() == null ? null : productService.getProductByServiceCode(opConfig.getProduct());
			this.operationTagFilter = opConfig.getOperation() == null ? null : new TagFilter(opConfig.getOperation());
			this.usageTypeTagFilter = opConfig.getUsageType() == null ? null : new TagFilter(opConfig.getUsageType());
		}
		
		public OperandType getType() {
			return type;
		}
		
		public Product getProduct() {
			return product;
		}
		
		public TagGroup aggregateTagGroup(TagGroup tg) {
			Operation operation = tg.operation;
			UsageType usageType = tg.usageType;
			
	        if (accounts != null && accounts.size() > 0 && !accounts.contains(tg.account)) {
	        	return null;
	        }
	        if (regions != null && regions.size() > 0 && !regions.contains(tg.region)) {
	        	return null;
	        }
	        if (zones != null && zones.size() > 0 && !zones.contains(tg.zone)) {
	        	return null;
	        }
	        if (product != null && product != tg.product) {
	        	return null;
	        }
	        if (operationTagFilter != null) {
	        	String op = operationTagFilter.getGroup(tg.operation.name);
	        	if (op == null)
	        		return null;
	        	
	        	operation = Operation.getOperation(op);
	        }
	        if (usageTypeTagFilter != null) {
	        	String ut = usageTypeTagFilter.getGroup(tg.usageType.name);
	        	if (ut == null)
	        		return null;
	        	
	        	usageType = UsageType.getUsageType(ut, tg.usageType.unit);
	        }
			
			return TagGroup.getTagGroup(tg.account, tg.region, tg.zone, tg.product, operation, usageType, tg.resourceGroup);
		}
		
		/**
		 * Get the operand TagGroup based on the supplied TagGroup. Values present in the operand config are used to
		 * override the values in the supplied TagGroup. For tags that have lists, there should only be one entry, so
		 * the first is always chosen.
		 */
		public TagGroup getTagGroup(TagGroup tg) {
			return TagGroup.getTagGroup(
					accounts.size() > 0 ? accounts.get(0) : tg.account,
					regions.size() > 0 ? regions.get(0) : tg.region,
					zones.size() > 0 ? zones.get(0) : tg.zone,
					product != null ? product : tg.product,
					operationTagFilter != null ? Operation.getOperation(operationTagFilter.getTag(tg.operation.name)) : tg.operation,
					usageTypeTagFilter != null ? UsageType.getUsageType(usageTypeTagFilter.getTag(tg.usageType.name), tg.usageType.unit) : tg.usageType,
					tg.resourceGroup);
		}
	}
	
	public class TagFilter {
		private String regex;
		private Pattern pattern;
		
		public TagFilter(String regex) {
			this.regex = regex;
		}
		
		public String getGroup(String name) {
			if (pattern == null)
				pattern = Pattern.compile(regex);
			Matcher matcher = pattern.matcher(name);
			if (matcher.matches())
				return matcher.group(1);
			return null;
		}
		
		/**
		 * Build the out tag by replacing any group reference with the group value provided.
		 */
		public String getTag(String group) {
			return regex.replace("${group}", group);
		}
	}
	
}

