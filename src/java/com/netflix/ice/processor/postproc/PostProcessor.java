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

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.AggregationTagGroup;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.CostAndUsageData;
import com.netflix.ice.processor.ReadWriteData;
import com.netflix.ice.processor.postproc.OperandConfig.OperandType;
import com.netflix.ice.tag.Product;

public class PostProcessor {
    protected Logger logger = LoggerFactory.getLogger(getClass());

	private List<RuleConfig> rules;
	private AccountService accountService;
	private ProductService productService;
	private Map<String, Double> operandValueCache;
	
	public PostProcessor(List<RuleConfig> rules, AccountService accountService, ProductService productService) {
		this.rules = rules;
		this.accountService = accountService;
		this.productService = productService;
		this.operandValueCache = Maps.newHashMap();
	}
	
	public void process(CostAndUsageData data) {
		logger.info("Post-process " + rules.size() + " rules");
		for (RuleConfig rc: rules) {
			try {
				process(rc, data);
			} catch (Exception e) {
				logger.error("Error post-processing cost and usage data for rule " + rc.getName() + ": " + e);
				e.printStackTrace();
			}
		}
	}
	
	private boolean isActive(RuleConfig rc, long startMilli) {
		long ruleStart = new DateTime(rc.getStart(), DateTimeZone.UTC).getMillis();
		long ruleEnd = new DateTime(rc.getEnd(), DateTimeZone.UTC).getMillis();
		return startMilli >= ruleStart && startMilli < ruleEnd;
	}
	
	protected void process(RuleConfig rc, CostAndUsageData data) throws Exception {
		// Make sure the rule is in effect for the start date
		if (!isActive(rc, data.getStartMilli())) {
			logger.info("Post-process rule " + rc.getName() + " is not active for this month, start=" + rc.getStart() + ", end=" + rc.getEnd());
			return;
		}
		
		if (rc.getIn().getProduct() == null) {
			logger.error("Post processing rule " + rc.getName() + " has no product specified for the 'in' operand");
			return;
		}
		logger.info("Post-process with rule " + rc.getName() + " on non-resource data");
		Rule rule = new Rule(rc, accountService, productService);
		processReadWriteData(rule, data, true);
		
		logger.info("Post-process with rule " + rc.getName() + " on resource data");
		processReadWriteData(rule, data, false);
	}
	
	protected void processReadWriteData(Rule rule, CostAndUsageData data, boolean isNonResource) throws Exception {
		List<Product> inProducts = rule.getIn().getProducts(productService);
		for (Product inProduct: inProducts) {
			ReadWriteData inUsageData = data.getUsage(isNonResource ? null : inProduct);
			ReadWriteData inCostData = data.getCost(isNonResource ? null : inProduct);
			
			// Get data maps for operands
			Map<String, ReadWriteData> usageData = Maps.newHashMap();
			Map<String, ReadWriteData> costData = Maps.newHashMap();
			for (String name: rule.getOperands().keySet()) {
				if (isNonResource) {
					usageData.put(name, inUsageData);
					costData.put(name, inCostData);
				}
				else {
					List<Product> products = rule.getOperands().get(name).getProducts(productService);
					if (products.isEmpty()) {
						usageData.put(name, inUsageData);
						costData.put(name, inCostData);
					}
					else {
						for (Product p: products) {
							usageData.put(name, (isNonResource || p == null) ? inUsageData : data.getUsage(p));
							costData.put(name, (isNonResource || p == null) ? inCostData : data.getCost(p));
						}
					}
				}
			}
		
			// Get data maps for results. Handle case where we're creating a new product
			List<ReadWriteData> resultData = Lists.newArrayList();
			for (Operand result: rule.getResults()) {
				Product p = result.getProduct(productService);
				ReadWriteData rwd;
				if (result.getType() == OperandType.usage) {
					rwd = (isNonResource || p == null) ? inUsageData : data.getUsage(p);
					if (rwd == null) {
						data.putUsage(p, new ReadWriteData());
						rwd = data.getUsage(p);
					}
				}
				else {
					rwd = (isNonResource || p == null) ? inCostData : data.getCost(p);
					if (rwd == null) {
						data.putCost(p, new ReadWriteData());
						rwd = data.getCost(p);
					}
				}
				resultData.add(rwd);
			}
			
			for (int i = 0; i < inUsageData.getNum(); i++) {
				// For each hour of usage...
			    Map<TagGroup, Double> usageMap = inUsageData.getData(i);
			    Map<TagGroup, Double> costMap = inCostData.getData(i);
			    
				// Get the aggregated value for the input operand
			    Map<AggregationTagGroup, Double> inValues = getInValues(rule, usageMap, costMap, isNonResource);
				if (inValues.size() == 0)
					continue;
					
				applyRule(rule, inValues, i, usageData, costData, resultData);
			}
		}
	}
	
	/**
	 * Aggregate the data using the regex groups contained in the input filters
	 * @throws Exception 
	 */
	protected Map<AggregationTagGroup, Double> getInValues(Rule rule, Map<TagGroup, Double> usageMap, Map<TagGroup, Double> costMap, boolean isNonResource) throws Exception {
		InputOperand in = rule.getIn();
		Map<AggregationTagGroup, Double> inValues = Maps.newHashMap();
		List<Product> inProducts = in.getProducts(productService);
		Map<TagGroup, Double> map = in.getType() == OperandType.cost ? costMap : usageMap;
		for (TagGroup tg: map.keySet()) {
			if (isNonResource && !inProducts.contains(tg.product))
				continue;
			
			AggregationTagGroup aggregatedTagGroup = in.aggregateTagGroup(tg, accountService, productService);
			if (aggregatedTagGroup == null)
				continue;
			
			Double existing = inValues.get(aggregatedTagGroup);
			inValues.put(aggregatedTagGroup, map.get(tg) + ((existing == null) ? 0.0 : existing));			
		}
		return inValues;
	}
	
	protected Double getOperandValue(InputOperand operand, AggregationTagGroup aggregationTagGroup, Map<TagGroup, Double> usageMap, Map<TagGroup, Double> costMap) {
		String key = operand.key(aggregationTagGroup);
		Double value = operandValueCache.get(key);
		if (value != null)
			return value;
		
		// Aggregate the values matching the operand
		Map<TagGroup, Double> map = operand.getType() == OperandType.cost ? costMap : usageMap;
		value = 0.0;
		for (TagGroup tg: map.keySet()) {
			if (operand.matches(aggregationTagGroup, tg)) {
				value += map.get(tg);
			}
		}
		operandValueCache.put(key, value);
		return value;
	}
	
	protected Map<String, Double> getOperandValueCache() {
		return operandValueCache;
	}
	
	protected void applyRule(Rule rule, Map<AggregationTagGroup, Double> in, int hour, Map<String, ReadWriteData> usageData, Map<String, ReadWriteData> costData, List<ReadWriteData> resultData) throws Exception {
		for (AggregationTagGroup atg: in.keySet()) {
			for (int i = 0; i < rule.getResults().size(); i++) {
				
				ResultOperand result = rule.getResult(i);
				TagGroup outTagGroup = result.getTagGroup(atg, productService);
				
				String expr = rule.getResultValue(i);
				if (expr != null && !expr.isEmpty()) {
					Double value = eval(rule, expr, atg, in, hour, usageData, costData);
					//logger.info("put: " + hour + ", " + outTagGroup + ", " + value);
					resultData.get(i).getData(hour).put(outTagGroup, value);
				}
			}
		}
	}
	
	private Double eval(Rule rule, String outExpr, AggregationTagGroup atg, Map<AggregationTagGroup, Double> in, int hour, Map<String, ReadWriteData> usageData, Map<String, ReadWriteData> costData) throws Exception {		
		// Replace variables
		outExpr = outExpr.replace("${in}", in.get(atg).toString());
		
		for (String op: rule.getOperands().keySet()) {			
			// Get the operand value from the proper data source
			Double opValue = getOperandValue(rule.getOperand(op), atg, usageData.get(op).getData(hour), costData.get(op).getData(hour));
			outExpr = outExpr.replace("${" + op + "}", opValue.toString());
		}
		
		Double value = new Evaluator().eval(outExpr);		
		
		//logger.debug(outExpr + " = " + value + " for in: " + tg + ", out: " + outTagGroup);
		return value;
	}
}
