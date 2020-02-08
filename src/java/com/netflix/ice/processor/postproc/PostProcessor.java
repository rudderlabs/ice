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
	private Map<String, Double[]> operandValueCache;
	
	public PostProcessor(List<RuleConfig> rules, AccountService accountService, ProductService productService) {
		this.rules = rules;
		this.accountService = accountService;
		this.productService = productService;
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
	
	public class OperandKey {
		final public String name;
		final public Product product;
		
		public OperandKey(String name, Product product) {
			this.name = name;
			this.product = product;
		}
		
		public String toString() {
			return "{" + name + "," + product + "}";
		}
	}
	
	protected void processReadWriteData(Rule rule, CostAndUsageData data, boolean isNonResource) throws Exception {
		// Get data maps for operands
		Map<OperandKey, ReadWriteData> opData = Maps.newHashMap();
		for (String name: rule.getOperands().keySet()) {			
			List<Product> products = isNonResource ? Lists.newArrayList(new Product[]{null}) : rule.getOperand(name).getProducts(productService);			
			for (Product p: products) {
				OperandKey opKey = new OperandKey(name, p);
				ReadWriteData rwd = rule.getOperand(name).getType() == OperandType.cost ? data.getCost(p) : data.getUsage(p);
				opData.put(opKey, rwd);
			}
		}
		
		logger.info("opData size: " + opData.size());
		
		// Get data maps for results. Handle case where we're creating a new product
		List<ReadWriteData> resultData = Lists.newArrayList();
		for (Operand result: rule.getResults()) {
			Product p = isNonResource ? null : result.getProduct(productService);
			ReadWriteData rwd = result.getType() == OperandType.usage ? data.getUsage(p) : data.getCost(p);
			if (rwd == null) {
				rwd = new ReadWriteData();
				if (result.getType() == OperandType.usage)
					data.putUsage(p, rwd);
				else
					data.putCost(p, rwd);
			}
			resultData.add(rwd);
		}
		
		logger.info("resultData size: " + resultData.size());
			
		// Get the aggregated value for the input operand
		Map<AggregationTagGroup, Double[]> inData = getInData(rule, data, isNonResource);
		operandValueCache = Maps.newHashMap();
		logger.info("  -- apply rule " + rule.config.getName() + " -- in data size = " + inData.size());
						
		Map<AggregationTagGroup, Map<String, Double[]>> opValues = getOperandValues(rule, inData, opData, data.getMaxNum());
		int results = applyRule(rule, inData, opValues, resultData);
		
		logger.info("  -- data for rule " + rule.config.getName() + " -- in data size = " + inData.size() + ", --- results size = " + results);
	}
	
	/**
	 * Aggregate the data using the regex groups contained in the input filters
	 * @throws Exception 
	 */
	protected Map<AggregationTagGroup, Double[]> getInData(Rule rule, CostAndUsageData data, boolean isNonResource) throws Exception {
		int maxHours = data.getMaxNum();
		InputOperand in = rule.getIn();
		Map<AggregationTagGroup, Double[]> inValues = Maps.newHashMap();
		List<Product> inProducts = isNonResource ? Lists.newArrayList(new Product[]{null}) : in.getProducts(productService);			
		
		for (Product inProduct: inProducts) {
			ReadWriteData inData = in.getType() == OperandType.cost ? data.getCost(inProduct) : data.getUsage(inProduct);
			for (TagGroup tg: inData.getTagGroups()) {
				if (!isNonResource && !inProducts.contains(tg.product))
					continue;
				
				AggregationTagGroup aggregatedTagGroup = in.aggregateTagGroup(tg, accountService, productService);
				if (aggregatedTagGroup == null)
					continue;
				
				Double[] values = inValues.get(aggregatedTagGroup);
				if (values == null) {
					values = new Double[maxHours];
					for (int i = 0; i < values.length; i++)
						values[i] = 0.0;
					inValues.put(aggregatedTagGroup, values);
				}
				for (int hour = 0; hour < inData.getNum(); hour++) {
					Double v = inData.getData(hour).get(tg);
					if (v != null)
						values[hour] += v;
				}
			}
		}
		return inValues;
	}
	
	protected Map<AggregationTagGroup, Map<String, Double[]>> getOperandValues(Rule rule, Map<AggregationTagGroup, Double[]> in, Map<OperandKey, ReadWriteData> opData, int maxHours) {
		//logger.info("get operand values");
		Map<AggregationTagGroup, Map<String, Double[]>> operandValueMap = Maps.newHashMap();
		
		// We only want to walk each ReadWriteData set once, so collect up all the operands for each
		Map<ReadWriteData, Map<String, InputOperand>> dataToOperandMap = Maps.newHashMap();
		for (OperandKey opKey: opData.keySet()) {
			ReadWriteData rwd = opData.get(opKey);
			if (rwd == null)
				continue;

			Map<String, InputOperand> operands = dataToOperandMap.get(rwd);
			if (operands == null) {
				operands = Maps.newHashMap();
				dataToOperandMap.put(rwd, operands);
			}
			operands.put(opKey.name, rule.getOperand(opKey.name));
		}
		
		// Get what we can from the cache
		Map<AggregationTagGroup, List<String>> nonCachedOperands = Maps.newHashMap();
		for (AggregationTagGroup atg: in.keySet()) {
			for (String opName: rule.getOperands().keySet()) {
				String key = rule.getOperand(opName).key(atg);
				if (operandValueCache.containsKey(key)) {
					Map<String, Double[]> opValuesMap = operandValueMap.get(atg);
					if (opValuesMap == null) {
						opValuesMap = Maps.newHashMap();
						operandValueMap.put(atg, opValuesMap);
					}
					opValuesMap.put(opName, operandValueCache.get(key));
				}
				else {
					List<String> opNames = nonCachedOperands.get(atg);
					if (opNames == null) {
						opNames = Lists.newArrayList();
						nonCachedOperands.put(atg, opNames);
					}
					opNames.add(opName);
				}
			}
		}
		
		for (ReadWriteData rwd: dataToOperandMap.keySet()) {
			Map<String, InputOperand> operands = dataToOperandMap.get(rwd);
			for (TagGroup tg: rwd.getTagGroups()) {
				for (AggregationTagGroup atg: in.keySet()) {
					Map<String, Double[]> opValuesMap = operandValueMap.get(atg);
					if (opValuesMap == null) {
						opValuesMap = Maps.newHashMap();
						operandValueMap.put(atg, opValuesMap);
					}
					
					
					for (String opName: operands.keySet()) {
						List<String> nonCachedOpNames = nonCachedOperands.get(atg);
						if (nonCachedOpNames == null || !nonCachedOpNames.contains(opName))
							continue; // we pulled it from the cache
						
						Double[] values = opValuesMap.get(opName);
						if (values == null) {
							values = new Double[maxHours];
							opValuesMap.put(opName, values);
							for (int i = 0; i < values.length; i++)
								values[i] = 0.0;
						}
							
						InputOperand operand = operands.get(opName);
								
						if (operand.matches(atg, tg)) {
							for (int hour = 0; hour < rwd.getNum(); hour++) {
								Double v = rwd.getData(hour).get(tg);
								if (v != null)
									values[hour] += v;
							}
						}					
					}
				}
			}
		}
		
		// Add opValues to the cache
		for (AggregationTagGroup atg: in.keySet()) {
			for (String opName: rule.getOperands().keySet()) {
				String key = rule.getOperand(opName).key(atg);
				if (!operandValueCache.containsKey(key)) {
					Map<String, Double[]> opValuesMap = operandValueMap.get(atg);
					operandValueCache.put(key, opValuesMap.get(opName));
				}
			}
		}
		
		return operandValueMap;
	}
	
	protected Map<String, Double[]> getOperandValueCache() {
		return operandValueCache;
	}
	
	protected int applyRule(Rule rule, Map<AggregationTagGroup, Double[]> in, Map<AggregationTagGroup, Map<String, Double[]>> opValues, List<ReadWriteData> resultData) throws Exception {
		int numResults = 0;
		
		for (AggregationTagGroup atg: in.keySet()) {
			
			// For each result operand...
			for (int i = 0; i < rule.getResults().size(); i++) {
				//logger.info("result " + i + " for atg: " + atg);
				ResultOperand result = rule.getResult(i);
				TagGroup outTagGroup = result.getTagGroup(atg, productService);
				
				String expr = rule.getResultValue(i);
				if (expr != null && !expr.isEmpty()) {
					//logger.info("process hour data");
					eval(rule, expr, in.get(atg), opValues.get(atg), resultData.get(i), outTagGroup);
					numResults++;
				}
			}
		}
		return numResults;
	}
	
	private void eval(Rule rule, 
			String outExpr, 
			Double[] inValues, 
			Map<String, Double[]> opValuesMap, 
			ReadWriteData resultData,
			TagGroup outTagGroup) throws Exception {

		// Process each hour of data
		for (int hour = 0; hour < inValues.length; hour++) {
			// Replace variables
			String expr = outExpr.replace("${in}", inValues[hour].toString());
			
			for (String op: rule.getOperands().keySet()) {			
				// Get the operand values from the proper data source
				Double opValue = opValuesMap.get(op)[hour];
				expr = expr.replace("${" + op + "}", opValue.toString());
			}
			Double value = new Evaluator().eval(expr);		
			resultData.getData(hour).put(outTagGroup, value);
			
			//if (hour == 0)
			//	logger.info("eval: " + outExpr + " = "  + expr + " = " + value + ", tg: " + outTagGroup);
		}
	}
}
