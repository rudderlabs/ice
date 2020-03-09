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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.time.StopWatch;
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
    protected boolean debug = false;

	private List<RuleConfig> rules;
	private AccountService accountService;
	private ProductService productService;
	
	private int cacheMisses;
	private int cacheHits;
	
	public PostProcessor(List<RuleConfig> rules, AccountService accountService, ProductService productService) {
		this.rules = rules;
		this.accountService = accountService;
		this.productService = productService;
		this.cacheMisses = 0;
		this.cacheHits = 0;
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
		// Get data maps for operands
		int opDataSize = 0;
		Map<String, List<ReadWriteData>> dataByOperand = Maps.newHashMap();
		for (String name: rule.getOperands().keySet()) {			
			List<Product> products = isNonResource ? Lists.newArrayList(new Product[]{null}) : rule.getOperand(name).getProducts(productService);			
			for (Product p: products) {
				ReadWriteData rwd = rule.getOperand(name).getType() == OperandType.cost ? data.getCost(p) : data.getUsage(p);
				if (rwd == null)
					continue;
				
				List<ReadWriteData> dataList = dataByOperand.get(name);
				if (dataList == null) {
					dataList = Lists.newArrayList();
					dataByOperand.put(name, dataList);
				}
				dataList.add(rwd);
				opDataSize++;
			}
		}
		
		logger.info("opData size: " + opDataSize);
		
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
		logger.info("  -- apply rule " + rule.config.getName() + " -- in data size = " + inData.size());
						
		Map<AggregationTagGroup, Map<String, Double[]>> opValues = getOperandValues(rule, inData, dataByOperand, data.getMaxNum());
		int results = applyRule(rule, inData, opValues, resultData);
		
		logger.info("  -- data for rule " + rule.config.getName() + " -- in data size = " + inData.size() + ", --- results size = " + results);
	}
	
	/**
	 * Aggregate the data using the regex groups contained in the input filters
	 * @throws Exception 
	 */
	protected Map<AggregationTagGroup, Double[]> getInData(Rule rule, CostAndUsageData data, boolean isNonResource) throws Exception {
		StopWatch sw = new StopWatch();
		sw.start();
		
		InputOperand in = rule.getIn();
		int maxHours = in.isMonthly() ? 1 : data.getMaxNum();
		Map<AggregationTagGroup, Double[]> inValues = Maps.newHashMap();
		List<Product> inProducts = isNonResource ? Lists.newArrayList(new Product[]{null}) : in.getProducts(productService);			
		
		for (Product inProduct: inProducts) {
			ReadWriteData inData = in.getType() == OperandType.cost ? data.getCost(inProduct) : data.getUsage(inProduct);
			if (inData == null)
				continue;
			
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
						values[in.isMonthly() ? 0 : hour] += v;
				}
			}
		}
		logger.info("getInData elapsed time: " + sw);
		return inValues;
	}
	
	/*
	 * Returns a map containing the operands needed to compute the results for each aggregation tag group.
	 * Each aggregation tag group entry hold a map of Double arrays keyed by the operand name.
	 */
	protected Map<AggregationTagGroup, Map<String, Double[]>> getOperandValues(Rule rule, Map<AggregationTagGroup, Double[]> in, Map<String, List<ReadWriteData>> dataByOperand, int maxHours) throws Exception {
		StopWatch sw = new StopWatch();
		sw.start();
		
		Map<AggregationTagGroup, Map<String, Double[]>> operandValueMap = Maps.newHashMap();
		Map<String, Double[]> operandValueCache = Maps.newHashMap();
		cacheMisses = 0;
		cacheHits = 0;
		
		// Determine which operands are aggregations that require scanning all the data
		Map<String, InputOperand> aggregationOperands = Maps.newHashMap();
		for (String opName: rule.getOperands().keySet()) {
			InputOperand op = rule.getOperand(opName);
			if (op.hasAggregation()) {
				// Queue the operand for tagGroup scanning
				aggregationOperands.put(opName, op);
			}
			else {
				// Get the values directly
				getValuesForOperand(opName, op, in, dataByOperand.get(opName), maxHours, operandValueMap, operandValueCache);
			}
		}
		if (!aggregationOperands.isEmpty()) {
			getAggregatedOperandValues(rule, aggregationOperands, in, dataByOperand, maxHours, operandValueMap, operandValueCache);
		}
		logger.info("getOperandValues elapsed time: " + sw + ", cache hits = " + cacheHits + ", cache misses = " + cacheMisses);
		return operandValueMap;
	}
	
	protected void getValuesForOperand(
			String opName,
			InputOperand op,
			Map<AggregationTagGroup, Double[]> in, 
			List<ReadWriteData> dataList,
			int maxHours,
			Map<AggregationTagGroup, Map<String, Double[]>> operandValueMap,
			Map<String, Double[]> operandValueCache) throws Exception {
		
		// Get the data map for the operand - there should only be one
		for (ReadWriteData data: dataList) {
			for (AggregationTagGroup atg: in.keySet()) {
				TagGroup tg = op.tagGroup(atg, accountService, productService);
				
				Map<String, Double[]> opValuesMap = operandValueMap.get(atg);
				if (opValuesMap == null) {
					opValuesMap = Maps.newHashMap();
					operandValueMap.put(atg, opValuesMap);
				}
				
				// See if the values are in the cache
				String cacheKey = op.cacheKey(atg);
				if (operandValueCache.containsKey(cacheKey)) {
					opValuesMap.put(opName, operandValueCache.get(cacheKey));
					cacheHits++;
					continue;
				}
	
				Double[] values = new Double[op.isMonthly() ? 1 : maxHours];
				for (int i = 0; i < values.length; i++)
					values[i] = 0.0;
				opValuesMap.put(opName, values);
				
				for (int hour = 0; hour < data.getNum(); hour++) {
					Double v = data.getData(hour).get(tg);
					values[op.isMonthly() ? 0 : hour] += v != null ? v : 0.0;
				}
				if (data.getNum() < maxHours && !op.isMonthly()) {
					// Fill out the remainder of the array
					for (int hour = data.getNum(); hour < maxHours; hour++)
						values[hour] = 0.0;
				}
				operandValueCache.put(cacheKey, values);
				cacheMisses++;
			}
		}
	}
	
	protected void getAggregatedOperandValues(
			Rule rule,
			Map<String, InputOperand> aggregationOperands, 
			Map<AggregationTagGroup, Double[]> in, 
			Map<String, List<ReadWriteData>> dataByOperand, 
			int maxHours,
			Map<AggregationTagGroup, Map<String, Double[]>> operandValueMap,
			Map<String, Double[]> operandValueCache) {
		
		StopWatch sw = new StopWatch();
		sw.start();
				
		Map<ReadWriteData, Collection<TagGroup>> tagGroupsCache = Maps.newHashMap();
		
		for (AggregationTagGroup atg: in.keySet()) {
			for (String opName: aggregationOperands.keySet()) {
				
				Map<String, Double[]> opValuesMap = operandValueMap.get(atg);
				if (opValuesMap == null) {
					opValuesMap = Maps.newHashMap();
					operandValueMap.put(atg, opValuesMap);
				}
				
				InputOperand op = rule.getOperand(opName);
				
				// See if the values are in the cache
				String cacheKey = op.cacheKey(atg);
				if (operandValueCache.containsKey(cacheKey)) {
					opValuesMap.put(opName, operandValueCache.get(cacheKey));
					cacheHits++;
					continue;
				}

				Double[] values = opValuesMap.get(opName);
				if (values == null) {
					values = new Double[op.isMonthly() ? 1 : maxHours];
					opValuesMap.put(opName, values);
					for (int i = 0; i < values.length; i++)
						values[i] = 0.0;
				}
				
				for (ReadWriteData rwd: dataByOperand.get(opName)) {
					Collection<TagGroup> tagGroups = tagGroupsCache.get(rwd);
					if (tagGroups == null) {
						tagGroups = rwd.getTagGroups();
						tagGroupsCache.put(rwd, tagGroups);
					}
					
					for (TagGroup tg: tagGroups) {															
						if (op.matches(atg, tg)) {
							for (int hour = 0; hour < rwd.getNum(); hour++) {
								Double v = rwd.getData(hour).get(tg);
								if (v != null)
									values[op.isMonthly() ? 0 : hour] += v;
							}
						}					
					}					
				}
				operandValueCache.put(cacheKey, values);
				if (debug)
					logger.info("operand " + opName + " value for hour 0: " + values[0]);
				cacheMisses++;				
			}
		}
				
		logger.info("getAggregatedOperandValues elapsed time: " + sw);
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
					eval(i, rule, expr, in.get(atg), opValues.get(atg), resultData.get(i), outTagGroup);
					numResults++;
				}
			}
		}
		return numResults;
	}
	
	private void eval(
			int index,
			Rule rule, 
			String outExpr, 
			Double[] inValues, 
			Map<String, Double[]> opValuesMap, 
			ReadWriteData resultData,
			TagGroup outTagGroup) throws Exception {

		// Process each hour of data - we'll only have one if 'in' is a monthly operand
		for (int hour = 0; hour < inValues.length; hour++) {
			// Replace variables
			String expr = outExpr.replace("${in}", inValues[hour].toString());
			
			for (String opName: rule.getOperands().keySet()) {			
				// Get the operand values from the proper data source
				InputOperand op = rule.getOperand(opName);
				Double opValue = opValuesMap.get(opName)[op.isMonthly() ? 0 : hour];
				expr = expr.replace("${" + opName + "}", opValue.toString());
			}
			try {
				Double value = new Evaluator().eval(expr);
				if (debug && hour == 0)
					logger.info("eval(" + index + "): " + outExpr + " = " + expr + " = " + value + ", " + outTagGroup);
				resultData.getData(hour).put(outTagGroup, value);
			}
			catch (Exception e) {
				logger.error("Error processing expression \"" + expr + "\", " + e.getMessage());
				throw e;
			}
			
			//if (hour == 0)
			//	logger.info("eval: " + outExpr + " = "  + expr + " = " + value + ", tg: " + outTagGroup);
		}
	}
	
	public int getCacheMisses() {
		return cacheMisses;
	}
	public int getCacheHits() {
		return cacheHits;
	}
}
