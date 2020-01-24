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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.AggregationTagGroup;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.CostAndUsageData;
import com.netflix.ice.processor.ReadWriteData;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.Zone.BadZone;

public class PostProcessorTest {
    static private ProductService ps;
	static private AccountService as;
	static private String a1 = "0123456789012";
	static private String a2 = "1234567890123";
	static private String a3 = "2345678901234";
	static private String productStr = "ProductForPPT";
	
	@BeforeClass
	static public void init() {
		ps = new BasicProductService();
		as = new BasicAccountService();
	}
	
	public enum DataType {
		cost,
		usage;
	}
    class TagGroupSpec {
    	DataType dataType;
    	String account;
    	String region;
    	String product;
    	String operation;
    	String usageType;
    	Double value;
    	
    	public TagGroupSpec(DataType dataType, String account, String region, String product, String operation, String usageType, Double value) {
    		this.dataType = dataType;
    		this.account = account;
    		this.region = region;
    		this.product = product;
    		this.operation = operation;
    		this.usageType = usageType;
    		this.value = value;
    	}

    	public TagGroupSpec(DataType dataType, String account, String region, String operation, String usageType, Double value) {
    		this.dataType = dataType;
    		this.account = account;
    		this.region = region;
    		this.product = productStr;
    		this.operation = operation;
    		this.usageType = usageType;
    		this.value = value;
    	}

    	public TagGroup getTagGroup() throws BadZone {
    		return TagGroup.getTagGroup(account, region, null, product, operation, usageType, null, null, as, ps);
    	}
    	
    	public TagGroup getTagGroup(String account) throws BadZone {
    		return TagGroup.getTagGroup(account, region, null, product, operation, usageType, null, null, as, ps);
    	}
    }
    
    private void loadData(TagGroupSpec[] dataSpecs, ReadWriteData usageData, ReadWriteData costData) throws BadZone {
        Map<TagGroup, Double> usages = usageData.getData(0);
        Map<TagGroup, Double> costs = costData.getData(0);
    	
        for (TagGroupSpec spec: dataSpecs) {
        	if (spec.dataType == DataType.cost)
        		costs.put(spec.getTagGroup(), spec.value);
        	else
        		usages.put(spec.getTagGroup(), spec.value);
        }
    }
    
	private void loadComputedCostData(ReadWriteData usageData, ReadWriteData costData) throws BadZone {
        TagGroupSpec[] dataSpecs = new TagGroupSpec[]{
        		new TagGroupSpec(DataType.usage, a1, "us-east-1", "OP1", "US-Requests-1", 1000.0),
        		new TagGroupSpec(DataType.usage, a1, "us-east-1", "OP1", "US-Requests-2", 2000.0),
        		new TagGroupSpec(DataType.usage, a1, "us-east-1", "OP1", "US-DataTransfer-Out-Bytes", 4000.0),
        		
        		new TagGroupSpec(DataType.usage, a1, "us-east-1", "OP2", "US-Requests-1", 8000.0),
        		new TagGroupSpec(DataType.usage, a1, "us-east-1", "OP2", "US-Requests-2", 16000.0),
        		new TagGroupSpec(DataType.usage, a1, "us-east-1", "OP2", "US-DataTransfer-Out-Bytes", 32000.0),
        		
        		new TagGroupSpec(DataType.usage, a1, "eu-west-1", "OP1", "EU-Requests-1", 10000.0),
        		new TagGroupSpec(DataType.usage, a1, "eu-west-1", "OP1", "EU-Requests-2", 20000.0),
        		new TagGroupSpec(DataType.usage, a1, "eu-west-1", "OP1", "EU-DataTransfer-Out-Bytes", 40000.0),
        };
        loadData(dataSpecs, usageData, costData);
	}
	
	private String computedCostYaml = "" +
			"name: ComputedCost\n" + 
			"start: 2019-11\n" + 
			"end: 2022-11\n" + 
			"operands:\n" + 
			"  data:\n" + 
			"    type: usage\n" + 
			"    usageType: ${group}-DataTransfer-Out-Bytes\n" + 
			"in:\n" + 
			"  type: usage\n" + 
			"  product: " + productStr + "\n" + 
			"  usageType: (..)-Requests-[12].*\n" + 
			"results:\n" + 
			"  - result:\n" + 
			"      type: cost\n" + 
			"      product: ComputedCost\n" + 
			"      usageType: ${group}-Requests\n" + 
			"    value: '(${in} - (${data} * 4 * 8 / 2)) * 0.01 / 1000'\n" + 
			"  - result:\n" + 
			"      type: usage\n" + 
			"      product: ComputedCost\n" + 
			"      usageType: ${group}-Requests\n" + 
			"    value: '${in} - (${data} * 4 * 8 / 2)'\n";

	
	private RuleConfig getConfig(String yaml) throws JsonParseException, JsonMappingException, IOException {		
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		RuleConfig rc = new RuleConfig();
		return mapper.readValue(yaml, rc.getClass());
	}
	
	@Test
	public void testGetInValue() throws Exception {
		ReadWriteData usageData = new ReadWriteData();
		ReadWriteData costData = new ReadWriteData();
		loadComputedCostData(usageData, costData);
				
		PostProcessor pp = new PostProcessor(null, as, ps);
		Rule rule = new Rule(getConfig(computedCostYaml), as, ps);
		
	    Map<TagGroup, Double> usageMap = usageData.getData(0);
	    Map<TagGroup, Double> costMap = costData.getData(0);
		Map<AggregationTagGroup, Double> inMap = pp.getInValues(rule, usageMap, costMap, true);
		
		assertEquals("Wrong number of matched tags", 3, inMap.size());
		// Scan map and make sure we have 2 US and 1 EU
		int us = 0;
		int eu = 0;
		for (AggregationTagGroup atg: inMap.keySet()) {
			if (atg.getUsageType().name.equals("US"))
				us++;
			else if (atg.getUsageType().name.equals("EU"))
				eu++;
		}
		assertEquals("Wrong number of US tagGroups", 2, us);
		assertEquals("Wrong number of EU tagGroups", 1, eu);
		
		TagGroupSpec[] specs = new TagGroupSpec[]{
				new TagGroupSpec(DataType.usage, a1, "us-east-1", "OP1", "US", 3000.0),
				new TagGroupSpec(DataType.usage, a1, "us-east-1", "OP2", "US", 24000.0),
				new TagGroupSpec(DataType.usage, a1, "eu-west-1", "OP1", "EU", 30000.0),
		};
		
		for (TagGroupSpec spec: specs) {
			TagGroup tg = spec.getTagGroup(a1);
			AggregationTagGroup atg = rule.getIn().aggregation.getAggregationTagGroup(tg);
			assertEquals("Wrong aggregation for " + tg.operation + " " + tg.usageType, spec.value, inMap.get(atg), 0.001);
		}
	}
	
	@Test
	public void testProcessReadWriteData() throws Exception {
		
		ReadWriteData usageData = new ReadWriteData();
		ReadWriteData costData = new ReadWriteData();
		loadComputedCostData(usageData, costData);
		CostAndUsageData data = new CostAndUsageData(0, null, null, null, as, ps);
        data.putUsage(null, usageData);
        data.putCost(null, costData);

		
		PostProcessor pp = new PostProcessor(null, as, ps);
		Rule rule = new Rule(getConfig(computedCostYaml), as, ps);
		pp.processReadWriteData(rule, data, true);
		
		// cost: 'in - (data * 4 * 8 / 2) * 0.01 / 1000'
		// 'in' is the sum of the two request values
		//
		// US: ((1000 + 2000) - (4000 * 4 * 8 / 2)) * 0.01 / 1000 == (3000 - 64000) * 0.00001 == 2999.36
		TagGroup usReqs = new TagGroupSpec(DataType.cost, a1, "us-east-1", "ComputedCost", "OP1", "US-Requests", null).getTagGroup();
		Double value = costData.getData(0).get(usReqs);
		assertNotNull("No cost value for US-Requests", value);
		assertEquals("Wrong cost value for US-Requests", -0.61, value, .0001);
		
		value = usageData.getData(0).get(usReqs);
		assertNotNull("No usage value for US-Requests", value);
		assertEquals("Wrong usage value for US-Requests", -61000.0, value, .0001);
		
		// EU:  ((10000 + 20000) - (40000 * 4 * 8 / 2)) * 0.01 / 1000 == (30000 - 640000) * 0.00001 == 29993.6
		TagGroup euReqs = new TagGroupSpec(DataType.cost, a1, "eu-west-1", "ComputedCost", "OP1", "EU-Requests", null).getTagGroup();
		Double euValue = costData.getData(0).get(euReqs);
		assertNotNull("No cost value for EU-Requests", euValue);
		assertEquals("Wrong cost value for EU-Requests", -6.1, euValue, .0001);
		
		euValue = usageData.getData(0).get(euReqs);
		assertNotNull("No usage value for EU-Requests", euValue);
		assertEquals("Wrong usage value for EU-Requests", -610000.0, euValue, .0001);
	}
	
	@Test
	public void testProcessReadWriteDataWithResources() throws Exception {
		
		ReadWriteData usageData = new ReadWriteData();
		ReadWriteData costData = new ReadWriteData();
		loadComputedCostData(usageData, costData);
		Product product = ps.getProductByServiceCode(productStr);
		CostAndUsageData data = new CostAndUsageData(0, null, null, null, as, ps);
        data.putUsage(product, usageData);
        data.putCost(product, costData);

		
		PostProcessor pp = new PostProcessor(null, as, ps);
		Rule rule = new Rule(getConfig(computedCostYaml), as, ps);
		pp.processReadWriteData(rule, data, false);
		
		Product outProduct = ps.getProductByServiceCode("ComputedCost");
		ReadWriteData outCostData = data.getCost(outProduct);
		
		// out: 'in - (data * 4 * 8 / 2) * 0.01 / 1000'
		// 'in' is the sum of the two request values
		//
		// US: (1000 + 2000) - (4000 * 4 * 8 / 2) * 0.01 / 1000 == 3000 - 64000 * 0.00001 == 2999.36
		TagGroup usReqs = new TagGroupSpec(DataType.cost, a1, "us-east-1", "ComputedCost", "OP1", "US-Requests", null).getTagGroup();
		Double value = outCostData.getData(0).get(usReqs);
		assertNotNull("No value for US-Requests", value);
		assertEquals("Wrong value for US-Requests", -0.61, value, .0001);
		
		// EU:  (10000 + 20000) - (40000 * 4 * 8 / 2) * 0.01 / 1000 == 30000 - 640000 * 0.00001 == 29993.6
		TagGroup euReqs = new TagGroupSpec(DataType.cost, a1, "eu-west-1", "ComputedCost", "OP1", "EU-Requests", null).getTagGroup();
		Double euValue = outCostData.getData(0).get(euReqs);
		assertNotNull("No value for EU-Requests", euValue);
		assertEquals("Wrong value for EU-Requests", -6.1, euValue, .0001);
	}

	// Config to add a surcharge of 3% to all costs split out by account, region, and zone
	// usage is the aggregated cost and cost is the 3% charge
	private String surchargeConfigYaml = "" +
		"name: ComputedCost\n" + 
		"start: 2019-11\n" + 
		"end: 2022-11\n" + 
		"in:\n" + 
		"  type: cost\n" +
		"  aggregate: [Product, Operation, UsageType]\n" + 
		"results:\n" + 
		"  - result:\n" + 
		"      type: cost\n" + 
		"      product: ComputedCost\n" + 
		"      operation: \n" + 
		"      usageType: Dollar\n" + 
		"    value: '${in} * 0.03'\n" + 
		"  - result:\n" + 
		"      type: usage\n" + 
		"      product: ComputedCost\n" + 
		"      operation: \n" + 
		"      usageType: Dollar\n" + 
		"    value: '${in}'\n";
	
	private void loadSurchargeData(ReadWriteData usageData, ReadWriteData costData) throws BadZone {
        TagGroupSpec[] dataSpecs = new TagGroupSpec[]{
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", "OP1", "US-Requests-1", 1000.0),
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", "OP2", "US-Requests-2", 2000.0),
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", "OP3", "US-DataTransfer-Out-Bytes", 4000.0),
        		
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", "OP4", "US-Requests-1", 8000.0),
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", "OP5", "US-Requests-2", 16000.0),
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", "OP6", "US-DataTransfer-Out-Bytes", 32000.0),
        		
        		new TagGroupSpec(DataType.cost, a1, "eu-west-1", "OP7", "EU-Requests-1", 10000.0),
        		new TagGroupSpec(DataType.cost, a1, "eu-west-1", "OP8", "EU-Requests-2", 20000.0),
        		new TagGroupSpec(DataType.cost, a1, "eu-west-1", "OP9", "EU-DataTransfer-Out-Bytes", 40000.0),
        		
        		new TagGroupSpec(DataType.cost, a2, "us-east-1", "OP1", "US-Requests-1", 1000.0),
        		new TagGroupSpec(DataType.cost, a2, "us-east-1", "OP2", "US-Requests-2", 2000.0),
        		new TagGroupSpec(DataType.cost, a2, "us-east-1", "OP3", "US-DataTransfer-Out-Bytes", 4000.0),
        		
        		new TagGroupSpec(DataType.cost, a2, "us-east-1", "OP4", "US-Requests-1", 8000.0),
        		new TagGroupSpec(DataType.cost, a2, "us-east-1", "OP5", "US-Requests-2", 16000.0),
        		new TagGroupSpec(DataType.cost, a2, "us-east-1", "OP6", "US-DataTransfer-Out-Bytes", 32000.0),
        		
        		new TagGroupSpec(DataType.cost, a2, "eu-west-1", "OP7", "EU-Requests-1", 10000.0),
        		new TagGroupSpec(DataType.cost, a2, "eu-west-1", "OP8", "EU-Requests-2", 20000.0),
        		new TagGroupSpec(DataType.cost, a2, "eu-west-1", "OP9", "EU-DataTransfer-Out-Bytes", 40000.0),
        };
        
        loadData(dataSpecs, usageData, costData);
	}
	
	@Test
	public void testSurchargeGetInValues() throws Exception {
		ReadWriteData usageData = new ReadWriteData();
		ReadWriteData costData = new ReadWriteData();
		loadSurchargeData(usageData, costData);
				
		PostProcessor pp = new PostProcessor(null, as, ps);
		Rule rule = new Rule(getConfig(surchargeConfigYaml), as, ps);
		
	    Map<TagGroup, Double> usageMap = usageData.getData(0);
	    Map<TagGroup, Double> costMap = costData.getData(0);
		Map<AggregationTagGroup, Double> inMap = pp.getInValues(rule, usageMap, costMap, true);
		
		assertEquals("Wrong number of matched tags", 4, inMap.size());
		// Scan map and make sure we have 2 us-east-1 and 2 eu-west-1
		int us = 0;
		int eu = 0;
		for (AggregationTagGroup atg: inMap.keySet()) {
			Region r = atg.getRegion();
			if (r == Region.US_EAST_1)
				us++;
			else if (r == Region.EU_WEST_1)
				eu++;
		}
		assertEquals("Wrong number of US tagGroups", 2, us);
		assertEquals("Wrong number of EU tagGroups", 2, eu);
		
		TagGroupSpec[] specs = new TagGroupSpec[]{
				new TagGroupSpec(DataType.usage, a1, "us-east-1", "", "", 63000.0),
				new TagGroupSpec(DataType.usage, a1, "eu-west-1", "", "", 70000.0),
		};
		
		for (TagGroupSpec spec: specs) {
			TagGroup tg = spec.getTagGroup(a1);
			AggregationTagGroup atg = rule.getIn().aggregation.getAggregationTagGroup(tg);
			assertEquals("Wrong aggregation for " + tg.operation + " " + tg.usageType, spec.value, inMap.get(atg), 0.001);
			tg = spec.getTagGroup(a2);
			assertEquals("Wrong aggregation for " + tg.operation + " " + tg.usageType, spec.value, inMap.get(atg), 0.001);
		}
	}

	private String splitCostYaml = "" +
			"name: SplitCost\n" + 
			"start: 2019-11\n" + 
			"end: 2022-11\n" + 
			"operands:\n" + 
			"  lump-usage:\n" + 
			"    type: usage\n" +
			"    accounts: [" + a1 + "]\n" +
			"    regions: [global]\n" +
			"    product: GlobalFee\n" + 
			"    operation: None\n" +
			"    usageType: Dollar\n" + 
			"  lump-cost:\n" +
			"    type: cost\n" + 
			"    accounts: [" + a1 + "]\n" +
			"    regions: [global]\n" +
			"    product: GlobalFee\n" + 
			"    operation: None\n" +
			"    usageType: Dollar\n" + 
			"in:\n" + 
			"  type: cost\n" + 
			"  product: '(?!GlobalFee$)^.*$'\n" +
			"  aggregate: [Zone,Product,Operation,UsageType,ResourceGroup]\n" +
			"results:\n" + 
			"  - result:\n" + 
			"      type: cost\n" + 
			"      account: '${group}'\n" + 
			"      region: '${group}'\n" +
			"      product: GlobalFee\n" +
			"      operation: Split\n" +
			"      usageType: Dollar\n" + 
			"    value: '${lump-cost} * ${in} / ${lump-usage}'\n" + 
			"  - result:\n" + 
			"      type: cost\n" + 
			"      account: " + a1 + "\n" +
			"      region: global\n" +
			"      product: GlobalFee\n" + 
			"      operation: None\n" +
			"      usageType: Dollar\n" + 
			"    value: 0.0\n";

	@Test
	public void testGlobalSplit() throws Exception {
		// Split $300 (3% of $10,000) of spend across three accounts based on individual account spend
        TagGroupSpec[] dataSpecs = new TagGroupSpec[]{
        		new TagGroupSpec(DataType.cost, a1, "global", "GlobalFee", "None", "Dollar", 300.0),
        		new TagGroupSpec(DataType.usage, a1, "global", "GlobalFee", "None", "Dollar", 10000.0),
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", "None", "Dollar", 5000.0),
        		new TagGroupSpec(DataType.cost, a2, "us-east-1", "None", "Dollar", 3000.0),
        		new TagGroupSpec(DataType.cost, a3, "us-east-1", "None", "Dollar", 1500.0),
        		new TagGroupSpec(DataType.cost, a3, "us-west-2", "None", "Dollar", 500.0),
        };
        
		ReadWriteData usageData = new ReadWriteData();
		ReadWriteData costData = new ReadWriteData();
		loadData(dataSpecs, usageData, costData);
		CostAndUsageData data = new CostAndUsageData(0, null, null, null, as, ps);
        data.putUsage(null, usageData);
        data.putCost(null, costData);
		
		PostProcessor pp = new PostProcessor(null, as, ps);
		Rule rule = new Rule(getConfig(splitCostYaml), as, ps);
		pp.processReadWriteData(rule, data, true);

		assertEquals("wrong number of operands in the cache", 2, pp.getOperandValueCache().size());
		ReadWriteData outCostData = data.getCost(null);
		
		// Should have zero-ed out the GlobalFee cost
		TagGroup globalFee = new TagGroupSpec(DataType.cost, a1, "global", "GlobalFee", "None", "Dollar", null).getTagGroup();
		Double value = outCostData.getData(0).get(globalFee);
		assertNotNull("No value for global fee", value);
		assertEquals("Wrong value for global fee", 0.0, value, .001);
		
		// Should have 50/30/15/5% split of $300
		TagGroup a1split = new TagGroupSpec(DataType.cost, a1, "us-east-1", "GlobalFee", "Split", "Dollar", null).getTagGroup();
		value = outCostData.getData(0).get(a1split);
		assertEquals("wrong value for account 1", 300.0 * 0.5, value, .001);
		
		TagGroup a2split = new TagGroupSpec(DataType.cost, a2, "us-east-1", "GlobalFee", "Split", "Dollar", null).getTagGroup();
		value = outCostData.getData(0).get(a2split);
		assertEquals("wrong value for account 2", 300.0 * 0.3, value, .001);
		
		TagGroup a3split = new TagGroupSpec(DataType.cost, a3, "us-east-1", "GlobalFee", "Split", "Dollar", null).getTagGroup();
		value = outCostData.getData(0).get(a3split);
		assertEquals("wrong value for account 3", 300.0 * 0.15, value, .001);
		a3split = new TagGroupSpec(DataType.cost, a3, "us-west-2", "GlobalFee", "Split", "Dollar", null).getTagGroup();
		value = outCostData.getData(0).get(a3split);
		assertEquals("wrong value for account 3", 300.0 * 0.05, value, .001);
	}
}
