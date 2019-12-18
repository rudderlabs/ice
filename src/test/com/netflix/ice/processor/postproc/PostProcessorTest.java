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
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.CostAndUsageData;
import com.netflix.ice.processor.ReadWriteData;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Zone.BadZone;

public class PostProcessorTest {
    static private ProductService ps;
	static private AccountService as;
	static private String a1 = "0123456789012";
	
	@BeforeClass
	static public void init() {
		ps = new BasicProductService();
		as = new BasicAccountService();
	}
	
    class TagGroupSpec {
    	boolean isCost;
    	String region;
    	String operation;
    	String usageType;
    	Double value;
    	
    	public TagGroupSpec(boolean isCost, String region, String operation, String usageType, Double value) {
    		this.isCost = isCost;
    		this.region = region;
    		this.operation = operation;
    		this.usageType = usageType;
    		this.value = value;
    	}
    	
    	public TagGroup getTagGroup() throws BadZone {
    		return TagGroup.getTagGroup(a1, region, null, "Product", operation, usageType, null, null, as, ps);
    	}
    	
    	public TagGroup getTagGroup(String product) throws BadZone {
    		return TagGroup.getTagGroup(a1, region, null, product, operation, usageType, null, null, as, ps);
    	}
    }
    
	private void loadData(ReadWriteData usageData, ReadWriteData costData) throws BadZone {
        Map<TagGroup, Double> usages = usageData.getData(0);
        Map<TagGroup, Double> costs = costData.getData(0);
        
        TagGroupSpec[] dataSpecs = new TagGroupSpec[]{
        		new TagGroupSpec(false, "us-east-1", "OP1", "US-Requests-1", 1000.0),
        		new TagGroupSpec(false, "us-east-1", "OP1", "US-Requests-2", 2000.0),
        		new TagGroupSpec(false, "us-east-1", "OP1", "US-DataTransfer-Out-Bytes", 4000.0),
        		
        		new TagGroupSpec(false, "us-east-1", "OP2", "US-Requests-1", 8000.0),
        		new TagGroupSpec(false, "us-east-1", "OP2", "US-Requests-2", 16000.0),
        		new TagGroupSpec(false, "us-east-1", "OP2", "US-DataTransfer-Out-Bytes", 32000.0),
        		
        		new TagGroupSpec(false, "eu-west-1", "OP1", "EU-Requests-1", 10000.0),
        		new TagGroupSpec(false, "eu-west-1", "OP1", "EU-Requests-2", 20000.0),
        		new TagGroupSpec(false, "eu-west-1", "OP1", "EU-DataTransfer-Out-Bytes", 40000.0),
        };
        
        for (TagGroupSpec spec: dataSpecs) {
        	if (spec.isCost)
        		costs.put(spec.getTagGroup(), spec.value);
        	else
        		usages.put(spec.getTagGroup(), spec.value);
        }
	}
	
	private RuleConfig getConfig() throws JsonParseException, JsonMappingException, IOException {
		String yaml = "" +
		"name: ComputedCost\n" + 
		"start: 2019-11\n" + 
		"end: 2022-11\n" + 
		"operands:\n" + 
		"  out:\n" + 
		"    product: ComputedCost\n" + 
		"    usageType: ${group}-Requests\n" + 
		"  in:\n" + 
		"    type: usage\n" + 
		"    product: Product\n" + 
		"    usageType: (..)-Requests-[12].*\n" + 
		"  data:\n" + 
		"    type: usage\n" + 
		"    usageType: ${group}-DataTransfer-Out-Bytes\n" + 
		"cost: '(${in} - (${data} * 4 * 8 / 2)) * 0.01 / 1000'\n" + 
		"usage: '${in} - (${data} * 4 * 8 / 2)'\n";
		
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		RuleConfig rc = new RuleConfig();
		return mapper.readValue(yaml, rc.getClass());
	}
	
	@Test
	public void testGetInValue() throws Exception {
		ReadWriteData usageData = new ReadWriteData();
		ReadWriteData costData = new ReadWriteData();
		loadData(usageData, costData);
				
		PostProcessor pp = new PostProcessor(null, as, ps);
		Rule rule = new Rule(getConfig(), as, ps);
		
	    Map<TagGroup, Double> usageMap = usageData.getData(0);
	    Map<TagGroup, Double> costMap = costData.getData(0);
		Map<TagGroup, Double> inMap = pp.getInValues(rule, usageMap, costMap, true);
		
		assertEquals("Wrong number of matched tags", 3, inMap.size());
		// Scan map and make sure we have 2 US and 1 EU
		int us = 0;
		int eu = 0;
		for (TagGroup tg: inMap.keySet()) {
			if (tg.usageType.name.equals("US"))
				us++;
			else if (tg.usageType.name.equals("EU"))
				eu++;
		}
		assertEquals("Wrong number of US tagGroups", 2, us);
		assertEquals("Wrong number of EU tagGroups", 1, eu);
		
		TagGroupSpec[] specs = new TagGroupSpec[]{
				new TagGroupSpec(false, "us-east-1", "OP1", "US", 3000.0),
				new TagGroupSpec(false, "us-east-1", "OP2", "US", 24000.0),
				new TagGroupSpec(false, "eu-west-1", "OP1", "EU", 30000.0),
		};
		
		for (TagGroupSpec spec: specs) {
			TagGroup tg = spec.getTagGroup();
			assertEquals("Wrong aggregation for " + tg.operation + " " + tg.usageType, spec.value, inMap.get(tg), 0.001);
		}
	}
	
	@Test
	public void testProcessReadWriteData() throws Exception {
		
		ReadWriteData usageData = new ReadWriteData();
		ReadWriteData costData = new ReadWriteData();
		loadData(usageData, costData);
		CostAndUsageData data = new CostAndUsageData(0, null, null, null, as, ps);
        data.putUsage(null, usageData);
        data.putCost(null, costData);

		
		PostProcessor pp = new PostProcessor(null, as, ps);
		Rule rule = new Rule(getConfig(), as, ps);
		pp.processReadWriteData(rule, data, true);
		
		// cost: 'in - (data * 4 * 8 / 2) * 0.01 / 1000'
		// 'in' is the sum of the two request values
		//
		// US: ((1000 + 2000) - (4000 * 4 * 8 / 2)) * 0.01 / 1000 == (3000 - 64000) * 0.00001 == 2999.36
		TagGroup usReqs = new TagGroupSpec(true, "us-east-1", "OP1", "US-Requests", null).getTagGroup("ComputedCost");
		Double value = costData.getData(0).get(usReqs);
		assertNotNull("No cost value for US-Requests", value);
		assertEquals("Wrong cost value for US-Requests", -0.61, value, .0001);
		
		value = usageData.getData(0).get(usReqs);
		assertNotNull("No usage value for US-Requests", value);
		assertEquals("Wrong usage value for US-Requests", -61000.0, value, .0001);
		
		// EU:  ((10000 + 20000) - (40000 * 4 * 8 / 2)) * 0.01 / 1000 == (30000 - 640000) * 0.00001 == 29993.6
		TagGroup euReqs = new TagGroupSpec(true, "eu-west-1", "OP1", "EU-Requests", null).getTagGroup("ComputedCost");
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
		loadData(usageData, costData);
		Product product = ps.getProductByServiceCode("Product");
		CostAndUsageData data = new CostAndUsageData(0, null, null, null, as, ps);
        data.putUsage(product, usageData);
        data.putCost(product, costData);

		
		PostProcessor pp = new PostProcessor(null, as, ps);
		Rule rule = new Rule(getConfig(), as, ps);
		pp.processReadWriteData(rule, data, false);
		
		Product outProduct = ps.getProductByServiceCode("ComputedCost");
		ReadWriteData outCostData = data.getCost(outProduct);
		
		// out: 'in - (data * 4 * 8 / 2) * 0.01 / 1000'
		// 'in' is the sum of the two request values
		//
		// US: (1000 + 2000) - (4000 * 4 * 8 / 2) * 0.01 / 1000 == 3000 - 64000 * 0.00001 == 2999.36
		TagGroup usReqs = new TagGroupSpec(true, "us-east-1", "OP1", "US-Requests", null).getTagGroup("ComputedCost");
		Double value = outCostData.getData(0).get(usReqs);
		assertNotNull("No value for US-Requests", value);
		assertEquals("Wrong value for US-Requests", -0.61, value, .0001);
		
		// EU:  (10000 + 20000) - (40000 * 4 * 8 / 2) * 0.01 / 1000 == 30000 - 640000 * 0.00001 == 29993.6
		TagGroup euReqs = new TagGroupSpec(true, "eu-west-1", "OP1", "EU-Requests", null).getTagGroup("ComputedCost");
		Double euValue = outCostData.getData(0).get(euReqs);
		assertNotNull("No value for EU-Requests", euValue);
		assertEquals("Wrong value for EU-Requests", -6.1, euValue, .0001);
	}

}
