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
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;

public class Rule {
    protected Logger logger = LoggerFactory.getLogger(getClass());

	private RuleConfig config;
	private Map<String, InputOperand> operands;
	private InputOperand in;
	private List<ResultOperand> results;
	
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
		if (config.getOperands() != null) {
			for (String oc: config.getOperands().keySet()) {
				operands.put(oc, new InputOperand(config.getOperand(oc), accountService));
			}
		}
		
		in = new InputOperand(config.getIn(), accountService);
		results = Lists.newArrayList();
		for (ResultConfig rc: config.getResults()) {
			results.add(new ResultOperand(rc.getResult(), accountService));
		}
	}
	
	public InputOperand getOperand(String name) {
		return operands.get(name);
	}
	
	public Map<String, InputOperand> getOperands() {
		return operands;
	}
	
	public InputOperand getIn() {
		return in;
	}
	
	public List<ResultOperand> getResults() {
		return results;
	}
	
	public ResultOperand getResult(int index) {
		return results.get(index);
	}
	
	public String getResultValue(int index) {
		return config.getResults().get(index).getValue();
	}
		
}

