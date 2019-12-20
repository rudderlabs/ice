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

public class RuleConfig {
	private String name;
	private String start;
	private String end;
	private Map<String, OperandConfig> operands;
	private OperandConfig in;
	private List<ResultConfig> results;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getStart() {
		return start;
	}
	public void setStart(String start) {
		this.start = start;
	}
	public String getEnd() {
		return end;
	}
	public void setEnd(String end) {
		this.end = end;
	}
	public Map<String, OperandConfig> getOperands() {
		return operands;
	}
	public void setOperands(Map<String, OperandConfig> operands) {
		this.operands = operands;
	}
	public OperandConfig getOperand(String name) {
		return this.operands.get(name);
	}
	public OperandConfig getIn() {
		return in;
	}
	public void setIn(OperandConfig in) {
		this.in = in;
	}
	
	public List<ResultConfig> getResults() {
		return results;
	}
	public void setResults(List<ResultConfig> results) {
		this.results = results;
	}
}
