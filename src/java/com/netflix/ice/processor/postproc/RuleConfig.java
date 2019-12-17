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

import java.util.Map;

public class RuleConfig {
	private String name;
	private String start;
	private String end;
	private Map<String, OperandConfig> operandConfigs;
	private String out;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Map<String, OperandConfig> getOperands() {
		return operandConfigs;
	}
	public void setOperands(Map<String, OperandConfig> operandConfigs) {
		this.operandConfigs = operandConfigs;
	}
	public String getOut() {
		return out;
	}
	public void setOut(String out) {
		this.out = out;
	}
	
	public OperandConfig getInOperand() {
		return operandConfigs.get("in");
	}
	
	public OperandConfig getOutOperand() {
		return operandConfigs.get("out");
	}
	
	public OperandConfig getOperand(String name) {
		return operandConfigs.get(name);
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
}
