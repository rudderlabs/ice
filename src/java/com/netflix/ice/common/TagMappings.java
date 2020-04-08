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
package com.netflix.ice.common;

import java.util.List;
import java.util.Map;

/**
 * TagMappings defines a set of mapping rules that allow you to apply values to a
 * User Tag based on values of other tags. You can optionally specify what accounts
 * the rules apply to based on use of either <i>include</i> or <i>exclude</i> arrays.
 * The mappings may also specify a start date to indicate when the rules should take
 * effect.
 * 
 * Example yml config data for setting an Environment tag based on an Application tag
 * for account # 123456789012 starting on Feb. 1, 2020.
 *
 * <pre>
 * include: [123456789012]
 * start: 2020-02
 * maps:
 *   NonProd:
 *     Application: [webServerTest, webServerStage]
 *   Prod:
 *     Application: [webServerProd]
 * </pre>
 */
public class TagMappings {
	public Map<String, Map<String, List<String>>> maps;
	public List<String> include;
	public List<String> exclude;
	public String start;
	
	public Map<String, Map<String, List<String>>> getMaps() {
		return maps;
	}
	public void setMaps(Map<String, Map<String, List<String>>> maps) {
		this.maps = maps;
	}
	public List<String> getInclude() {
		return include;
	}
	public void setInclude(List<String> include) {
		this.include = include;
	}
	public List<String> getExclude() {
		return exclude;
	}
	public void setExclude(List<String> exclude) {
		this.exclude = exclude;
	}
	public String getStart() {
		return start;
	}
	public void setStart(String start) {
		this.start = start;
	}
}
	
