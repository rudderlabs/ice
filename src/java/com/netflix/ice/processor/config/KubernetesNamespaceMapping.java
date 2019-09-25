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
package com.netflix.ice.processor.config;

import java.util.List;

public class KubernetesNamespaceMapping {
	private String tag;
	private String value;
	private List<String> patterns;

	public KubernetesNamespaceMapping() {		
	}
	
	public KubernetesNamespaceMapping(String tag, String value, List<String> patterns) {
		this.tag = tag;
		this.value = value;
		this.patterns = patterns;
	}
	
	public String getTag() {
		return tag;
	}
	public void setTag(String tag) {
		this.tag = tag;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public List<String> getPatterns() {
		return patterns;
	}
	public void setPatterns(List<String> patterns) {
		this.patterns = patterns;
	}
}

