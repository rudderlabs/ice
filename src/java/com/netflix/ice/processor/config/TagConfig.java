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
import java.util.Map;

/*
 * TagConfig holds the name of a user tag and an optional set of aliases it may go by.
 * It also includes a list of values the tag may take along with a set of aliases that each value may take.
 */
public class TagConfig {
	public String name;
	public List<String> aliases;
	public Map<String, List<String>> values;
	public Map<String, Map<String, List<String>>> mapped;
		
	public TagConfig() {}
	
	public TagConfig(String name, List<String> aliases, Map<String, List<String>> values) {
		this.name = name;
		this.aliases = aliases;
		this.values = values;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<String> getAliases() {
		return aliases;
	}

	public void setAliases(List<String> aliases) {
		this.aliases = aliases;
	}

	public Map<String, List<String>> getValues() {
		return values;
	}

	public void setValues(Map<String, List<String>> values) {
		this.values = values;
	}


}
