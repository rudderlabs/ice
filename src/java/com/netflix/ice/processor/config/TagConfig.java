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
