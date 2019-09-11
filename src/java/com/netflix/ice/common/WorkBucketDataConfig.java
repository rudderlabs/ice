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

import com.google.gson.Gson;
import com.netflix.ice.common.Config.TagCoverage;
import com.netflix.ice.tag.Account;

/**
 * Work bucket data-dependent configuration items used by the reader
 */
public class WorkBucketDataConfig {
	private final String startMonth;
	private final List<String> userTags;
    private final boolean familyRiBreakout;
    private final List<Account> accounts;
    private final Map<String, List<String>> zones;
    private final TagCoverage tagCoverage;
	
	public WorkBucketDataConfig(String startMonth, List<Account> accounts, Map<String, List<String>> zones, List<String> userTags, boolean familyRiBreakout, TagCoverage tagCoverage) {
		this.startMonth = startMonth;
		this.accounts = accounts;
		this.zones = zones;
		this.userTags = userTags;
		this.familyRiBreakout = familyRiBreakout;
		this.tagCoverage = tagCoverage;
	}
	
	public WorkBucketDataConfig(String json) {
		Gson gson = new Gson();
		WorkBucketDataConfig c = gson.fromJson(json, this.getClass());
		this.startMonth = c.startMonth;
		this.accounts = c.accounts;
		this.zones = c.zones;
		this.userTags = c.userTags;
		this.familyRiBreakout = c.familyRiBreakout;
		this.tagCoverage = c.tagCoverage;
	}

	public String getStartMonth() {
		return startMonth;
	}

	public List<Account> getAccounts() {
		return accounts;
	}
	
	public Map<String, List<String>> getZones() {
		return zones;
	}

	public List<String> getUserTags() {
		return userTags;
	}

	public boolean getFamilyRiBreakout() {
		return familyRiBreakout;
	}
	
	public TagCoverage getTagCoverage() {
		return tagCoverage;
	}
	
	public String toJSON() {
		Gson gson = new Gson();
    	return gson.toJson(this);
	}
}
