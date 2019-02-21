package com.netflix.ice.common;

import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
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
	
	public WorkBucketDataConfig(String startMonth, List<Account> accounts, Map<String, List<String>> zones, List<String> userTags, boolean familyRiBreakout) {
		this.startMonth = startMonth;
		this.accounts = accounts;
		this.zones = zones;
		this.userTags = userTags;
		this.familyRiBreakout = familyRiBreakout;
	}
	
	public WorkBucketDataConfig(String json) {
		Gson gson = new Gson();
		WorkBucketDataConfig c = gson.fromJson(json, this.getClass());
		this.startMonth = c.startMonth;
		this.accounts = c.accounts;
		this.zones = c.zones;
		this.userTags = c.userTags;
		this.familyRiBreakout = c.familyRiBreakout;
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
	
	public String toJSON() {
		Gson gson = new Gson();
    	return gson.toJson(this);
	}
}
