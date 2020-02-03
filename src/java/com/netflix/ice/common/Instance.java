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

import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.Zone;
import com.netflix.ice.tag.Zone.BadZone;

public class Instance {
    protected static Logger logger = LoggerFactory.getLogger(Instance.class);
    
    public static final String tagSeparator = "<|>";
    public static final String tagSeparatorRegex = "<\\|>";
    public static final String tagSeparatorReplacement = "<~>";

    public final String id;
	public final String type;
	public final Account account;
	public final Region region;
	public final Zone zone;
	public final Product product;
	public final Map<String, String> tags;
	
	public final long startMillis; // start time of billing lineitem record. Not serialized.
	
	public Instance(String id, String type, Account account, Region region, Zone zone, Product product, Map<String, String> tags, long startMillis) {
		this.id = id;
		this.type = type;
		this.account = account;
		this.region = region;
		this.zone = zone;
		this.product = product;
		this.tags = Maps.newHashMap();
		for (String k: tags.keySet()) {
			String v = tags.get(k);
			if (v.contains(tagSeparator)) {
				logger.warn("Tag " + k + "=" + v + " has a value with the tagSeparator " + tagSeparator + ". Replacing with " + tagSeparatorReplacement);
				v = v.replace(tagSeparator, tagSeparatorReplacement);
			}
			this.tags.put(k, v);
		}
		this.startMillis = startMillis;
	}
	
	public Instance(String[] values, AccountService accountService, ProductService productService) throws BadZone {
        this.id = values[0];
        this.type = values[1];
        this.account = accountService.getAccountById(values[2]);
        this.region = Region.getRegionByName(values[4]);
        this.zone = (values.length > 5 && !values[5].isEmpty()) ? this.region.getZone(values[5]) : null;
        this.product = productService.getProductByServiceCode(values[6]);

        final int TAGS_INDEX = 7;
        Map<String, String> tags = Maps.newHashMap();
        if (values.length > TAGS_INDEX) {
	        if (values.length > TAGS_INDEX + 1) {
	            StringBuilder tagsStr = new StringBuilder();
	            for (int i = TAGS_INDEX; i < values.length; i++)
	            	tagsStr.append(values[i] + ",");
	            // remove last comma
	            tagsStr.deleteCharAt(tagsStr.length() - 1);
	        	values[TAGS_INDEX] = tagsStr.toString();
	        }
	        // Remove quotes from tags if present
	        if (values[TAGS_INDEX].startsWith("\""))
	        	values[TAGS_INDEX] = values[TAGS_INDEX].substring(1, values[TAGS_INDEX].length() - 1);
	        
	        tags = parseResourceTags(values[TAGS_INDEX]);
        }
        this.tags = tags;
        this.startMillis = 0;
	}
	
	public static String[] header() {
		return new String[] {"InstanceID", "InstanceType", "AccountId", "AccountName", "Region", "Zone", "Product", "Tags"};
	}
	
	public String[] values() {
		return new String[]{
			id,
			type,
			account.getId(),
			account.getIceName(),
			region.toString(),
			zone == null ? "" : zone.toString(),
			product.getServiceCode(),
			resourceTagsToString(tags),
		};
	}
	
	private String resourceTagsToString(Map<String, String> tags) {
    	StringBuilder sb = new StringBuilder();
    	boolean first = true;
    	for (Entry<String, String> entry: tags.entrySet()) {
    		String tag = entry.getKey();
    		if (tag.startsWith("user:"))
    			tag = tag.substring("user:".length());
    		sb.append((first ? "" : tagSeparator) + tag + "=" + entry.getValue());
    		first = false;
    	}
    	String ret = sb.toString();
    	return ret;
	}
	
	private static Map<String, String> parseResourceTags(String in) {
		Map<String, String> tags = Maps.newHashMap();
		String[] pairs = in.split(tagSeparatorRegex);
		if (pairs[0].isEmpty())
			return tags;
		
		for (String tag: pairs) {
			// split on first "="
			int i = tag.indexOf("=");
			if (i <= 0) {
				logger.error("Bad tag: " + tag);
				continue;
			}
			String key = tag.substring(0, i);
			tags.put(key, tag.substring(i + 1));
		}
		return tags;
	}
}
