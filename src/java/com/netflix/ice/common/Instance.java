package com.netflix.ice.common;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.Zone;

public class Instance {
    protected static Logger logger = LoggerFactory.getLogger(Instance.class);

    public final String id;
	public final String type;
	public final Account account;
	public final Region region;
	public final Zone zone;
	public final Map<String, String> tags;
	
	public Instance(String id, String type, Account account, Region region, Zone zone, Map<String, String> tags) {
		this.id = id;
		this.type = type;
		this.account = account;
		this.region = region;
		this.zone = zone;
		this.tags = tags;
	}
	
	public static String header() {
		return "InstanceID,InstanceType,AccountId,AccountName,Region,Zone,Tags\n";
	}
	
	public String serialize() {
		String[] cols = new String[]{
			id,
			type,
			account.id,
			account.name,
			region.toString(),
			zone == null ? "" : zone.toString(),
			resourceTagsToString(tags),
		};
		return StringUtils.join(cols, ",") + "\n";
	}
	
	private String resourceTagsToString(Map<String, String> tags) {
    	StringBuilder sb = new StringBuilder();
    	boolean first = true;
    	for (Entry<String, String> entry: tags.entrySet()) {
    		String tag = entry.getKey();
    		if (tag.startsWith("user:"))
    			tag = tag.substring("user:".length());
    		sb.append((first ? "" : "|") + tag + "=" + entry.getValue());
    		first = false;
    	}
    	String ret = sb.toString();
    	if (ret.contains(","))
    		ret = "\"" + ret + "\"";
    	return ret;
	}
	
	public static Instance deserialize(String in, AccountService accountService) {
		// remove the newline before splitting
        String[] values = in.trim().split(",");
        
        String id = values[0];
        String type = values[1];
        Account account = accountService.getAccountById(values[2]);
        Region region = Region.getRegionByName(values[4]);
        Zone zone = (values.length > 5 && !values[5].isEmpty()) ? Zone.getZone(values[5]) : null;

        Map<String, String> tags = Maps.newHashMap();
        if (values.length > 6) {
	        if (values.length > 7) {
	            StringBuilder tagsStr = new StringBuilder();
	            for (int i = 6; i < values.length; i++)
	            	tagsStr.append(values[i] + ",");
	            // remove last comma
	            tagsStr.deleteCharAt(tagsStr.length() - 1);
	        	values[6] = tagsStr.toString();
	        }
	        // Remove quotes from tags if present
	        if (values[6].startsWith("\""))
	        	values[6] = values[6].substring(1, values[6].length() - 1);
	        
	        tags = parseResourceTags(values[6]);
        }
        
    	return new Instance(id, type, account, region, zone, tags);
	}
	

	private static Map<String, String> parseResourceTags(String in) {
		Map<String, String> tags = Maps.newHashMap();
		String[] pairs = in.split("\\|");
		if (pairs[0].isEmpty())
			return tags;
		
		for (String tag: pairs) {
			// split on first "="
			int i = tag.indexOf("=");
			if (i <= 0) {
				logger.error("Bad tag: " + tag);
			}
			String key = tag.substring(0, i);
			tags.put(key, tag.substring(i + 1));
		}
		return tags;
	}
}
