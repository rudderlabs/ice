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
package com.netflix.ice.basic;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import com.amazonaws.services.ec2.model.Tag;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.LineItem;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.common.TagConfig;
import com.netflix.ice.common.TagMappings;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.ResourceGroup;

public class BasicResourceService extends ResourceService {
    //private final static Logger logger = LoggerFactory.getLogger(BasicResourceService.class);

    protected final String[] customTags;
    private final List<String> userTags;
    
    // Map of tags where each tag has a list of aliases. Outer key is the payerAccountId.
    private Map<String, Map<String, TagConfig>> tagConfigs;
    
    // Map of tag values to canonical name. All keys are lower case.
    // Maps are nested by Payer Account ID, Tag Key, then Value
    private Map<String, Map<String, Map<String, String>>> tagValuesInverted;
    
    // Map containing the lineItem column indeces that match the canonical tag keys specified by CustomTags
    // Key is the Custom Tag name (without the "user:" prefix). First index in the list is always the exact
    // custom tag name match if present.
    private Map<String, List<Integer>> tagLineItemIndeces;
    
    private final Map<String, Integer> tagResourceGroupIndeces;
    
    private static final String USER_TAG_PREFIX = "user:";
    
    // Map containing values to assign to destination tags based on a match with a value
    // in a source tag. These are returned if the requested resource doesn't have a tag value.
    // Primary map key is the payer account ID, secondary map key is the destination tag key
    private Map<String, Map<String, List<MappedTags>>> mappedTags;
    
    private class MappedTags {
    	Map<Integer, Map<String, String>> maps; // Primary map key is source tag index, secondary map key is the source tag value
    	List<String> include;
    	List<String> exclude;
    	
    	public MappedTags(TagMappings config) {
			maps = Maps.newHashMap();
			for (String mappedValue: config.maps.keySet()) {
				Map<String, List<String>> configValueMaps = config.maps.get(mappedValue);
				for (String sourceTag: configValueMaps.keySet()) {
					Integer sourceTagIndex = tagResourceGroupIndeces.get(sourceTag);
					Map<String, String> mappedValues = maps.get(sourceTagIndex);
					if (mappedValues == null) {
						mappedValues = Maps.newHashMap();
						maps.put(sourceTagIndex, mappedValues);
					}
					for (String target: configValueMaps.get(sourceTag)) {
						mappedValues.put(target.toLowerCase(), mappedValue);
					}
				}
			}
			include = config.include;
			if (include == null)
				include = Lists.newArrayList();
			exclude = config.exclude;
			if (exclude == null)
				exclude = Lists.newArrayList();
    	}
    }

    // Map of default user tag values for each account. These are returned if the requested resource doesn't
    // have a tag value nor a mapped value. Outer key is the account ID, inner map key is the tag name.
    private Map<String, Map<String, String>> defaultTags;

    public BasicResourceService(ProductService productService, String[] customTags, String[] additionalTags) {
		super();
		this.customTags = customTags;
		this.tagValuesInverted = Maps.newHashMap();
		this.tagResourceGroupIndeces = Maps.newHashMap();
		for (int i = 0; i < customTags.length; i++)
			tagResourceGroupIndeces.put(customTags[i], i);
				
		userTags = Lists.newArrayList();
		for (String tag: customTags) {
			if (!tag.isEmpty())
				userTags.add(tag);
		}
		for (String tag: additionalTags) {
			if (!tag.isEmpty())
				userTags.add(tag);		
		}
		
		this.defaultTags = Maps.newHashMap();
		this.tagConfigs = Maps.newHashMap();
		this.tagValuesInverted = Maps.newHashMap();
		this.mappedTags = Maps.newHashMap();
	}
    
    @Override
    public Map<String, Map<String, TagConfig>> getTagConfigs() {
    	return tagConfigs;
    }

    @Override
    public void setTagConfigs(String payerAccountId, List<TagConfig> tagConfigs) {    	
    	if (tagConfigs == null) {
    		// Remove existing configs and indeces
    		this.tagConfigs.remove(payerAccountId);
    		this.tagValuesInverted.remove(payerAccountId);
    		return;
    	}
    	
    	Map<String, TagConfig> configs = Maps.newHashMap();
    	for (TagConfig config: tagConfigs) {
    		configs.put(config.name, config);
    	}
    	this.tagConfigs.put(payerAccountId, configs);
    	
    	// Create inverted indexes for each of the tag value alias sets
		Map<String, Map<String, String>> indeces = Maps.newHashMap();
		for (TagConfig config: tagConfigs) {
			if (config.values == null || config.values.isEmpty())
				continue;
			
			Map<String, String> invertedIndex = Maps.newConcurrentMap();
			for (Entry<String, List<String>> entry: config.values.entrySet()) {			
				for (String val: entry.getValue()) {
			    	// key is all lower case and strip out all whitespace
					invertedIndex.put(val.toLowerCase().replace(" ", ""), entry.getKey());
				}
				// Handle upper/lower case differences of key and remove any whitespace
				invertedIndex.put(entry.getKey().toLowerCase().replace(" ", ""), entry.getKey());
			}
			indeces.put(config.name, invertedIndex);
		}
		this.tagValuesInverted.put(payerAccountId, indeces);
		
		// Create the maps setting tags based on the values of other tags
		Map<String, List<MappedTags>> mapped = Maps.newHashMap();
		for (TagConfig config: tagConfigs) {
			if (config.mapped == null || config.mapped.isEmpty())
				continue;
			List<MappedTags> mappedTags = Lists.newArrayList();
			for (TagMappings m: config.mapped)
				mappedTags.add(new MappedTags(m));
			mapped.put(config.name, mappedTags);			
		}
		this.mappedTags.put(payerAccountId, mapped);
    }

	@Override
    public void init() {		
    }
	
	@Override
	public String[] getCustomTags() {
		return customTags;
	}
	
	@Override
	public List<String> getUserTags() {
		return userTags;
	}
	
	@Override
	public int getUserTagIndex(String tag) {
		return tagResourceGroupIndeces.get(tag);
	}

    @Override
    public ResourceGroup getResourceGroup(Account account, Region region, Product product, LineItem lineItem, long millisStart) {
        // Build the resource group based on the values of the custom tags
    	String[] tags = new String[customTags.length];
       	boolean hasTag = false;
       	for (int i = 0; i < customTags.length; i++) {
       		tags[i] = getUserTagValue(lineItem, customTags[i]);
       	}
       	
       	for (int i = 0; i < customTags.length; i++) {
       		String v = tags[i];
        	if (v == null || v.isEmpty())
        		v = getMappedUserTagValue(account, lineItem.getPayerAccountId(), customTags[i], tags);
        	if (v == null || v.isEmpty())
        		v = getDefaultUserTagValue(account, customTags[i]);
        	tags[i] = v;
        	hasTag = v == null ? hasTag : true;
        }
        // If we didn't have any tags, just return a ResourceGroup
        return hasTag ? ResourceGroup.getResourceGroup(tags) : ResourceGroup.getResourceGroup(product.name, true);
    }
    
    @Override
    public ResourceGroup getResourceGroup(Account account, Product product, List<Tag> reservedInstanceTags) {
        // Build the resource group based on the values of the custom tags
    	String[] tags = new String[customTags.length];
       	boolean hasTag = false;
       	for (int i = 0; i < customTags.length; i++) {
           	String v = null;
   			// find first matching key with a legitimate value
       		for (Tag riTag: reservedInstanceTags) {
       			if (riTag.getKey().toLowerCase().equals(customTags[i].toLowerCase())) {
       				v = riTag.getValue();
       				if (v != null && !v.isEmpty())
       					break;
       			}
       		}
        	if (v == null || v.isEmpty())
        		v = getDefaultUserTagValue(account, customTags[i]);
        	tags[i] = v;
        	hasTag = v == null ? hasTag : true;
       	}
        // If we didn't have any tags, just return a ResourceGroup
        return hasTag ? ResourceGroup.getResourceGroup(tags) : ResourceGroup.getResourceGroup(product.name, true);
    }
    
    @Override
    public void putDefaultTags(String accountId, Map<String, String> tags) {
    	defaultTags.put(accountId, tags);
    }
    
    private String getDefaultUserTagValue(Account account, String tag) {
    	// return the default user tag value for the specified account if there is one.
    	Map<String, String> defaults = defaultTags.get(account.getId());
    	return defaults == null ? null : defaults.get(tag);
    }
    
    private String getMappedUserTagValue(Account account, String payerAccount, String tag, String[] tags) {
    	// return the default user tag value for the specified account if there is one.
    	Map<String, List<MappedTags>> mappedTagsForPayerAccount = mappedTags.get(payerAccount);
    	if (mappedTagsForPayerAccount == null)
    		return null;
    	
    	List<MappedTags> listOfMappedTags = mappedTagsForPayerAccount.get(tag);
    	if (listOfMappedTags == null)
    		return null;
    	
    	for (MappedTags mt: listOfMappedTags) {
        	Map<Integer, Map<String, String>> sourceTags = mt.maps;
        	if (sourceTags == null)
        		continue;
        	
        	// If we have an include filter, make sure the account is in the list
        	if (!mt.include.isEmpty() && !mt.include.contains(account.getId()))
        		continue;
        	
        	// If we have an exclude filter, make sure the account is not in the list
        	if (!mt.exclude.isEmpty() && mt.exclude.contains(account.getId()))
        		continue;
        	
        	for (Integer sourceTagIndex: sourceTags.keySet()) {
        		String have = tags[sourceTagIndex];
        		if (have == null)
        			continue;
        		Map<String, String> values = sourceTags.get(sourceTagIndex);
        		String v = values.get(have.toLowerCase());
        		if (v != null)
        			return v;
        	}
    	}
    	
    	return null;
    }
    
    @Override
    public String getUserTagValue(LineItem lineItem, String tag) {
    	Map<String, Map<String, String>> indeces = tagValuesInverted.get(lineItem.getPayerAccountId());    	
    	Map<String, String> invertedIndex = indeces == null ? null : indeces.get(tag);
    	
    	// Grab the first non-empty value
    	for (int index: tagLineItemIndeces.get(tag)) {
    		if (lineItem.getResourceTagsSize() > index) {
    	    	// cut all white space from tag value
    			String val = lineItem.getResourceTag(index).replace(" ", "");
    			
    			if (!StringUtils.isEmpty(val)) {
    				if (invertedIndex != null && invertedIndex.containsKey(val.toLowerCase())) {
	    				val = invertedIndex.get(val.toLowerCase());
	    			}
	    			return val;
    			}
    		}
    	}
    	return null;
    }
    
    @Override
    public boolean[] getUserTagCoverage(LineItem lineItem) {
    	boolean[] userTagCoverage = new boolean[userTags.size()];
        for (int i = 0; i < userTags.size(); i++) {
        	String v = getUserTagValue(lineItem, userTags.get(i));
        	userTagCoverage[i] = !StringUtils.isEmpty(v);
        }    	
    	return userTagCoverage;
    }

    @Override
    public void commit() {

    }
    
    @Override
    public void initHeader(String[] header, String payerAccountId) {
    	tagLineItemIndeces = Maps.newHashMap();
    	Map<String, TagConfig> configs = tagConfigs.get(payerAccountId);
    	
    	/*
    	 * Create a list of billing report line item indeces for
    	 * each of the configured user tags. The list will first have
    	 * the exact match for the name if it exists in the report
    	 * followed by any case variants and specified aliases
    	 */
    	for (String tag: userTags) {
    		String fullTag = USER_TAG_PREFIX + tag;
    		List<Integer> indeces = Lists.newArrayList();
    		tagLineItemIndeces.put(tag, indeces);
    		
    		// First check the preferred key name
    		int index = -1;
    		for (int i = 0; i < header.length; i++) {
    			if (header[i].equals(fullTag)) {
    				index = i;
    				break;
    			}
    		}
    		if (index >= 0) {
    			indeces.add(index);
    		}
    		// Look for alternate names
            for (int i = 0; i < header.length; i++) {
            	if (i == index) {
            		continue;	// skip the exact match we handled above
            	}
            	if (fullTag.equalsIgnoreCase(header[i])) {
            		indeces.add(i);
            	}
            }
            // Look for aliases
            if (configs != null && configs.containsKey(tag)) {
            	TagConfig config = configs.get(tag);
            	if (config != null && config.aliases != null) {
	            	for (String alias: config.aliases) {
	            		String fullAlias = USER_TAG_PREFIX + alias;
	                    for (int i = 0; i < header.length; i++) {
	                    	if (fullAlias.equalsIgnoreCase(header[i])) {
	                    		indeces.add(i);
	                    	}
	                    }
	            	}
            	}
            }
    	}
    }
}
