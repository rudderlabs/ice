package com.netflix.ice.basic;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.LineItem;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.ResourceGroup;

public class BasicResourceService extends ResourceService {
    private final static Logger logger = LoggerFactory.getLogger(BasicResourceService.class);

    protected final String[] customTags;
    private List<String> userTags;
    
    @SuppressWarnings("unchecked")
	private List<List<String>> productNamesWithResources = Lists.<List<String>>newArrayList(
            Lists.newArrayList(Product.ec2, Product.ec2Instance, Product.ebs),
            Lists.newArrayList(Product.rds, Product.rdsInstance),
            Lists.newArrayList(Product.redshift),
            Lists.newArrayList(Product.s3));
    
	private List<List<Product>> productsWithResources = Lists.<List<Product>>newArrayList();

    // Map of tags where each tag has a list of aliases.
    private final Map<String, List<String>> tagKeys;
    
    // Map of tag values to canonical name. All keys are lower case.
    private final Map<String, String> tagValuesInverted;
    
    // Map containing the lineItem column indecies that match the canonical tag keys specified by CustomTags
    // Key is the Custom Tag name (without the "user:" prefix). First index in the list is always the exact
    // custom tag name match if present.
    private Map<String, List<Integer>> tagIndecies;
    
    private static final String USER_TAG_PREFIX = "user:";
    
	public static class Key implements Comparable<Key> {
        public final String account;
        public final String tag;
        
        public Key(String account, String tag) {
            this.account = account;
            this.tag = tag;
        }

        public int compareTo(Key t) {
            int result = this.account.compareTo(t.account);
            if (result != 0)
                return result;
            result = this.tag.compareTo(t.tag);
            return result;
        }

        @Override
        public String toString() {
            return account + "|" + tag;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null)
                return false;
            Key other = (Key)o;
            return
                this.account.equals(other.account) &&
                this.tag.equals(other.tag);
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + this.account.hashCode();
            result = prime * result + this.tag.hashCode();
            return result;
        }
	}
    // Map of default user tag values for each account. These are returned if the requested resource doesn't
    // have a tag value.
    private final Map<Key, String> defaultTags;

    public BasicResourceService(ProductService productService, String[] customTags, String[] additionalTags,
    		Map<String, List<String>> tagKeys, Map<String, List<String>> tagValues, Map<Key, String> defaultTags) {
		super();
		this.customTags = customTags;
		
		this.tagKeys = tagKeys;		
		this.tagValuesInverted = Maps.newHashMap();
		for (Entry<String, List<String>> entry: tagValues.entrySet()) {			
			for (String val: entry.getValue()) {
				this.tagValuesInverted.put(val.toLowerCase(), entry.getKey());
			}
			// Handle upper/lower case differences of key
			this.tagValuesInverted.put(entry.getKey().toLowerCase(), entry.getKey());
		}
		
        for (List<String> l: productNamesWithResources) {
        	List<Product> lp = Lists.newArrayList();
        	for (String name: l) {
        		lp.add(productService.getProductByName(name));
        	}
        	productsWithResources.add(lp);
        }
		logger.info("Initialized BasicResourceService with " + productsWithResources.size() + " products");
		
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
		if (defaultTags != null) {
			this.defaultTags.putAll(defaultTags);
		}
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
    public ResourceGroup getResourceGroup(Account account, Region region, Product product, LineItem lineItem, long millisStart) {
        // Build the resource group based on the values of the custom tags
    	String[] tags = new String[customTags.length];
       	boolean hasTag = false;
       	for (int i = 0; i < customTags.length; i++) {
        	String v = getUserTagValue(lineItem, customTags[i]);
        	if (v == null)
        		v = getDefaultUserTagValue(account, customTags[i]);
        	tags[i] = v;
        	hasTag = v == null ? hasTag : true;
        }
        // If we didn't have any tags, just return a ResourceGroup
        return hasTag ? ResourceGroup.getResourceGroup(tags) : ResourceGroup.getResourceGroup(product.name, true);
    }
    
    private String getDefaultUserTagValue(Account account, String tag) {
    	// return the default user tag value for the specified account if there is one.
    	Key key = new Key(account.name, tag);
    	return defaultTags.get(key);
    }
    
    @Override
    public String getUserTagValue(LineItem lineItem, String tag) {
    	// Grab the first non-empty value
    	for (int index: tagIndecies.get(tag)) {
    		if (lineItem.getResourceTagsSize() > index) {
    			String val = lineItem.getResourceTag(index);
    			if (!StringUtils.isEmpty(val)) {
	    			if (tagValuesInverted.containsKey(val.toLowerCase())) {
	    				val = tagValuesInverted.get(val.toLowerCase());
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
    public List<List<Product>> getProductsWithResources() {
        return productsWithResources;
        
//        List<List<Product>> result = Lists.newArrayList();
//        for (Product product: ReaderConfig.getInstance().productService.getProducts()) {
//            result.add(Lists.<Product>newArrayList(product));
//        }
//        return result;
    }

    @Override
    public void commit() {

    }
    
    @Override
    public void initHeader(String[] header) {
    	tagIndecies = Maps.newHashMap();
    	for (String tag: userTags) {
    		String fullTag = USER_TAG_PREFIX + tag;
    		List<Integer> indecies = Lists.newArrayList();
    		tagIndecies.put(tag, indecies);
    		
    		// First check the preferred key name
    		int index = -1;
    		for (int i = 0; i < header.length; i++) {
    			if (header[i].equals(fullTag)) {
    				index = i;
    				break;
    			}
    		}
    		if (index >= 0) {
    			indecies.add(index);
    		}
    		// Look for alternate names
            for (int i = 0; i < header.length; i++) {
            	if (i == index) {
            		continue;	// skip the exact match we handled above
            	}
            	if (fullTag.equalsIgnoreCase(header[i])) {
            		indecies.add(i);
            	}
            }
            // Look for aliases
            if (tagKeys.containsKey(tag)) {
            	for (String alias: tagKeys.get(tag)) {
            		String fullAlias = USER_TAG_PREFIX + alias;
                    for (int i = 0; i < header.length; i++) {
                    	if (fullAlias.equalsIgnoreCase(header[i])) {
                    		indecies.add(i);
                    	}
                    }
            	}
            }
    	}
    }
}
