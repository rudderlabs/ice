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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.tag.Product;

import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;

public class BasicProductService implements ProductService {
	/*
	 * Product lookup maps built dynamically as we encounter the product names while
	 * processing billing reports or reading tagdb files.
	 * 
	 * Map of products keyed by the full Amazon name including the AWS and Amazon prefix
	 */
    private static ConcurrentMap<String, Product> productsByAwsName = Maps.newConcurrentMap();
    /*
     * Map of products keyed by the name without the AWS or Amazon prefix. Also has entries for override names
     */
    private static ConcurrentMap<String, Product> productsByName = Maps.newConcurrentMap();
    /*
     * Map of products keyed by the filename used for saving the data
     */
    private static ConcurrentMap<String, Product> productsByFileName = Maps.newConcurrentMap();
       
    public BasicProductService(Properties overrideProductNames) {
		super();
		if (overrideProductNames != null) {
			for (String overrideName: overrideProductNames.stringPropertyNames())
				Product.addOverride(overrideProductNames.getProperty(overrideName), overrideName);
		}
	}

	public Product getProductByAwsName(String awsName) {
        Product product = productsByAwsName.get(awsName);
        if (product == null) {
            product = new Product(awsName);
            addProduct(product);
        }
        return product;
    }
    
    public Product getProductByFileName(String fileName) {
    	// Look up the product by the name used for the tagdb file
    	Product product = productsByFileName.get(fileName);
    	if (product == null) {
    		String name = Product.getNameFromFileName(fileName);
    		product = new Product(name);
    		addProduct(product);
    	}
    	return product;
    }

    public Product getProductByName(String name) {
        Product product = productsByName.get(name);
        if (product == null) {
            product = new Product(name);
            addProduct(product);
        }
        return product;
    }
    
    private void addProduct(Product product) {
        productsByName.put(product.name, product);
        productsByFileName.put(product.getFileName(), product);

        String canonicalName = product.getCanonicalName();
        if (!canonicalName.equals(product.name)) {
        	// Product is using an alternate name, also save the canonical name
        	productsByName.put(canonicalName, product);
        }
        
        productsByAwsName.put("AWS " + canonicalName, product);
        productsByAwsName.put("Amazon " + canonicalName, product);
        // AmazonCloudWatch is at least one of the product names that doesn't use a space. There may be others...
        productsByAwsName.put("Amazon" + canonicalName, product);
    }

    public Collection<Product> getProducts() {
        return productsByName.values();
    }

    public List<Product> getProducts(List<String> names) {
    	List<Product> result = Lists.newArrayList();
    	for (String name: names)
    	    result.add(productsByName.get(name));
    	return result;
    }
}
