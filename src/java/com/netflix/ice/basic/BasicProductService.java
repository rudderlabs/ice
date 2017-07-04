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
import java.util.concurrent.ConcurrentMap;

public class BasicProductService implements ProductService {

	/*
	 * Product lookup maps built dynamically as we encounter the product names while
	 * processing billing reports or reading tagdb files.
	 */
    private static ConcurrentMap<String, Product> productsByAwsName = Maps.newConcurrentMap();
    private static ConcurrentMap<String, Product> productsByName = Maps.newConcurrentMap();
       
    public Product getProductByAwsName(String awsName) {
        Product product = productsByAwsName.get(awsName);
        if (product == null) {
            product = new Product(awsName);
            productsByAwsName.put(product.name, product);
            productsByName.put(product.name, product);
        }
        return product;
    }

    public Product getProductByName(String name) {
        Product product = productsByName.get(name);
        if (product == null) {
            product = new Product(name);
            productsByName.put(product.name, product);

            String awsName = Product.getAwsName(name);
            productsByAwsName.put("AWS " + awsName, product);
            productsByAwsName.put("Amazon " + awsName, product);
        }
        return product;
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
