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
import com.netflix.ice.common.LineItem;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 *
 */
public class SampleMapDbResourceService extends ResourceService {

    public static final String UNKNOWN = "unknown";
    private static final Logger logger = LoggerFactory.getLogger(SampleMapDbResourceService.class);
    @SuppressWarnings("unchecked")
	private List<List<String>> productNamesWithResources = Lists.<List<String>>newArrayList(
            Lists.newArrayList(Product.ec2, Product.ec2Instance, Product.ebs),
            Lists.newArrayList(Product.rds, Product.rdsInstance),
            Lists.newArrayList(Product.s3));
    
	private List<List<Product>> productsWithResources = Lists.<List<Product>>newArrayList();

    MapDb instanceDb;
    ProductService productService;
    
    public SampleMapDbResourceService(ProductService productService) {
		super();
		this.productService = productService;
    }

    public void init(String[] customTags) {
        instanceDb = new MapDb("instances");
        
        for (List<String> l: productNamesWithResources) {
        	List<Product> lp = Lists.newArrayList();
        	for (String name: l) {
        		lp.add(productService.getProductByName(name));
        	}
        	productsWithResources.add(lp);
        }
    }

    @Override
    public void commit() {
        instanceDb.commit();
    }
    
	@Override
	public void initHeader(String[] header) {
		logger.info("initHeader...");
	}


    @Override
    public List<List<Product>> getProductsWithResources() {
        return productsWithResources;
    }

    @Override
    public String getResource(Account account, Region region, Product product, LineItem lineItem, long millisStart) {

        if (product.isEc2() || product.isEc2Instance() || product.isEbs() || product.isEC2CloudWatch()) {
            return getEc2Resource(account, region, lineItem, millisStart);
        }
        else if (product.isRds() || product.isRdsInstance()) {
            return getRdsResource(account, region, lineItem, millisStart);
        }
        else if (product.isS3()) {
            return getS3Resource(account, region, lineItem, millisStart);
        }
        else if (product.isEip()) {
            return null;
        }
        else {
            return lineItem.getResource();
        }
    }

    protected String getEc2Resource(Account account, Region region, LineItem lineItem, long millisStart) {
    	String resourceId = lineItem.getResource();
        String autoScalingGroupName = lineItem.getResourceTagsSize() > 0 ? lineItem.getResourceTag(0) : null;

        if (StringUtils.isEmpty(autoScalingGroupName)) {
            return UNKNOWN;
        }
        else if (resourceId.startsWith("i-")) {
            String appName = autoScalingGroupName.length() > 5 ? autoScalingGroupName.substring(0, 5) : autoScalingGroupName;
            instanceDb.SetResource(account, region, lineItem.getResource(), appName, millisStart);
            return autoScalingGroupName;
        }
        else {
            return UNKNOWN;
        }
    }

    protected String getRdsResource(Account account, Region region, LineItem lineItem, long millisStart) {
    	String resourceId = lineItem.getResource();
        if (resourceId.indexOf(":db:") > 0)
            return resourceId.substring(resourceId.indexOf(":db:") + 4);
        else
            return resourceId;
    }

    protected String getS3Resource(Account account, Region region, LineItem lineItem, long millisStart) {
        return lineItem.getResource();
    }
}
