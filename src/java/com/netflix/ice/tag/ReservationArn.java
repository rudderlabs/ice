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
package com.netflix.ice.tag;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.netflix.ice.tag.Product.Code;

public class ReservationArn extends Tag {
	private static final long serialVersionUID = 1L;
    protected static Logger logger = LoggerFactory.getLogger(ReservationArn.class);

    private static ConcurrentMap<String, ReservationArn> tagsByName = Maps.newConcurrentMap();
    private static Map<Product.Code, String> prefixes = Maps.newHashMap();
    private static Map<Product.Code, String> products = Maps.newHashMap();
    
	public static ReservationArn debugReservationArn = null; // Set to a valid reservation ARN for debugging
    
    static {
    	products.put(Code.DynamoDB, "dynamodb");
    	products.put(Code.Ec2Instance, "ec2");
    	products.put(Code.ElastiCache, "elasticache");
    	products.put(Code.Elasticsearch, "es");
    	products.put(Code.RdsInstance, "rds");
    	products.put(Code.Redshift, "redshift");
    	
    	prefixes.put(Code.DynamoDB, "reserved-instances/");
    	prefixes.put(Code.Ec2Instance, "reserved-instances/");
    	prefixes.put(Code.ElastiCache, "reserved-instance:");
    	prefixes.put(Code.Elasticsearch, "reserved-instances/");
    	prefixes.put(Code.RdsInstance, "ri:");
    	prefixes.put(Code.Redshift, "reserved-instances/");
    	
    }

	private ReservationArn(String name) {
		super(name);
	}
	
	public static ReservationArn get(Account account, Region region, Product product, String id) throws Exception {
		String prod = products.get(product.getCode());
		if (prod == null) {
			logger.error("Could not find product string for " + product.getCode());
			throw new Exception("Product is null");
		}
		String prefix = prefixes.get(product.getCode());
		if (prefix == null) {
			logger.error("Could not find prefix string for " + product.getCode());
			throw new Exception("Prefix is null");
		}
		
		String arn = "arn:aws:" + prod + ":" + region.name + ":" + account.getId() + ":" + prefix + id;
		return get(arn);
	}
	
	public static ReservationArn get(String name) {
		if (name == null)
			return null;
		ReservationArn tag = tagsByName.get(name);
        if (tag == null) {
        	tagsByName.putIfAbsent(name, new ReservationArn(name));
        	tag = tagsByName.get(name);
        }
        return tag;
	}
}
