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

import static org.junit.Assert.*;

import org.junit.Test;

import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;

public class ReservationArnTest {

	@Test
	public void testGet() throws Exception {
		AccountService as = new BasicAccountService();
		ProductService ps = new BasicProductService();
		String account = "123456789012";
		String reservationID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
		Region region = Region.US_WEST_2;
		
		class TestCase {
			public String name;
			public String code;
			public String prefix;
			
			TestCase(String n, String c, String p) {
				name = n;
				code = c;
				prefix = p;
			}
		}
		
		TestCase[] testCases = new TestCase[]{
				new TestCase(Product.dynamoDB, "dynamodb", "reserved-instances/"),
				new TestCase(Product.ec2Instance, "ec2", "reserved-instances/"),
				new TestCase(Product.elastiCache, "elasticache", "reserved-instance:"),
				new TestCase(Product.elasticsearch, "es", "reserved-instances/"),
				new TestCase(Product.rdsInstance, "rds", "ri:"),
				new TestCase(Product.redshift, "redshift", "reserved-instances/"),
		};
		
		for (TestCase tc: testCases) {
			ReservationArn arn = ReservationArn.get(as.getAccountById(account), region, ps.getProductByName(tc.name), reservationID);
			String expect = "arn:aws:" + tc.code + ":" + region.name + ":" + account + ":" + tc.prefix + reservationID;
			assertEquals("Incorrect ARN for " + tc.name, expect, arn.name);
		}
	}

}
