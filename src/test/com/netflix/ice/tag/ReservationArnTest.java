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
import com.netflix.ice.tag.Product.Code;

public class ReservationArnTest {

	@Test
	public void testGet() throws Exception {
		AccountService as = new BasicAccountService();
		ProductService ps = new BasicProductService();
		String account = "123456789012";
		String reservationID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
		Region region = Region.US_WEST_2;
		
		class TestCase {
			public Code productCode;
			public String arnProductCode;
			public String prefix;
			
			TestCase(Code c, String apc, String p) {
				productCode = c;
				arnProductCode = apc;
				prefix = p;
			}
		}
		
		TestCase[] testCases = new TestCase[]{
				new TestCase(Code.DynamoDB, "dynamodb", "reserved-instances/"),
				new TestCase(Code.Ec2Instance, "ec2", "reserved-instances/"),
				new TestCase(Code.ElastiCache, "elasticache", "reserved-instance:"),
				new TestCase(Code.Elasticsearch, "es", "reserved-instances/"),
				new TestCase(Code.RdsInstance, "rds", "ri:"),
				new TestCase(Code.Redshift, "redshift", "reserved-instances/"),
		};
		
		for (TestCase tc: testCases) {
			ReservationArn arn = ReservationArn.get(as.getAccountById(account, ""), region, ps.getProduct(tc.productCode), reservationID);
			String expect = "arn:aws:" + tc.arnProductCode + ":" + region.name + ":" + account + ":" + tc.prefix + reservationID;
			assertEquals("Incorrect ARN for " + tc.productCode, expect, arn.name);
		}
	}

}
