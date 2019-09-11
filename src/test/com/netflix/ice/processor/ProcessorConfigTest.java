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
package com.netflix.ice.processor;

import static org.junit.Assert.*;

import java.util.Map;
import java.util.Properties;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.google.common.collect.Maps;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.basic.BasicReservationService;
import com.netflix.ice.common.IceOptions;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.processor.config.AccountConfig;
import com.netflix.ice.processor.pricelist.PriceListService;

public class ProcessorConfigTest {
	
	class TestConfig extends ProcessorConfig {

		public TestConfig(Properties properties,
				AWSCredentialsProvider credentialsProvider,
				ProductService productService,
				ReservationService reservationService,
				ResourceService resourceService,
				PriceListService priceListService, boolean compress)
				throws Exception {
			super(properties, credentialsProvider, productService,
					reservationService, resourceService,
					priceListService, compress);
		}
		
		@Override
	    protected void initZones() {			
		}
		
		@Override
	    protected Map<String, AccountConfig> getAccountsFromOrganizations() {
			Map<String, AccountConfig> accounts = Maps.newHashMap();
			return accounts;
		}
		
		@Override
		protected void processBillingDataConfig(Map<String, AccountConfig> accountConfigs) {
		}
		
	}

	@Test
	public void testEdpDiscounts() throws Exception {
		Properties props = new Properties();
		
        @SuppressWarnings("deprecation")
		AWSCredentialsProvider credentialsProvider = new InstanceProfileCredentialsProvider();
        
        ProductService productService = new BasicProductService();
        ReservationService reservationService = new BasicReservationService(null, null);
        ResourceService resourceService = null;
        PriceListService priceListService = null;
        
        props.setProperty(IceOptions.START_MONTH, "2019-01");
        props.setProperty(IceOptions.WORK_S3_BUCKET_NAME, "foo");
        props.setProperty(IceOptions.WORK_S3_BUCKET_REGION, "us-east-1");
        props.setProperty(IceOptions.BILLING_S3_BUCKET_NAME, "bar");
        props.setProperty(IceOptions.BILLING_S3_BUCKET_REGION, "us-east-1");
        
        props.setProperty(IceOptions.EDP_DISCOUNTS, "2019-01:5,2019-02:8");
        
		ProcessorConfig config = new TestConfig(
				props,
	            credentialsProvider,
	            productService,
	            reservationService,
	            resourceService,
	            priceListService,
	            true);
		
		// discount prior to start
		assertEquals("Wrong discount", 0.00, config.getDiscount(new DateTime("2018-12-31T00:00:00Z", DateTimeZone.UTC)), 0.0001);
		// discount equal to start
		assertEquals("Wrong discount", 0.05, config.getDiscount(new DateTime("2019-01-01T00:00:00Z", DateTimeZone.UTC)), 0.0001);
		// discount during first period
		assertEquals("Wrong discount", 0.05, config.getDiscount(new DateTime("2019-01-02T13:00:00Z", DateTimeZone.UTC)), 0.0001);
		// discount at start of second period
		assertEquals("Wrong discount", 0.08, config.getDiscount(new DateTime("2019-02-01T00:00:00Z", DateTimeZone.UTC)), 0.0001);
		// discount during second period
		assertEquals("Wrong discount", 0.08, config.getDiscount(new DateTime("2019-02-02T04:00:00Z", DateTimeZone.UTC)), 0.0001);

		assertEquals("Wrong discounted cost", 0.95, config.getDiscountedCost(new DateTime("2019-01-01T00:00:00Z", DateTimeZone.UTC), 1.0), 0.0001);
	}

}
