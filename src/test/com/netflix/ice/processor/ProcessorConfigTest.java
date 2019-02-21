package com.netflix.ice.processor;

import static org.junit.Assert.*;

import java.util.Properties;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.basic.BasicReservationService;
import com.netflix.ice.common.IceOptions;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.ResourceService;
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
		
	}

	@Test
	public void testEdpDiscounts() throws Exception {
		Properties props = new Properties();
		
        @SuppressWarnings("deprecation")
		AWSCredentialsProvider credentialsProvider = new InstanceProfileCredentialsProvider();
        
        ProductService productService = new BasicProductService(null);
        ReservationService reservationService = new BasicReservationService(null, null, false);
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
