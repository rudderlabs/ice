/*
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.netflix.ice.reader.ReaderConfig
import com.netflix.ice.processor.LineItemProcessor
import com.netflix.ice.processor.ProcessorConfig
import com.netflix.ice.processor.ReservationService
import com.netflix.ice.JSONConverter

import org.apache.commons.lang.StringUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.netflix.ice.common.IceOptions
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.InstanceProfileCredentialsProvider
import com.google.common.collect.Lists
import com.netflix.ice.tag.Account
import com.netflix.ice.tag.Region
import com.google.common.collect.Maps
import com.netflix.ice.basic.BasicProductService
import com.netflix.ice.basic.BasicReservationService
import com.netflix.ice.basic.BasicLineItemProcessor
import com.netflix.ice.basic.BasicResourceService
import com.netflix.ice.processor.pricelist.PriceListService
import com.netflix.ice.basic.BasicManagers

import org.joda.time.DateTime
import org.joda.time.DateTimeZone

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.BasicSessionCredentials

import org.apache.commons.io.IOUtils

import com.netflix.ice.common.ResourceService
import com.netflix.ice.common.ProductService

class BootStrap {
    private static boolean initialized = false;
    private static Logger logger = LoggerFactory.getLogger(BootStrap.class);

    private ReaderConfig readerConfig;
    private ProcessorConfig processorConfig;

    def init = { servletContext ->
        if (initialized) {
            return;
        }

        InputStream is = null;
        try {

            logger.info('Starting ice...');
			
            logger.info('Read ice.properties ...');
			Properties prop = getProperties("ice.propertiesfile", "ice.properties");			
            logger.info('Reading tag.properties...');
			Properties tagProp = getProperties("tag.propertiesfile", "tag.properties");
			
            AWSCredentialsProvider credentialsProvider;

            if (StringUtils.isEmpty(System.getProperty("ice.s3AccessKeyId")) || StringUtils.isEmpty(System.getProperty("ice.s3SecretKey")))
                credentialsProvider = new InstanceProfileCredentialsProvider();
            else
                credentialsProvider = new AWSCredentialsProvider() {
                        public AWSCredentials getCredentials() {
                            if (StringUtils.isEmpty(System.getProperty("ice.s3AccessToken")))
                                return new AWSCredentials() {
                                    public String getAWSAccessKeyId() {
                                        return System.getProperty("ice.s3AccessKeyId");
                                    }

                                    public String getAWSSecretKey() {
                                        return System.getProperty("ice.s3SecretKey");
                                    }
                                };
                            else
                                return new BasicSessionCredentials(System.getProperty("ice.s3AccessKeyId"), System.getProperty("ice.s3SecretKey"),
                                        System.getProperty("ice.s3AccessToken"));
                        }

                        public void refresh() {
                        }
                    };

			JSONConverter.register();
				
            Properties properties = new Properties();
            properties.setProperty(IceOptions.WORK_S3_BUCKET_NAME, prop.getProperty(IceOptions.WORK_S3_BUCKET_NAME));
            properties.setProperty(IceOptions.WORK_S3_BUCKET_REGION, prop.getProperty(IceOptions.WORK_S3_BUCKET_REGION));
            properties.setProperty(IceOptions.WORK_S3_BUCKET_PREFIX, prop.getProperty(IceOptions.WORK_S3_BUCKET_PREFIX));
			properties.setProperty(IceOptions.TAG_COVERAGE, prop.getProperty(IceOptions.TAG_COVERAGE, ""));
				
			// Stash any debug properties
			for (String name: prop.stringPropertyNames()) {
				if (name.startsWith(IceOptions.DEBUG + ".")) {
					properties.setProperty(name, prop.getProperty(name));
				}
			}
			
			ProductService productService = new BasicProductService(getSubProperties(tagProp, "tag.product."));

            if ("true".equals(prop.getProperty("ice.processor"))) {
				if (!StringUtils.isEmpty(prop.getProperty(IceOptions.START_MONTH))) {
	                properties.setProperty(IceOptions.START_MONTH, prop.getProperty(IceOptions.START_MONTH));
	            }
	            else {
					DateTime now = new DateTime(DateTimeZone.UTC);
	                properties.setProperty(IceOptions.START_MONTH, "" + now.year().get() + "-" + now.monthOfYear().get());
	            }

			    if (prop.getProperty(IceOptions.PROCESS_ONCE) != null) {
                	properties.setProperty(IceOptions.PROCESS_ONCE, prop.getProperty(IceOptions.PROCESS_ONCE));
					if ("true".equals(prop.getProperty(IceOptions.PROCESS_ONCE))) {
						properties.setProperty(IceOptions.PROCESSOR_REGION, getInstanceRegion());
						properties.setProperty(IceOptions.PROCESSOR_INSTANCE_ID, getInstanceId());
					}
                }

                properties.setProperty(IceOptions.LOCAL_DIR, prop.getProperty("ice.processor.localDir", "/mnt/ice"));
                properties.setProperty(IceOptions.BILLING_S3_BUCKET_NAME, prop.getProperty(IceOptions.BILLING_S3_BUCKET_NAME));
                properties.setProperty(IceOptions.BILLING_S3_BUCKET_REGION, prop.getProperty(IceOptions.BILLING_S3_BUCKET_REGION));
                properties.setProperty(IceOptions.BILLING_S3_BUCKET_PREFIX, prop.getProperty(IceOptions.BILLING_S3_BUCKET_PREFIX, ""));
                properties.setProperty(IceOptions.BILLING_PAYER_ACCOUNT_ID, prop.getProperty(IceOptions.BILLING_PAYER_ACCOUNT_ID, ""));
                properties.setProperty(IceOptions.BILLING_ACCESS_ROLENAME, prop.getProperty(IceOptions.BILLING_ACCESS_ROLENAME, ""));
                properties.setProperty(IceOptions.BILLING_ACCESS_EXTERNALID, prop.getProperty(IceOptions.BILLING_ACCESS_EXTERNALID, ""));
                properties.setProperty(IceOptions.KUBERNETES_S3_BUCKET_NAME, prop.getProperty(IceOptions.KUBERNETES_S3_BUCKET_NAME, ""));
                properties.setProperty(IceOptions.KUBERNETES_S3_BUCKET_REGION, prop.getProperty(IceOptions.KUBERNETES_S3_BUCKET_REGION, ""));
                properties.setProperty(IceOptions.KUBERNETES_S3_BUCKET_PREFIX, prop.getProperty(IceOptions.KUBERNETES_S3_BUCKET_PREFIX, ""));
                properties.setProperty(IceOptions.KUBERNETES_ACCOUNT_ID, prop.getProperty(IceOptions.KUBERNETES_ACCOUNT_ID, ""));
                properties.setProperty(IceOptions.KUBERNETES_ACCESS_ROLENAME, prop.getProperty(IceOptions.KUBERNETES_ACCESS_ROLENAME, ""));
                properties.setProperty(IceOptions.KUBERNETES_ACCESS_EXTERNALID, prop.getProperty(IceOptions.KUBERNETES_ACCESS_EXTERNALID, ""));
				properties.setProperty(IceOptions.COST_AND_USAGE_START_DATE, prop.getProperty(IceOptions.COST_AND_USAGE_START_DATE, ""));
				properties.setProperty(IceOptions.COST_AND_USAGE_NET_UNBLENDED_START_DATE, prop.getProperty(IceOptions.COST_AND_USAGE_NET_UNBLENDED_START_DATE, ""));
				properties.setProperty(IceOptions.EDP_DISCOUNTS, prop.getProperty(IceOptions.EDP_DISCOUNTS, ""));
				
				// Grab all the account properties
				for (String name: prop.stringPropertyNames()) {
					if (name.startsWith("ice.account.") || name.startsWith("ice.owneraccount.")) {
						properties.setProperty(name, prop.getProperty(name));
					}
				}

				ReservationService.ReservationPeriod reservationPeriod =
                    ReservationService.ReservationPeriod.valueOf(prop.getProperty(IceOptions.RESERVATION_PERIOD, "threeyear"));
                ReservationService.ReservationUtilization reservationUtilization =
                    ReservationService.ReservationUtilization.valueOf(prop.getProperty(IceOptions.RESERVATION_UTILIZATION, "HEAVY"));

				// Resource Tagging stuff
				String[] customTags = prop.getProperty(IceOptions.CUSTOM_TAGS, "").split(",");
				String[] additionalTags = prop.getProperty(IceOptions.ADDITIONAL_TAGS, "").split(",");

                ResourceService resourceService = StringUtils.isEmpty(prop.getProperty(IceOptions.CUSTOM_TAGS)) ? null : new BasicResourceService(productService, customTags, additionalTags);

                properties.setProperty(IceOptions.RESOURCE_GROUP_COST, prop.getProperty(IceOptions.RESOURCE_GROUP_COST, "modeled"));
				properties.setProperty(IceOptions.FAMILY_RI_BREAKOUT, prop.getProperty(IceOptions.FAMILY_RI_BREAKOUT, ""));
				properties.setProperty(IceOptions.WRITE_JSON_FILES, prop.getProperty(IceOptions.WRITE_JSON_FILES, ""));
				
				if (prop.getProperty(IceOptions.PROCESSOR_THREADS) != null)
					properties.setProperty(IceOptions.PROCESSOR_THREADS, prop.getProperty(IceOptions.PROCESSOR_THREADS));
									
				ReservationService reservationService = new BasicReservationService(reservationPeriod, reservationUtilization, "true".equals(prop.getProperty(IceOptions.RESERVATION_CAPACITY_POLLER)));
				PriceListService priceListService = new PriceListService(
					properties.getProperty(IceOptions.LOCAL_DIR), 
					properties.getProperty(IceOptions.WORK_S3_BUCKET_NAME), 
					properties.getProperty(IceOptions.WORK_S3_BUCKET_PREFIX));
				
				
                processorConfig = new ProcessorConfig(
                        properties,
                        credentialsProvider,
                        productService,
                        reservationService,
                        resourceService,
						priceListService,
						true)
				processorConfig.start();
            }

            if ("true".equals(prop.getProperty("ice.reader"))) {
                properties.setProperty(IceOptions.LOCAL_DIR, prop.getProperty("ice.reader.localDir", "/mnt/ice"));
                if (prop.getProperty(IceOptions.MONTHLY_CACHE_SIZE) != null)
                    properties.setProperty(IceOptions.MONTHLY_CACHE_SIZE, prop.getProperty(IceOptions.MONTHLY_CACHE_SIZE));
                if (prop.getProperty(IceOptions.CURRENCY_RATE) != null)
                    properties.setProperty(IceOptions.CURRENCY_RATE, prop.getProperty(IceOptions.CURRENCY_RATE));
                if (prop.getProperty(IceOptions.CURRENCY_SIGN) != null)
                    properties.setProperty(IceOptions.CURRENCY_SIGN, prop.getProperty(IceOptions.CURRENCY_SIGN));
                if (prop.getProperty(IceOptions.HIGHSTOCK_URL) != null)
                    properties.setProperty(IceOptions.HIGHSTOCK_URL, prop.getProperty(IceOptions.HIGHSTOCK_URL));
				if (prop.getProperty(IceOptions.COMPANY_NAME) != null)
					properties.setProperty(IceOptions.COMPANY_NAME, prop.getProperty(IceOptions.COMPANY_NAME));
				if (prop.getProperty(IceOptions.DASHBOARD_NOTICE) != null)
					properties.setProperty(IceOptions.DASHBOARD_NOTICE, prop.getProperty(IceOptions.DASHBOARD_NOTICE));

                readerConfig = new ReaderConfig(
                        properties,
                        credentialsProvider,
                        new BasicManagers(true),
                        productService,
                        null
                );
                readerConfig.start();
            }

            initialized = true;

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Startup failed", e);
            System.exit(0);
        }
    }

    def destroy = {
        logger.info("Shutting down ice...");

        try {
            if (processorConfig != null)
                processorConfig.shutdown();
            if (readerConfig != null)
                readerConfig.shutdown();
        }
        catch (Exception e) {
            logger.error("Failed to shut down...", e);
        }

        logger.info("Shut down complete.");
        initialized = false;
    }

    private static String getCurrentRole() throws IOException {
        String role = httpGet("http://169.254.169.254/latest/meta-data/iam/security-credentials/");
        logger.info("Found role: " + role);
        return role;
    }
    private static String getInstanceRegion() throws IOException {
        String region = httpGet("http://169.254.169.254/latest/meta-data/placement/availability-zone");
		region = region.substring(0, region.length()-1);
        logger.info("Found region: " + region);
        return region;
    }
    private static String getInstanceId() throws IOException {
        String instanceId = httpGet("http://169.254.169.254/latest/meta-data/instance-id");
        logger.info("Found instance ID: " + instanceId);
        return instanceId;
    }
	private static String httpGet(String url) throws IOException {
		URL reqUrl = new URL(url);
		URLConnection urlConnection = reqUrl.openConnection();
		InputStream input = urlConnection.getInputStream();
		String resp = IOUtils.toString(input).trim();
		input.close();
		return resp;
	}
	
	private Properties getProperties(String key, String defaultValue) {
		Properties prop = new Properties();
        InputStream is = null;
		try {
			is = getClass().getClassLoader().getResourceAsStream(System.getProperty(key, defaultValue));
			if (is == null) {
				if (System.getenv().get("ICE_HOME") == null)
					throw new IllegalArgumentException("ICE_HOME is not set.");
				is = new FileInputStream(new File(System.getenv().get("ICE_HOME"), System.getProperty(key, defaultValue)));
			}
			prop.load(is);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Startup failed", e);
            System.exit(0);
        }
        finally {
            if (is != null)
                is.close();
        }
		return prop;
	}
	
	private Properties getSubProperties(Properties tagProps, String prefix) {
		Properties subProperties = new Properties();
		for (String name: tagProps.stringPropertyNames()) {
			if (name.startsWith(prefix)) {
				String subName = name.substring(prefix.length());
				subProperties.setProperty(subName, tagProps.getProperty(name));
			}
		}
		return subProperties;
	}
}
	