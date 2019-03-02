package com.netflix.ice.processor.kubernetes;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.basic.BasicReservationService;
import com.netflix.ice.basic.BasicResourceService;
import com.netflix.ice.common.IceOptions;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.ProcessorConfig;
import com.netflix.ice.processor.ReservationService;
import com.netflix.ice.processor.pricelist.PriceListService;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.Zone;

public class KubernetesProcessorTest {
	private static final String resourceDir = "src/test/resources/";

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
	    protected Map<String, String> getDefaultAccountNames() {
			return null;
		}
	}
	
	class TestKubernetesProcessor extends KubernetesProcessor {

		public TestKubernetesProcessor(ProcessorConfig config, DateTime start) throws IOException {
			super(config, start);
		}

		@Override
		protected List<KubernetesReport> getReportsToProcess(DateTime start) {
			return null;
		}
	}
	
	class TestKubernetesReport extends KubernetesReport {

		public TestKubernetesReport(DateTime month, String[] userTags) {
			super(null, null, null, null, null, null, month, userTags);
		}
		
		public void test(File file) {
			readFile(file);
		}
	}
	
	@Test
	public void testProcessHourClusterData() throws Exception {
		Properties props = new Properties();
		
        @SuppressWarnings("deprecation")
		AWSCredentialsProvider credentialsProvider = new InstanceProfileCredentialsProvider();
        
        List<Account> accounts = Lists.newArrayList(new Account("123456789012", "Account1"));
        ProductService productService = new BasicProductService(null);
        ReservationService reservationService = new BasicReservationService(null, null, false);
        
        String[] customTags = new String[]{ "ClusterTag", "ComputeTag", "NamespaceTag" };
        Map<String, List<String>> tagKeys = Maps.newHashMap();
        Map<String, List<String>> tagValues = Maps.newHashMap();
        ResourceService resourceService = new BasicResourceService(productService, customTags, new String[]{}, tagKeys, tagValues, null);
        
        props.setProperty(IceOptions.START_MONTH, "2019-01");
        props.setProperty(IceOptions.WORK_S3_BUCKET_NAME, "foo");
        props.setProperty(IceOptions.WORK_S3_BUCKET_REGION, "us-east-1");
        props.setProperty(IceOptions.BILLING_S3_BUCKET_NAME, "bar");
        props.setProperty(IceOptions.BILLING_S3_BUCKET_REGION, "us-east-1");
        props.setProperty(IceOptions.KUBERNETES_CLUSTER_NAME_FORMULA, "ClusterTag");
        props.setProperty(IceOptions.KUBERNETES_COMPUTE_TAG, "ComputeTag:compute");
        props.setProperty(IceOptions.KUBERNETES_NAMESPACE_TAG, "NamespaceTag");
        props.setProperty(IceOptions.KUBERNETES_USER_TAGS, "UserTag1,UserTag2");
        
		ProcessorConfig config = new TestConfig(
				props,
	            credentialsProvider,
	            productService,
	            reservationService,
	            resourceService,
	            null,
	            true);
		
		KubernetesProcessor kp = new TestKubernetesProcessor(config, null);
		
		TestKubernetesReport tkr = new TestKubernetesReport(new DateTime("2019-01", DateTimeZone.UTC), customTags);
		
		File file = new File(resourceDir, "kubernetes-2019-01.csv");
		tkr.test(file);
		List<String[]> hourClusterData = tkr.getData("dev-usw2a", 395);
		Map<TagGroup, Double> hourCostData = Maps.newHashMap();

		Region.US_WEST_2.addZone("us-west-2a");
		Zone us_west_2a = Zone.getZone("us-west-2a");
		Product ec2Instance = productService.getProductByName(Product.ec2Instance);
		UsageType usageType = UsageType.getUsageType("r5.4xlarge", "hours");
		
		String[] tags = new String[]{ "dev-usw2a", "compute", "", };
		ResourceGroup resourceGroup = ResourceGroup.getResourceGroup(tags);
		TagGroup tg = TagGroup.getTagGroup(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.ondemandInstances, usageType, resourceGroup);
		double cost = 40.0;
		hourCostData.put(tg, cost);
		kp.processHourClusterData(hourCostData, tg, "dev-usw2a", tkr, hourClusterData);
		
		String[] atags = new String[]{ "dev-usw2a", "compute", "kube-system", };
		ResourceGroup arg = ResourceGroup.getResourceGroup(atags);
		TagGroup atg = TagGroup.getTagGroup(tg.account, tg.region, tg.zone, tg.product, tg.operation, tg.usageType, arg);
		
		Double allocatedCost = hourCostData.get(atg);
		assertNotNull("No allocated cost for kube-system namespace", allocatedCost);
		assertEquals("Incorrect allocated cost", 0.4133, allocatedCost, 0.0001);
		String[] unusedTags = new String[]{ "dev-usw2a", "compute", "unused", };
		TagGroup unusedTg = TagGroup.getTagGroup(tg.account, tg.region, tg.zone, tg.product, tg.operation, tg.usageType, ResourceGroup.getResourceGroup(unusedTags));
		double unusedCost = hourCostData.get(unusedTg);
		assertEquals("Incorrect unused cost", 21.1983, unusedCost, 0.0001);
		
		// Add up all the cost values to see if we get back to 40.0
		double total = 0.0;
		for (double v: hourCostData.values())
			total += v;
		assertEquals("Incorrect total cost when adding unused and allocated values", 40.0, total, 0.001);		
	}

}
