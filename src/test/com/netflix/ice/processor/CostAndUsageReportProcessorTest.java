package com.netflix.ice.processor;

import static org.junit.Assert.*;

import java.io.File;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.junit.Test;

import com.netflix.ice.common.LineItem;

public class CostAndUsageReportProcessorTest {
    private static final String resourcesDir = "src/test/resources";

	@Test
	public void testReportName() {
		String bucketPrefix = "prefix/hourly-cost-and-usage";
		String reportName = CostAndUsageReportProcessor.reportName(bucketPrefix);
		assertTrue("Incorrect report name", reportName.equals("hourly-cost-and-usage"));
	}

	@Test
	public void testGetDateTimeFromCostAndUsageReportNotNull() {
		String reportName = "hourly-cost-and-usage";
		String key = "hourly/hourly-cost-and-usage/20170601-20170701/hourly-cost-and-usage-Manifest.json";
		DateTime dataTime = CostAndUsageReportProcessor.getDateTimeFromCostAndUsageReport(key, CostAndUsageReportProcessor.getPattern(reportName));
		assertNotEquals("Returned time is null", dataTime, null);
	}
	
	@Test
	public void testGetDateTimeFromCostAndUsageReportNull() {
		String reportName = "hourly-cost-and-usage";
		String key = "hourly/hourly-cost-and-usage/20170601-20170701/f255bdaf-4148-4b28-8201-c3d190f27a13/hourly-cost-and-usage-Manifest.json";
		DateTime dataTime = CostAndUsageReportProcessor.getDateTimeFromCostAndUsageReport(key, CostAndUsageReportProcessor.getPattern(reportName));
		assertEquals("Returned time is null", dataTime, null);
	}
	
	@Test
	public void testLineItem() {
		String cau1 = "somelineitemid,2017-08-01T00:00:00Z/2017-08-01T01:00:00Z,,AWS,Anniversary,123456789012,2017-08-01T00:00:00Z,2017-09-01T00:00:00Z,234567890123,DiscountedUsage,2017-08-01T00:00:00Z,2017-08-01T01:00:00Z,AmazonEC2,USW2-BoxUsage:c3.4xlarge,RunInstances,us-west-2a,i-02345901991a472d6,1.00000000,32.0,32.0,USD,0.0000000000,0.00000000,0.2632173639,0.26321736,\"Linux/UNIX (Amazon VPC) c3.4xlarge reserved instance applied\",,Amazon Elastic Compute Cloud,,,,,,,,,,2.8 GHz,,Yes,,,,,,,,,,,,55,,,Yes,,,,,,,,,,,,,Compute optimized,c3.4xlarge,,,No License required,US West (Oregon),AWS Region,,,,,,,30 GiB,,,,,,High,32,Linux,RunInstances,,,Intel Xeon E5-2680 v2 (Ivy Bridge),,NA,,64-bit,Intel AVX; Intel Turbo,Compute Instance,,,,,,,,,,,,AmazonEC2,9GHZN7VCNV2MGV4N,,,,2 x 160 SSD,,,,,Shared,,,,,,USW2-BoxUsage:c3.4xlarge,16,,,,,,1yr,standard,Partial Upfront,0.8400000000,0.8400000000,Reserved,Hrs,,,,arn:aws:ec2:us-west-2:123456789012:reserved-instances/aaaaaaaa-1942-qqqq-xxxx-cec03ddb1234,,,,,,,,,,";
		CostAndUsageReportProcessor cauProc = new CostAndUsageReportProcessor(null);
		File manifest = new File(resourcesDir + "/manifestTest.json");
        CostAndUsageReport cauReport = new CostAndUsageReport(manifest, cauProc);
        LineItem lineItem = new CostAndUsageReportLineItem(true, cauReport);
		lineItem.setItems(cau1.split(","));
		
		assertTrue("AccountID wrong", lineItem.getAccountId().equals("234567890123"));	    
		assertTrue("Product is wrong", lineItem.getProduct().equals("Amazon Elastic Compute Cloud"));	    
		assertTrue("Zone is wrong", lineItem.getZone().equals("us-west-2a"));	    
		assertTrue("Reserved is wrong", lineItem.getReserved().equals("Reserved"));	    
		assertTrue("Description is wrong", lineItem.getDescription().equals("\"Linux/UNIX (Amazon VPC) c3.4xlarge reserved instance applied\""));	    
		assertTrue("UsageType is wrong", lineItem.getUsageType().equals("USW2-BoxUsage:c3.4xlarge"));	    
		assertTrue("Operation is wrong", lineItem.getOperation().equals("RunInstances"));	    
		assertEquals("UsageQuantity is wrong", Double.parseDouble(lineItem.getUsageQuantity()), 1.0, 0.001);	    
		assertTrue("StartTime is wrong", lineItem.getStartTime().equals("2017-08-01T00:00:00Z"));		
		assertTrue("EndTime is wrong", lineItem.getEndTime().equals("2017-08-01T01:00:00Z"));		
		assertTrue("Rate is wrong", lineItem.getRate().equals("0.2632173639"));		
		assertTrue("Cost is wrong", lineItem.getCost().equals("0.26321736"));		
		assertTrue("Resource is wrong", lineItem.getResource().equals("i-02345901991a472d6"));		
		assertTrue("HasResources is wrong", lineItem.hasResources());		
		assertEquals("Start millis is wrong " + lineItem.getStartMillis(), lineItem.getStartMillis(), 1501545600000L);		
		assertEquals("End millis is wrong " + lineItem.getEndMillis(), lineItem.getEndMillis(), 1501549200000L); 
		
		String tagsHeader = StringUtils.join(lineItem.getResourceTagsHeader(), ",");
		assertTrue("Resource tags header is wrong " + tagsHeader, tagsHeader.equals(
				"resourceTags/aws:createdBy,resourceTags/user:Environment,resourceTags/user:Email,resourceTags/user:BusinessUnit,resourceTags/user:Name,resourceTags/user:Project,resourceTags/user:Product"));
		
		// items defined above starts out with no tags
		assertEquals("Resource tag size is wrong: " + lineItem.getResourceTagsSize(), lineItem.getResourceTagsSize(), 0);		
		assertTrue("ResourceTag is wrong", lineItem.getResourceTag(0).equals(""));		
		assertTrue("ResourceTagString is wrong", lineItem.getResourceTagsString().equals(""));		
		assertTrue("IsReserved is wrong", lineItem.isReserved());
	}


}
