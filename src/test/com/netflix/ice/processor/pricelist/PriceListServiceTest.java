package com.netflix.ice.processor.pricelist;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import org.joda.time.DateTime;
import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.ice.processor.pricelist.InstancePrices.LeaseContractLength;
import com.netflix.ice.processor.pricelist.InstancePrices.OfferingClass;
import com.netflix.ice.processor.pricelist.InstancePrices.Product;
import com.netflix.ice.processor.pricelist.InstancePrices.PurchaseOption;
import com.netflix.ice.processor.pricelist.InstancePrices.Rate;
import com.netflix.ice.processor.pricelist.InstancePrices.RateKey;
import com.netflix.ice.processor.pricelist.InstancePrices.ServiceCode;
import com.netflix.ice.processor.pricelist.VersionIndex.Version;
import com.netflix.ice.reader.InstanceMetrics;
import com.netflix.ice.tag.UsageType;

public class PriceListServiceTest {
	private static final String resourceDir = "src/test/resources/";
	private static PriceListService priceListService = null;
	
	@BeforeClass
	public static void init() throws Exception {
		priceListService = new PriceListService(resourceDir, null, null);
		priceListService.init();
	}

	@Test
	public void testPriceListService() throws Exception {
		File versionIndexFile = new File(resourceDir + "VersionIndex.json");
		InputStream stream = new FileInputStream(versionIndexFile);
        VersionIndex index = new VersionIndex(stream);
        stream.close();
        
        String id = index.getVersionId(VersionIndex.dateFormatter.parseDateTime("2017-08-01T00:00:00Z"));
        Version version = index.getVersion(id);
		
		File testFile = new File(resourceDir + "PriceListTestData.json");
		stream = new FileInputStream(testFile);
        PriceList priceList = new PriceList(stream);
        stream.close();

       	InstancePrices prices = new InstancePrices(ServiceCode.AmazonEC2, id, version.getBeginDate(), version.getEndDate());
       	prices.importPriceList(priceList, PriceListService.tenancies);
       	
       	verify(prices.getPrices().entrySet().iterator().next().getValue());

       	ByteArrayOutputStream buf = new ByteArrayOutputStream();
       	DataOutputStream out = new DataOutputStream(buf);
       	InstancePrices.Serializer.serialize(out, prices);
        
        assertTrue("No output from writeObject", out.size() > 0);
        
        ByteArrayInputStream inBuf = new ByteArrayInputStream(buf.toByteArray());
        DataInputStream in = new DataInputStream(inBuf);
        InstancePrices ip = InstancePrices.Serializer.deserialize(in);
        
        assertNotEquals("No object returned from readObject", ip, null);
       	verify(ip.getPrices().entrySet().iterator().next().getValue());
	}
	
	private void verify(Product p) {
		assertEquals("OnDemand rate doesn't match, expected 0.023, got " + p.onDemandRate, p.onDemandRate, 0.023, 0.001);
		verifyRate(p, LeaseContractLength.oneyear, PurchaseOption.noUpfront, OfferingClass.standard, 0, 0.0168);
		verifyRate(p, LeaseContractLength.oneyear, PurchaseOption.partialUpfront, OfferingClass.standard, 70, 0.008);
		verifyRate(p, LeaseContractLength.oneyear, PurchaseOption.allUpfront, 	OfferingClass.standard, 137, 0);
		verifyRate(p, LeaseContractLength.threeyear, PurchaseOption.noUpfront, 		OfferingClass.standard, 0, 0.012);
		verifyRate(p, LeaseContractLength.threeyear, PurchaseOption.partialUpfront,	OfferingClass.standard, 145, 0.006);
		verifyRate(p, LeaseContractLength.threeyear, PurchaseOption.allUpfront,		OfferingClass.standard, 272, 0);
		verifyRate(p, LeaseContractLength.threeyear, PurchaseOption.noUpfront,		OfferingClass.convertible, 0, 0.0139);
		verifyRate(p, LeaseContractLength.threeyear, PurchaseOption.partialUpfront,	OfferingClass.convertible, 169, 0.0064);
		verifyRate(p, LeaseContractLength.threeyear, PurchaseOption.allUpfront,		OfferingClass.convertible, 332, 0);
	}
	
	private void verifyRate(Product p, LeaseContractLength lcl, PurchaseOption po, OfferingClass oc, double fixed, double hourly) {
		RateKey rateKey = new RateKey(lcl, po, oc);
		Rate rate = p.reservationRates.get(rateKey);
		assertNotEquals("No rate for " + rateKey, rate, null);
		assertEquals("Reservation fixed rate for " + rateKey + " doesn't match, expected " + fixed + ", got " + rate.fixed, rate.fixed, fixed, 0.001);
		assertEquals("Reservation hourly rate for " + rateKey + " doesn't match, expected " + hourly + ", got " + rate.hourly, rate.hourly, hourly, 0.001);		
	}

	@Test
	public void testImportCurrentEc2PriceList() throws Exception {
		
		priceListService.getPrices(DateTime.now(), ServiceCode.AmazonEC2);
		
		// Spot check some instance metrics
		InstanceMetrics im = priceListService.getInstanceMetrics();
		assertEquals("m1.small should have normalization of 1", 1.0, im.getNormalizationFactor(UsageType.getUsageType("m1.small", "hours")), 0.1);
		assertEquals("m1.xlarge should have normalization of 8", 8.0, im.getNormalizationFactor(UsageType.getUsageType("m1.xlarge", "hours")), 0.1);
	}
	@Test
	public void testImportCurrentRdsPriceList() throws Exception {
		
		priceListService.getPrices(DateTime.now(), ServiceCode.AmazonRDS);
	}
	@Test
	public void testImportCurrentRedshiftPriceList() throws Exception {
		
		priceListService.getPrices(DateTime.now(), ServiceCode.AmazonRedshift);
	}
	
	@Test
	public void testInit() throws Exception {
		priceListService.init();
	}
}
