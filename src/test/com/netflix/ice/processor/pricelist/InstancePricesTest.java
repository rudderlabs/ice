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
package com.netflix.ice.processor.pricelist;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;

import com.netflix.ice.processor.pricelist.InstancePrices.LeaseContractLength;
import com.netflix.ice.processor.pricelist.InstancePrices.OfferingClass;
import com.netflix.ice.processor.pricelist.InstancePrices.PurchaseOption;
import com.netflix.ice.processor.pricelist.InstancePrices.Rate;
import com.netflix.ice.processor.pricelist.InstancePrices.ServiceCode;
import com.netflix.ice.processor.pricelist.VersionIndex.Version;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.UsageType;

public class InstancePricesTest {
	private static final String resourceDir = "src/test/resources/";

	class TestCase {
		Region region;
		UsageType usageType;
		LeaseContractLength leaseContractLength;
		PurchaseOption purchaseOption;
		OfferingClass offeringClass;
		
		TestCase(Region r, String ut, LeaseContractLength lcl, PurchaseOption po, OfferingClass oc) {
			region = r;
			usageType = UsageType.getUsageType(ut, "hours");
			leaseContractLength = lcl;
			purchaseOption = po;
			offeringClass = oc;			
		}
		
		void runOnDemand(InstancePrices prices) {
			double od = prices.getOnDemandRate(region, usageType);
			assertNotEquals("No on-demand rate for " + region + ", " + usageType, od, 0, 0.001);
		}
		
		void runReservation(InstancePrices prices) {
			Rate rsv = prices.getReservationRate(region, usageType, leaseContractLength, purchaseOption, offeringClass);
			assertNotEquals("No reservation rate for " + this, rsv, null);
		}
		
		public String toString() {
			return "{" + region + ", " + usageType + ", " + leaseContractLength + ", " + purchaseOption + ", " + offeringClass + "}";
		}
	}
	
	@Test
	public void test() throws IOException {
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
       	
       	TestCase[] testCases = new TestCase[] {
       			new TestCase(Region.US_EAST_1, "t2.small", LeaseContractLength.oneyear, PurchaseOption.allUpfront, OfferingClass.standard),
       			new TestCase(Region.US_EAST_1, "t2.small", LeaseContractLength.threeyear, PurchaseOption.allUpfront, OfferingClass.standard),
       			new TestCase(Region.US_EAST_1, "t2.small", LeaseContractLength.threeyear, PurchaseOption.allUpfront, OfferingClass.convertible),
       			new TestCase(Region.AP_SOUTHEAST_1, "c5.9xlarge.linsqlent", LeaseContractLength.oneyear, PurchaseOption.allUpfront, OfferingClass.standard),
       	};
       	
       	for (TestCase tc: testCases) {
       		tc.runOnDemand(prices);
       		tc.runReservation(prices);
       	}
       	
	}

}
