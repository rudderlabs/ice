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
import java.net.MalformedURLException;
import org.junit.Test;

public class VersionIndexTest {
	private static final String resourceDir = "src/test/resources/";

	@Test
	public void testVersionIndex() throws MalformedURLException, IOException {
		File testFile = new File(resourceDir + "VersionIndex.json");
		InputStream stream = new FileInputStream(testFile);
        VersionIndex index = new VersionIndex(stream);
        stream.close();
        
        String version = index.getVersionId(VersionIndex.dateFormatter.parseDateTime("2017-02-01T00:00:00Z"));
        String priceListUrl = index.getVersion(version).offerVersionUrl;
        assertTrue("Unexpected URL result", priceListUrl.equals("/offers/v1.0/aws/AmazonEC2/20170224022054/index.json"));
        
        version = index.getVersionId(VersionIndex.dateFormatter.parseDateTime("2017-03-01T00:00:00Z"));
        priceListUrl = index.getVersion(version).offerVersionUrl;
        assertTrue("Unexpected URL result", priceListUrl.equals("/offers/v1.0/aws/AmazonEC2/20170324211955/index.json"));
	}
}
