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
