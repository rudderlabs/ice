package com.netflix.ice.processor.pricelist;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

import org.junit.Test;

import com.netflix.ice.processor.pricelist.Index.Offer;

public class IndexTest {
	private static final String domain = "https://pricing.us-east-1.amazonaws.com";
	private static final String priceListIndexUrl = "/offers/v1.0/aws/index.json";

	@Test
	public void testIndex() throws MalformedURLException, IOException {
		InputStream stream = new URL(domain + priceListIndexUrl).openStream();
        Index index = new Index(stream);
        stream.close();
        Offer offer = index.getOffer("AmazonEC2");
        String indexURL = offer.versionIndexUrl;
        assertFalse("Price List Index has empty URL", indexURL.isEmpty());
        assertTrue("Unexpected URL result", indexURL.startsWith("/offers/v1.0/aws/AmazonEC2/"));
	}

}
