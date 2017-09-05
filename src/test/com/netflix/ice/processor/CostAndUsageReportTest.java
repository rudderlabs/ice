package com.netflix.ice.processor;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;

import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.netflix.ice.common.LineItem;
import com.netflix.ice.processor.CostAndUsageReport.Manifest;

public class CostAndUsageReportTest {
    private static final String resourcesDir = "src/test/resources";

	@Test
	public void testReadManifest() {
		File manifest = new File(resourcesDir + "/manifestTest.json");
        Reader reader;
        
        try {
			reader = new BufferedReader(new FileReader(manifest));
	        Gson gson = new GsonBuilder().create();
		    gson.fromJson(reader, Manifest.class);
		} catch (FileNotFoundException e) {
			fail("Failed to parse manifest file" + e);
		}
	}
	
	@Test
	public void testGetCategoryHeader() {
		File file = new File(resourcesDir + "/manifestTest.json");
        Reader reader;
        Manifest manifest;
        try {
			reader = new BufferedReader(new FileReader(file));
	        Gson gson = new GsonBuilder().create();
		    manifest = gson.fromJson(reader, Manifest.class);
		} catch (FileNotFoundException e) {
			fail("Failed to parse manifest file" + e);
			return;
		}
        
        String[] expect = new String[]{
                "resourceTags/aws:createdBy",
                "resourceTags/user:Environment",
                "resourceTags/user:Email",
                "resourceTags/user:BusinessUnit",
                "resourceTags/user:Name",
                "resourceTags/user:Project",
                "resourceTags/user:Product",
        };
        String[] got = manifest.getCategoryHeader("resourceTags");
        assertEquals("Incorrect header length", expect.length, got.length);
        for (int i = 0; i < expect.length; i++)
        	assertTrue("Incorrect header at position " + i, expect[i].equals(got[i]));
        
	}
	
	@Test
	public void testISOTimeFormatter() {
		Long millis = LineItem.amazonBillingDateFormatISO.parseMillis("2017-07-01T00:00:00Z");
		Long expect = 1498867200000L;
		assertEquals("Error parsing time in ISO format", millis, expect);
	}
}
