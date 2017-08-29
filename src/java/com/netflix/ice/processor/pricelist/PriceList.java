package com.netflix.ice.processor.pricelist;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
/**
 * PriceList holds the data imported from an AWS price list:
 *   http://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/price-changes.html
 */
public class PriceList {
	Root priceList;

	public PriceList(InputStream in) throws IOException {
        Reader reader = null;
		reader = new BufferedReader(new InputStreamReader(in));
        Gson gson = new GsonBuilder().create();
	    this.priceList = gson.fromJson(reader, Root.class);
		reader.close();
	}
	
	public Map<String, Product> getProducts() {
		return priceList.products;
	}
	
	public Terms getTerms() {
		return priceList.terms;
	}
	
	public class Root {
		Index.Header header;
		String version;
		Map<String, Product> products;
		Terms terms;
	}
	
	static public class Product {
		String sku;
		String productFamily;
		private Map<String, String> attributes;
		
		public enum Attributes {
	        servicecode,
	        location,
	        locationType,
	        instanceType,
	        currentGeneration,
	        instanceFamily,
	        vcpu,
	        physicalProcessor,
	        clockSpeed,
	        memory,
	        storage,
	        networkPerformance,
	        processorArchitecture,
	        tenancy,
	        operatingSystem,
	        licenseModel,
	        usagetype,
	        operation,
	        ecu,
	        normalizationSizeFactor,
	        preInstalledSw,
	        processorFeatures,
	        deploymentOption;
		}

		public String getAttribute(Attributes a) {
			String v = attributes.get(a.toString());
			return v == null ? "" : v;
		}

	}
	
	public class Terms {
		Map<String, Map<String, Term>> OnDemand; // Key is SKU, second Key is SKU.OfferTermCode
		Map<String, Map<String, Term>> Reserved; // Key is SKU, second Key is SKU.OfferTermCode
	}
	
	public class Term {
		String offerTermCode;
		String sku;
		String effectiveDate;
		Map<String, Rate> priceDimensions;
		TermAttributes termAttributes;
	}
		
	public class Rate {
		String rateCode;
		String description;
		String beginRange;
		String endRange;
		String unit;
		Map<String, String> pricePerUnit; // Key is currency e.g. "USD"
		String[] appliesTo;
	}
	
	public class TermAttributes {
		String LeaseContractLength; // e.g. "1yr" or "3yr"
		String OfferingClass; // e.g. standard, convertible
		String PurchaseOption; // e.g. "All Upfront", "No Upfront", "Partial Upfront"
	}

}
