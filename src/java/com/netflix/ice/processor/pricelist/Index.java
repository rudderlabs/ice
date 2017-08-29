package com.netflix.ice.processor.pricelist;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Map;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class Index {
	private Root index;
	
	public Index(InputStream in) throws IOException {
        Reader reader = null;
		reader = new BufferedReader(new InputStreamReader(in));
        Gson gson = new GsonBuilder().create();
	    this.index = gson.fromJson(reader, Root.class);
		reader.close();
	}
	
	public Offer getOffer(String service) {
		return index.offers.get(service);
	}
	
	public class Root {
		Header header;
		Map<String, Offer> offers = Maps.newHashMap();
	}

	public class Header {
		String formatVersion;
		String disclaimer;
		String publicationDate;
		String offerCode;
	}
	
	public class Offer {
		String offerCode;
		String versionIndexUrl;
		String currentVersionUrl;
	}
}

