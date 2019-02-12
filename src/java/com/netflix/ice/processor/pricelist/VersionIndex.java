package com.netflix.ice.processor.pricelist;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * AWS Price List Version Index
 * 
 * VersionIndex reads a JSON price list version index from the supplied input stream
 * and loads the data into its internal structure.
 * 
 * Amazon typically issues new price lists at the beginning of a month and will be
 * valid for at least one month. On occasion, the index will have entries that end
 * one second before the start of the next month and another entry that lasts for 
 * only the final second in the month. We want to be sure to ignore these.
 * 
 * For example:
 * 
 *   "20161213014831" : {
 *     "versionEffectiveBeginDate" : "2016-12-01T00:00:00Z",
 *     "versionEffectiveEndDate" : "2017-01-31T23:59:59Z",
 *     "offerVersionUrl" : "/offers/v1.0/aws/AmazonEC2/20161213014831/index.json"
 *   },
 *   "20170210223144" : {
 *     "versionEffectiveBeginDate" : "2017-01-31T23:59:59Z",
 *     "versionEffectiveEndDate" : "2017-02-01T00:00:00Z",
 *     "offerVersionUrl" : "/offers/v1.0/aws/AmazonEC2/20170210223144/index.json"
 *   },
 *   "20170224022054" : {
 *     "versionEffectiveBeginDate" : "2017-02-01T00:00:00Z",
 *     "versionEffectiveEndDate" : "2017-02-28T23:59:59Z",
 *     "offerVersionUrl" : "/offers/v1.0/aws/AmazonEC2/20170224022054/index.json"
 *   },
 *   "20170302183221" : {
 *     "versionEffectiveBeginDate" : "2017-02-28T23:59:59Z",
 *     "versionEffectiveEndDate" : "2017-03-01T00:00:00Z",
 *     "offerVersionUrl" : "/offers/v1.0/aws/AmazonEC2/20170302183221/index.json"
 *   },
 *   "20170324211955" : {
 *     "versionEffectiveBeginDate" : "2017-03-01T00:00:00Z",
 *     "versionEffectiveEndDate" : "",
 *     "offerVersionUrl" : "/offers/v1.0/aws/AmazonEC2/20170324211955/index.json"
 *   },
 * 
 * @author jaroth
 *
 */
public class VersionIndex {
	public static final DateTimeFormatter dateFormatter = ISODateTimeFormat.dateTimeParser().withZone(DateTimeZone.UTC);
	
	Root index;
	
	protected VersionIndex() {
		// Used for testing
	}

	public VersionIndex(InputStream in) throws IOException {
        Reader reader = null;
		reader = new BufferedReader(new InputStreamReader(in));
        Gson gson = new GsonBuilder().create();
	    this.index = gson.fromJson(reader, Root.class);
		reader.close();
	}
	
	public String getVersionId(DateTime time) {		
		for (String k: index.versions.keySet()) {
			Version v = index.versions.get(k);
			if (time.isBefore(v.getBeginDate()))
				continue;
			if (!v.hasNoEnd()) {
				DateTime end = v.getEndDate();
				if (time.equals(end) || time.isAfter(end))
					continue;
			}
			return k;
		}
		return null;
	}
	
	public Version getVersion(String version) {
		return index.versions.get(version);
	}

	public class Root {
		Index.Header header;
		String currentVersion;
		HashMap<String, Version> versions;
	}
	
	public class Version {
		String versionEffectiveBeginDate;
		String versionEffectiveEndDate;
		String offerVersionUrl;
		
		public DateTime getBeginDate() {
			return dateFormatter.parseDateTime(versionEffectiveBeginDate);
		}
		public DateTime getEndDate() {
			return versionEffectiveEndDate.isEmpty() ? null : dateFormatter.parseDateTime(versionEffectiveEndDate);
		}
		public boolean hasNoEnd() {
			return versionEffectiveEndDate.isEmpty();
		}
	}
}
