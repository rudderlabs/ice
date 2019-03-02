package com.netflix.ice.processor.kubernetes;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.processor.kubernetes.KubernetesReport.KubernetesColumn;
import com.netflix.ice.tag.UserTag;

public class Tagger {
    protected Logger logger = LoggerFactory.getLogger(getClass());
	private static final String taggerConfigFilename = "k8s_tagger_config.json";
	
	private final ResourceService resourceService;
	private final int numCustomTags;
	private final TaggerConfig config;
	
	class TaggerConfig {
		private String[] tagsToCopy;
		private List<Rule> rules;
		
		TaggerConfig(String[] tagsToCopy, Map<String, String> rulesFromProperties, ResourceService resourceService) {
			this.tagsToCopy = tagsToCopy;
			rules = Lists.newArrayList();
			for (String nameValuePair: rulesFromProperties.keySet()) {
				String[] parts = nameValuePair.split("\\.");
				int tagIndex = resourceService.getUserTagIndex(parts[0]);
				this.rules.add(new Rule(tagIndex, parts[1], rulesFromProperties.get(nameValuePair)));
			}			
		}
		
		TaggerConfig(String json) {
			Gson gson = new Gson();
			TaggerConfig c = gson.fromJson(json, this.getClass());
			this.rules = c.rules;
			this.tagsToCopy = c.tagsToCopy;
		}
		
		public String toJSON() {
			Gson gson = new Gson();
	    	return gson.toJson(this);
		}
		
		public String[] getTagsToCopy() {
			return tagsToCopy;
		}
		
		public List<Rule> getRules() {
			return rules;
		}
	}

	public Tagger(String[] tagsToCopy, Map<String, String> rules, ResourceService resourceService, String workBucketName, String workBucketPrefix, String localDir) throws IOException {
		this.resourceService = resourceService;
		this.numCustomTags = resourceService.getCustomTags().length;
		
		TaggerConfig config = getConfigFromWorkBucket(workBucketName, workBucketPrefix, localDir);
		if (config == null) {
			config = new TaggerConfig(tagsToCopy, rules, resourceService);
			writeConfigToWorkBucket(config, workBucketName, workBucketPrefix, localDir);
		}
		this.config = config;
	}
	
	protected TaggerConfig getConfigFromWorkBucket(String workBucketName, String workBucketPrefix, String localDir) {
		if (localDir == null)
			return null;
		
		// See if we have a a config in the work bucket
    	File file = new File(localDir, taggerConfigFilename);
    	file.delete(); // Delete if it exists so we get a fresh copy from S3 each time
		boolean downloaded = AwsUtils.downloadFileIfNotExist(workBucketName, workBucketPrefix, file);
    	if (downloaded) {
        	String json;
			try {
				json = new String(Files.readAllBytes(file.toPath()), "UTF-8");
			}
	        catch (AmazonS3Exception e) {
	            if (e.getStatusCode() == 404)
	            	logger.warn("File not found in s3: " + file.getName());
	            else
	            	logger.error("Error reading from file " + file.getName(), e);
	            return null;
	        }
			catch (IOException e) {
				logger.error("Error reading from file " + file.getName(), e);
				return null;
			}
        	return new TaggerConfig(json);    	
    	}
    	return null;
	}
	
	protected void writeConfigToWorkBucket(TaggerConfig config, String workBucketName, String workBucketPrefix, String localDir) throws IOException {
		if (localDir == null)
			return;
		
        File file = new File(localDir, taggerConfigFilename);
    	OutputStream os = new FileOutputStream(file);
    	OutputStreamWriter writer = new OutputStreamWriter(os);
    	writer.write(config.toJSON());
    	writer.close();
    	
    	logger.info("Upload work bucket data config file");
    	AwsUtils.upload(workBucketName, workBucketPrefix, file);
	}

	public void tag(KubernetesReport report, String[] item, UserTag[] userTags) {
		String namespace = report.getString(item, KubernetesColumn.Namespace);
		
		UserTag[] overlay = new UserTag[numCustomTags];
		String[] tagsToCopy = config.getTagsToCopy();
		if (tagsToCopy != null) {
			// Copy the requested tags if not empty
			for (String t: tagsToCopy) {
				String value = report.getUserTag(item, t);
				if (value.isEmpty())
					continue;
				overlay[resourceService.getUserTagIndex(t)] = UserTag.get(value);
			}
		}
		for (Rule r: config.getRules()) {
			if (r.matches(namespace) && (overlay[r.getTagIndex()] == null || overlay[r.getTagIndex()].name.isEmpty())) {
				userTags[r.getTagIndex()] = r.getValue();
			}
		}
		// Overwrite tags that have values
		for (int i = 0; i < numCustomTags; i++) {
			if (overlay[i] != null)
				userTags[i] = overlay[i];
		}
	}
	
	class Rule {
		private final int tagIndex;
		private final UserTag value;
		private final List<Pattern> patterns;
		
		Rule(int tagIndex, String value, String patterns) {
			this.tagIndex = tagIndex;
			this.value = UserTag.get(value);
			String[] patternStrings = patterns.split(",");
			this.patterns = Lists.newArrayList();
			for (String p: patternStrings) {
				if (p.isEmpty())
					continue;
				this.patterns.add(Pattern.compile(p));
			}
		}
		
		public boolean matches(String namespace) {
			for (Pattern p: patterns) {
				Matcher m = p.matcher(namespace);
				if (m.matches())
					return true;
			}
			return false;
		}
		
		public int getTagIndex() {
			return tagIndex;
		}
		
		public UserTag getValue() {
			return value;
		}
	}
}
