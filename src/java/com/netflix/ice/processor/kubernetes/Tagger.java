package com.netflix.ice.processor.kubernetes;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
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
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.JsonDeserializer;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.processor.kubernetes.KubernetesReport.KubernetesColumn;
import com.netflix.ice.tag.UserTag;

public class Tagger {
    protected Logger logger = LoggerFactory.getLogger(getClass());
	private static final String taggerConfigFilename = "k8s_tagger_config.json";
	
	private final ResourceService resourceService;
	private final int numCustomTags;
	protected final TaggerConfig config;
	
	class TaggerConfig {
		private String[] tagsToCopy;
		private List<Rule> rules;
		
		TaggerConfig(String[] tagsToCopy, Map<String, String> rulesFromProperties, ResourceService resourceService) {
			this.tagsToCopy = tagsToCopy;
			rules = Lists.newArrayList();
			for (String nameValuePair: rulesFromProperties.keySet()) {
				String[] parts = nameValuePair.split("\\.");
				this.rules.add(new Rule(parts[0], parts[1], rulesFromProperties.get(nameValuePair)));
			}			
		}
		
		TaggerConfig(String json) {
			Gson gson = new GsonBuilder().registerTypeAdapter(Rule.class, new JsonRuleDeserializer()).create();
			TaggerConfig c = gson.fromJson(json, this.getClass());
			this.rules = c.rules;
			this.tagsToCopy = c.tagsToCopy;
		}
		
		public String toJSON() {
			Gson gson = new GsonBuilder().registerTypeAdapter(Rule.class, new JsonRuleSerializer()).create();
	    	return gson.toJson(this);
		}
		
		public String[] getTagsToCopy() {
			return tagsToCopy;
		}
		
		public List<Rule> getRules() {
			return rules;
		}
	}
	
	public Tagger(String[] tagsToCopy, Map<String, String> rules, ResourceService resourceService, String kubernetesBucketName, String region, String kubernetesBucketPrefix, String localDir,
			String accountId, String roleName, String externalId, String workBucketName, String workBucketPrefix) throws IOException {
		this.resourceService = resourceService;
		this.numCustomTags = resourceService.getCustomTags().length;
		
		TaggerConfig config = getConfigFromKubernetesBucket(kubernetesBucketName, region, kubernetesBucketPrefix, localDir, accountId, roleName, externalId);
		if (config == null) {
			logger.info("Could not read existing configuration for Kubernetes Tagger in " + kubernetesBucketName + "/" + kubernetesBucketPrefix + ", creating config from properties file");
			config = new TaggerConfig(tagsToCopy, rules, resourceService);
		}
		writeConfigToWorkBucket(config, workBucketName, workBucketPrefix, localDir, kubernetesBucketName, kubernetesBucketPrefix);
		this.config = config;
	}
	
	protected TaggerConfig getConfigFromKubernetesBucket(String kubernetesBucketName, String region, String kubernetesBucketPrefix, String localDir, String accountId, String roleName, String externalId) {
		if (localDir == null)
			return null;
		
		// See if we have a a config in the kubernetes bucket
    	File file = new File(localDir, taggerConfigFilename);
		boolean downloaded = AwsUtils.downloadFileIfChangedSince(kubernetesBucketName, region, kubernetesBucketPrefix, file, 0, accountId, roleName, externalId);
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
			try {
				return new TaggerConfig(json);
			}
			catch (Exception e) {
				logger.error("Error parsing json file " + file.getName());
				return null;
			}
    	}
    	return null;
	}
	
	protected void writeConfigToWorkBucket(TaggerConfig config, String workBucketName, String workBucketPrefix, String localDir, String bucketName, String bucketPrefix) throws IOException {
		if (localDir == null)
			return;
		
		String filename = bucketName + (bucketPrefix.isEmpty() ? "" : "_" + bucketPrefix.replace("/", "_")) + "_" + taggerConfigFilename;
        File file = new File(localDir, filename);
    	OutputStream os = new FileOutputStream(file);
    	OutputStreamWriter writer = new OutputStreamWriter(os);
    	writer.write(config.toJSON());
    	writer.close();
    	
    	logger.info("Upload kubernetes tagging config to work bucket");
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
		private final String tagName;		
		private final UserTag value;		
		private final String patterns;
		
		private final int tagIndex;
		private final List<Pattern> compiledPatterns;

		
		Rule(String tagName, String value, String patterns) {
			this.tagName = tagName;
			this.tagIndex = resourceService.getUserTagIndex(tagName);
			this.value = UserTag.get(value);
			this.patterns = patterns;
			String[] patternStrings = patterns.split(",");
			this.compiledPatterns = Lists.newArrayList();
			for (String p: patternStrings) {
				if (p.isEmpty())
					continue;
				this.compiledPatterns.add(Pattern.compile(p));
			}
		}
		
		public boolean matches(String namespace) {
			for (Pattern p: compiledPatterns) {
				Matcher m = p.matcher(namespace);
				if (m.matches())
					return true;
			}
			return false;
		}
		
		public String getTagName() {
			return tagName;
		}
		
		public int getTagIndex() {
			return tagIndex;
		}
		
		public UserTag getValue() {
			return value;
		}
		
		public String getPatterns() {
			return patterns;
		}
	}
	
	class JsonRuleSerializer implements JsonSerializer<Rule> {

		@Override
		public JsonElement serialize(Rule rule, Type typeOfT,
				JsonSerializationContext context) {
			JsonObject json = new JsonObject();
			json.addProperty("tagName", rule.tagName);
			json.addProperty("value", rule.value.name);
			json.addProperty("patterns", rule.patterns);
			return json;
		}
		
	}
	
	class JsonRuleDeserializer implements JsonDeserializer<Rule> {

		@Override
		public Rule deserialize(JsonElement json, Type typeOfT,
				JsonDeserializationContext context) throws JsonParseException {
			JsonObject jsonObject = json.getAsJsonObject();
			Rule rule = new Rule(
					jsonObject.get("tagName").getAsString(),
					jsonObject.get("value").getAsString(),
					jsonObject.get("patterns").getAsString());
			return rule;
		}
		
	}
}
