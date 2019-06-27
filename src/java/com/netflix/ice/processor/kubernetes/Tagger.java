package com.netflix.ice.processor.kubernetes;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.processor.config.KubernetesNamespaceMapping;
import com.netflix.ice.processor.kubernetes.KubernetesReport.KubernetesColumn;
import com.netflix.ice.tag.UserTag;

public class Tagger {
    protected Logger logger = LoggerFactory.getLogger(getClass());
	
	private final ResourceService resourceService;
	private final int numCustomTags;
	private final List<Rule> rules;
	private final List<String> tagsToCopy;
		
	public Tagger(List<String> tagsToCopy, List<KubernetesNamespaceMapping> namespaceMappings, ResourceService resourceService) {
		this.resourceService = resourceService;
		this.numCustomTags = resourceService.getCustomTags() == null ? 0 : resourceService.getCustomTags().length;
		this.rules = Lists.newArrayList();
		if (namespaceMappings != null) {
			for (KubernetesNamespaceMapping m: namespaceMappings) {
				this.rules.add(new Rule(m));
			}
		}
		this.tagsToCopy = tagsToCopy;
	}
		
	public void tag(KubernetesReport report, String[] item, UserTag[] userTags) {
		String namespace = report.getString(item, KubernetesColumn.Namespace);
		
		UserTag[] overlay = new UserTag[numCustomTags];
		if (tagsToCopy != null) {
			// Copy the requested tags if not empty
			for (String t: tagsToCopy) {
				String value = report.getUserTag(item, t);
				if (value.isEmpty())
					continue;
				overlay[resourceService.getUserTagIndex(t)] = UserTag.get(value);
			}
		}
		for (Rule r: rules) {
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
		private final KubernetesNamespaceMapping mapping;		
		private final UserTag value;
		private final int tagIndex;
		private final List<Pattern> compiledPatterns;

		
		Rule(KubernetesNamespaceMapping mapping) {
			this.mapping = mapping;
			this.tagIndex = resourceService.getUserTagIndex(mapping.getTag());
			this.value = UserTag.get(mapping.getValue());
			this.compiledPatterns = Lists.newArrayList();
			for (String p: mapping.getPatterns()) {
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
			return mapping.getTag();
		}
		
		public int getTagIndex() {
			return tagIndex;
		}
		
		public UserTag getValue() {
			return value;
		}
	}
	
}
