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
package com.netflix.ice.processor.config;

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.gson.Gson;
import com.netflix.ice.common.TagConfig;
import com.netflix.ice.processor.postproc.RuleConfig;

/*
 *  BillingDataConfig loads and holds AWS account name and default tagging configuration data
 *  that is specified in S3 along side the billing data reports.
 *
 *  Example BillingDataConfig JSON:
 *

{
	"accounts": [
		{
			"id": "123456789012",
			"name": "act1",
			"parents": [ "root", "ou" ],
			"tags": {
				"TagName": "tag-value"
			},
			"riProducts": [ "ec2", "rds" ],
			"role": "ice",
			"externalId": ""
		}
	],
	"tags":[
		{
			"name": "Environment",
			"aliases": [ "env" ],
			"values": {
				"Prod": [ "production", "prd" ]
			}
		}
	]
}
  
 *
 *  Example BillingDataConfig YAML:
 *  

accounts:
  -  id: 123456789012
	 name: act1
	 parents: [root, ou]
	 tags:
	   TagName: tag-value
	 riProducts: [ec2, rds]
	 role: ice
	 externalId:
	 
tags:
  - name: Environment
    aliases: [env]
    values:
      Prod: [production, prd]
      
kubernetes:
  - bucket: k8s-report-bucket
    prefix: hourly/kubernetes
    clusterNameFormulae: [ 'Cluster.toLower()', 'Cluster.regex("k8s-(.*)")' ]
    computeTag: Role
    computeValue: compute
    namespaceTag: K8sNamespace
    namespaceMappings:
      - tag: Environment
        value: Prod
        patterns: [ ".*prod.*", ".*production.*", ".*prd.*" ]
    tags: [ userTag1, userTag2 ]
postprocrules:
  - name: ComputedCost
    product: Product
    start: 2019-11
    end: 2022-11
    operands: 
      data:
        type: usage
		usageType: ${group}-DataTransfer-Out-Bytes
    in:
      type: usage
      product: Product
      usageType: (..)-Requests-[12].*
    results:
      - result:
        type: cost
        product: ComputedCost
        usageType: ${group}-Requests
        value: '(${in} - (${data} * 4 * 8 / 2)) * 0.01 / 1000'
      - result:
        type: usage
        product: ComputedCost
        usageType: ${group}-Requests
        value: '${in} - (${data} * 4 * 8 / 2)'
 *  
 */
public class BillingDataConfig {
	public List<AccountConfig> accounts;
    public List<TagConfig> tags;
    public List<KubernetesConfig> kubernetes;
    public List<RuleConfig> postprocrules;

    public BillingDataConfig() {    	
    }
    
	public BillingDataConfig(List<AccountConfig> accounts, List<TagConfig> tags, List<KubernetesConfig> kubernetes, List<RuleConfig> ruleConfigs) {
		this.accounts = accounts;
		this.tags = tags;
		this.kubernetes = kubernetes;
		this.postprocrules = ruleConfigs;
	}

	public BillingDataConfig(String in) throws JsonParseException, JsonMappingException, IOException {
		BillingDataConfig config = null;
		
		if (in.trim().startsWith("{")) {
			Gson gson = new Gson();
			config = gson.fromJson(in, getClass());
		}
		else {
			ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			config = mapper.readValue(in, getClass());			
		}
		this.accounts = config.accounts;
		this.tags = config.tags;
		this.kubernetes = config.kubernetes;
		this.postprocrules = config.postprocrules;
	}
	
	public String toJSON() {
		Gson gson = new Gson();
    	return gson.toJson(this);
	}
	
	public List<AccountConfig> getAccounts() {
		return accounts;
	}

	public void setAccounts(List<AccountConfig> accounts) {
		this.accounts = accounts;
	}

	public List<TagConfig> getTags() {
		return tags;
	}

	public void setTags(List<TagConfig> tags) {
		this.tags = tags;
	}

	public List<KubernetesConfig> getKubernetes() {
		return kubernetes;
	}

	public void setKubernetes(List<KubernetesConfig> kubernetes) {
		this.kubernetes = kubernetes;
	}

	public List<RuleConfig> getPostprocrules() {
		return postprocrules;
	}

	public void setPostprocrules(List<RuleConfig> postprocrules) {
		this.postprocrules = postprocrules;
	}
	
}
