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
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.gson.Gson;

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
            
 *  
 */
public class BillingDataConfig {
	public List<AccountConfig> accounts;
    public List<TagConfig> tags;
    public List<KubernetesConfig> kubernetes;

    public BillingDataConfig() {    	
    }
    
	public BillingDataConfig(List<AccountConfig> accounts, List<TagConfig> tags, List<KubernetesConfig> kubernetes) {
		this.accounts = accounts;
		this.tags = tags;
		this.kubernetes = kubernetes;
	}

	public BillingDataConfig(String in) throws JsonParseException, JsonMappingException, IOException {
		BillingDataConfig config = null;
		
		if (in.trim().startsWith("{")) {
			Gson gson = new Gson();
			config = gson.fromJson(in, getClass());
		}
		else {
			ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
			config = mapper.readValue(in, getClass());			
		}
		this.accounts = config.accounts;
		this.tags = config.tags;
		this.kubernetes = config.kubernetes;
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
	
}
