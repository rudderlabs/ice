package com.netflix.ice.processor;

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
            
 *  
 */
public class BillingDataConfig {
    public List<AccountConfig> accounts;
    public List<TagConfig> tags;

    public BillingDataConfig() {    	
    }
    
	public BillingDataConfig(List<AccountConfig> accounts, List<TagConfig> tags) {
		this.accounts = accounts;
		this.tags = tags;
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
	
}
