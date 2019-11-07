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
package com.netflix.ice.tag;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

public class Account extends Tag {
	private static final long serialVersionUID = 1L;
	
	// Account ID goes into the parent name since it's immutable. Hide the value so it can't be accessed directly
	// All other values associated with the account can be modified in the AWS Organizations service,
	// so we can't make them final.
	private String iceName; // Name assigned to account for display in the dashboards
	private String awsName; // Name of account returned by Organizations
	private List<String> parents; // parent organizational units as defined by the Organizations service
	private String status;  // status as returned by the Organizations service
	private Map<String, String> tags;

    public Account(String accountId, String accountName, List<String> parents) {
        super(accountId);
        this.iceName = accountName;
        this.awsName = null;
        this.parents = parents;
        this.status = null;
        this.tags = null;
    }
    
    public Account(String accountId, String accountName, String awsName, List<String> parents, String status, Map<String, String> tags) {
    	super(accountId);
        this.iceName = StringUtils.isEmpty(accountName) ? awsName : accountName;
        this.awsName = awsName;
        this.parents = parents;
        this.status = status;
        this.tags = tags;
    }
    
    public void update(Account a) {
		this.iceName = a.iceName;
		this.awsName = a.awsName;
		this.parents = a.parents;
		this.status = a.status;
		this.tags = a.tags;
    }
    
    @Override
    public String toString() {
        return this.iceName;
    }

    @Override
    public int compareTo(Tag t) {
        if (t == aggregated)
            return -t.compareTo(this);
        int result = ("a" + this.iceName).compareTo("a" + ((Account) t).iceName);
        return result;
    }

    public String getId() {
    	return super.name;
    }

	public String getIceName() {
		return iceName;
	}

	public String getAwsName() {
		return awsName;
	}

	public List<String> getParents() {
		return parents;
	}

	public String getStatus() {
		return status;
	}

	public Map<String, String> getTags() {
		return tags;
	}
}
