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

import org.apache.commons.lang.StringUtils;

public class Account extends Tag {
	private static final long serialVersionUID = 1L;
	
	public final String id;
	public final String awsName; // Name of account returned by Organizations
	public final List<String> parents; // parent organizational units as defined by the Organizations service
	public final String status;  // status as returned by the Organizations service

    public Account(String accountId, String accountName, List<String> parents) {
        super(accountName);
        this.id = accountId;
        this.awsName = null;
        this.parents = parents;
        this.status = null;
    }
    
    public Account(String accountId, String accountName, String awsName, List<String> parents, String status) {
        super(StringUtils.isEmpty(accountName) ? awsName : accountName);
        this.id = accountId;
        this.awsName = awsName;
        this.parents = parents;
        this.status = status;
    }
}
