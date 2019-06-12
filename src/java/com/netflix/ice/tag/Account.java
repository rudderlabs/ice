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

import org.apache.commons.lang.StringUtils;

public class Account extends Tag {
	private static final long serialVersionUID = 1L;
	
	public final String id;
	public final String awsName; /* Name of account returned by Organizations */

    public Account(String accountId, String accountName) {
        super(accountName);
        this.id = accountId;
        this.awsName = null;
    }
    
    public Account(String accountId, String accountName, String awsName) {
        super(StringUtils.isEmpty(accountName) ? awsName : accountName);
        this.id = accountId;
        this.awsName = awsName;
    }
}
