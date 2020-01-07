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

import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class SavingsPlanArn extends Tag {
	private static final long serialVersionUID = 1L;
    protected static Logger logger = LoggerFactory.getLogger(SavingsPlanArn.class);

    private static ConcurrentMap<String, SavingsPlanArn> tagsByName = Maps.newConcurrentMap();

	private SavingsPlanArn(String name) {
		super(name);
	}

	public static SavingsPlanArn get(String name) {
		if (name == null)
			return null;
		SavingsPlanArn tag = tagsByName.get(name);
        if (tag == null) {
        	tagsByName.putIfAbsent(name, new SavingsPlanArn(name));
        	tag = tagsByName.get(name);
        }
        return tag;
	}
	
	public enum ArnParts {
		arn(0),
		aws(1),
		product(2),
		region(3),
		account(4),
		id(5);
		
		private int index;
		
		ArnParts(int index) {
			this.index = index;
		}
		
		public int getIndex() {
			return index;
		}
	}
	
	public String getAccountId() {
		return name.split(":")[ArnParts.account.getIndex()];
	}
}
