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

import java.util.Map;

import com.google.common.collect.Maps;

public class ConsolidatedOperation extends Tag {
	private static final long serialVersionUID = 1L;
	private static Map<String, String> consolidatedName = Maps.newHashMap();
	private static Map<String, Integer> seqMap = Maps.newHashMap();
	
	private int seq = Integer.MAX_VALUE;
	
	static {
		consolidatedName.put("Amortized RIs", "Amortized RIs");
		consolidatedName.put("Bonus RIs", "RIs");
		consolidatedName.put("Borrowed RIs", "RIs");
		consolidatedName.put("Lent RIs", "Lent RIs");
		consolidatedName.put("Savings", "Savings");
		consolidatedName.put("Unused RIs", "Unused RIs");
		consolidatedName.put("Used RIs", "RIs");
		consolidatedName.put("Spot Instances", "Spot Instances");
		consolidatedName.put("On-Demand Instances", "On-Demand Instances");
		
		seqMap.put("Savings", 0);
		seqMap.put("Spot Instances", 1);
		seqMap.put("On-Demand Instances", 2);
		seqMap.put("RIs", 3);
		seqMap.put("Amortized RIs", 4);
		seqMap.put("Unused RIs", 5);
		seqMap.put("Lent RIs", 7);
	}

	public ConsolidatedOperation(String name) {
		super(getConsolidatedName(name));
		if (seqMap.get(this.name) != null) {
			seq = seqMap.get(this.name);
		}
	}
	
	public static String getConsolidatedName(String name) {
		String prefix = name.split("-")[0].trim();
		String retval = consolidatedName.get(prefix);
		return retval == null ? name : retval;
	}
	
	protected Integer getSeq() {
		return seq;
	}

    @Override
    public int compareTo(Tag t) {
        if (t instanceof ConsolidatedOperation) {
	    	ConsolidatedOperation o = (ConsolidatedOperation)t;
	        int result = this.seq - o.seq;
	        return result == 0 ? this.name.compareTo(t.name) : result;
        }
        else {
        	return super.compareTo(t);
        }
    }

}
