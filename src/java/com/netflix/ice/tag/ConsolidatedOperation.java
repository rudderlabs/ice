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
		consolidatedName.put("BorrowedAmortized RIs", "Amortized RIs");
		consolidatedName.put("LentAmortized RIs", "Amortized RIs");
		consolidatedName.put("UnusedAmortized RIs", "Amortized RIs");
		consolidatedName.put("Bonus RIs", "RIs");
		consolidatedName.put("Borrowed RIs", "RIs");
		consolidatedName.put("Lent RIs", "RIs");
		consolidatedName.put("Savings", "Savings");
		consolidatedName.put("Savings RIs", "Savings");
		consolidatedName.put("Unused RIs", "Unused RIs");
		consolidatedName.put("Used RIs", "RIs");
		consolidatedName.put("Spot Instances", "Spot Instances");
		consolidatedName.put("On-Demand Instances", "On-Demand Instances");

		consolidatedName.put("SavingsPlan Savings", "SavingsPlan Savings");
		consolidatedName.put("SavingsPlan Amortized", "SavingsPlan Amortized");
		consolidatedName.put("SavingsPlan BorrowedAmortized", "SavingsPlan Amortized");
		consolidatedName.put("SavingsPlan LentAmortized", "SavingsPlan Amortized");
		consolidatedName.put("SavingsPlan UnusedAmortized", "SavingsPlan Amortized");
		consolidatedName.put("SavingsPlan Bonus", "SavingsPlan Used");
		consolidatedName.put("SavingsPlan Used", "SavingsPlan Used");
		consolidatedName.put("SavingsPlan Borrowed", "SavingsPlan Used");
		consolidatedName.put("SavingsPlan Lent", "SavingsPlan Used");
		consolidatedName.put("SavingsPlan Unused", "SavingsPlan Unused");

		seqMap.put("Savings", 0);
		seqMap.put("Spot Instances", 1);
		seqMap.put("On-Demand Instances", 2);
		seqMap.put("RIs", 3);
		seqMap.put("Amortized RIs", 4);
		seqMap.put("Unused RIs", 5);
		
		seqMap.put("SavingsPlan Savings", 6);
		seqMap.put("SavingsPlan Used", 7);
		seqMap.put("SavingsPlan Amortized", 8);
		seqMap.put("SavingsPlan Unused", 9);
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
	        return result == 0 ? this.getName().compareToIgnoreCase(t.getName()) : result;
        }
        else {
        	return super.compareTo(t);
        }
    }

}
