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
package com.netflix.ice.common;

import java.util.Map;

import com.google.common.collect.Maps;

public enum PurchaseOption {
	None("None"),
	NoUpfront("No Upfront"),
	PartialUpfront("Partial Upfront"),
	AllUpfront("All Upfront"),
	Heavy("Heavy Utilization"),
	Medium("Medium Utilization"),
	Light("Light Utilization");
	
	public final String name;
	
	private static Map<String, PurchaseOption> purchaseOptionByName = Maps.newHashMap();
	static {
		for (PurchaseOption po: PurchaseOption.values()) {
			purchaseOptionByName.put(po.name, po);
			purchaseOptionByName.put(po.name(), po);
		}
	}
	
	private PurchaseOption(String name) {
		this.name = name;
	}
	
	public static PurchaseOption get(String name) {
		return purchaseOptionByName.get(name);
	}
}

