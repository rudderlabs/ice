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
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.Maps;

public class OrganizationalUnit extends Tag {
	private static final long serialVersionUID = 1L;
	public static final String none = "(none)";

	private static ConcurrentMap<String, OrganizationalUnit> orgUnits = Maps.newConcurrentMap();

	private OrganizationalUnit(String name) {
		super(name);
	}
	
	public static OrganizationalUnit get(List<String> parents) {
		if (parents == null)
			return null;
		String name = String.join("/", parents);
		
		return get(name);
	}
	
	public static OrganizationalUnit get(String name) {
		if (name == null)
			return null;
		
		OrganizationalUnit ou = orgUnits.get(name);
		if (ou == null) {
	       	orgUnits.putIfAbsent(name, new OrganizationalUnit(name));
	       	ou = orgUnits.get(name);
       }
       return ou;
	}
}
