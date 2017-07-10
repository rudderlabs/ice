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

public class FamilyTag extends Tag {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public FamilyTag(String name) {
		super(getFamilyName(name));
	}
	
	protected static String getFamilyName(String name) {
		String[] tokens = name.split("\\.");
		int endStartIndex = 2;
		StringBuilder family = new StringBuilder();
		family.append(tokens[0]);
		
		if (tokens[0].equals("db")) {
			family.append("." + tokens[1]);
			endStartIndex++;
		}
		for (int i = endStartIndex; i < tokens.length; i++)
			family.append("." + tokens[i]);
		
		return family.toString();		
	}
}
