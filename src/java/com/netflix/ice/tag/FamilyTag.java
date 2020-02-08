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
	private static final long serialVersionUID = 1L;

	public FamilyTag(String name) {
		super(getFamilyName(name));
	}
	
	public static String getFamilyName(String name) {
		if (name.contains(".")) {
			String[] tokens = name.split("\\.");
			if (tokens[0].equals("db") && tokens.length >= 4) {
				// RDS instance
				StringBuilder family = new StringBuilder(32);
				family.append(tokens[0]);			
				family.append("." + tokens[1]);
				
				int endStartIndex = 3;
				if (tokens[endStartIndex].equals("multiaz"))			
					endStartIndex++;
				
				InstanceDb db = InstanceDb.valueOf(tokens[endStartIndex]);
				if (!db.familySharing) {
					// Don't consolidate if it doesn't support family sharing
					return name;
				}
				
				for (int i = endStartIndex; i < tokens.length; i++)
					family.append("." + tokens[i]);
				
				return family.toString();		
			}
			else if (tokens.length > 2) {
				// Not a Linux EC2 instance, so don't consolidate
				return name;
			}
			// EC2 Linux
			return tokens[0];
		}
		else if (name.contains("-")) {
	        int index = name.indexOf("-");
	        if (index == 2 && Region.cloudFrontRegions.contains(name.substring(0, 2))) {
	        	return name.substring(index+1);
	        }
		}
		return name;
	}
}
