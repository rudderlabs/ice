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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

public class Zone extends Tag {
	private static final long serialVersionUID = 1L;
	public final Region region;

    private Zone(Region region, String name) {
        super(name);
        this.region = region;
    }
    private static ConcurrentMap<String, Zone> zonesByName = Maps.newConcurrentMap();
    
    // addZone should only be called by Region.addZone()
    protected static Zone addZone(Region region, String name) {
    	if (name.isEmpty() || name.equals(region.name))
    		return null;
    	
    	// Make sure we have a lower-case letter for the zone
        if (!region.name.equals(name.substring(0, name.length()-1)) || !Character.isLowerCase(name.charAt(name.length()-1))) {
        	logger.error("Attempt to add zone with invalid name to region " + region + ": " + name);
            return null;
        }
        
    	zonesByName.putIfAbsent(name, new Zone(region, name));
    	return zonesByName.get(name);
    }
        
    public static Collection<Zone> getZones() {
        return zonesByName.values();
    }

    public static List<Zone> getZones(List<String> names) {
        List<Zone> result = Lists.newArrayList();
        for (String name: names)
            result.add(zonesByName.get(name));
        return result;
    }
}

