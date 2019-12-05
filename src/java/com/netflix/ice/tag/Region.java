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
import com.netflix.ice.tag.Zone.BadZone;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

public class Region extends Tag {
	private static final long serialVersionUID = 1L;
	
	public static final Region US_EAST_1 = new Region("us-east-1", "USE1", "US", "US East (N. Virginia)");
    public static final Region US_EAST_2 = new Region("us-east-2", "USE2", "US", "US East (Ohio)");
    public static final Region US_WEST_1 = new Region("us-west-1", "USW1", "US", "US West (N. California)");
    public static final Region US_WEST_2 = new Region("us-west-2", "USW2", "US", "US West (Oregon)");
    public static final Region US_WEST_2_LAX_1 = new Region("us-west-2-lax-1", "LAX1", "US", "US West (Los Angeles)");
    public static final Region CA_CENTRAL_1 = new Region("ca-central-1", "CAN1", "CA", "Canada (Central)");
    public static final Region EU_WEST_1 = new Region("eu-west-1", "EU", "EU", "EU (Ireland)");
    public static final Region EU_CENTRAL_1 = new Region("eu-central-1", "EUC1", "EU", "EU (Frankfurt)");
    public static final Region EU_WEST_2 = new Region("eu-west-2", "EUW2", "EU", "EU (London)");
    public static final Region EU_WEST_3 = new Region("eu-west-3", "EUW3", "EU", "EU (Paris)");
    public static final Region EU_NORTH_1 = new Region("eu-north-1", "EUN1", "EU", "EU (Stockholm)");
    public static final Region AP_EAST_1 = new Region("ap-east-1", "APE1", "AP", "Asia Pacific (Hong Kong)");
    public static final Region AP_NORTHEAST_1 = new Region("ap-northeast-1","APN1", "JP", "Asia Pacific (Tokyo)");
    public static final Region AP_NORTHEAST_2 = new Region("ap-northeast-2","APN2", "AP", "Asia Pacific (Seoul)");
    public static final Region AP_NORTHEAST_3 = new Region("ap-northeast-3","APN3", "AP", "Asia Pacific (Osaka-Local)");
    public static final Region AP_SOUTHEAST_1 = new Region("ap-southeast-1", "APS1", "AP", "Asia Pacific (Singapore)");
    public static final Region AP_SOUTHEAST_2 = new Region("ap-southeast-2", "APS2", "AU", "Asia Pacific (Sydney)");
    public static final Region AP_SOUTH_1 = new Region("ap-south-1", "APS3", "IN", "Asia Pacific (Mumbai)");
    public static final Region SA_EAST_1 = new Region("sa-east-1", "SAE1", "SA", "South America (Sao Paulo)");
    public static final Region ME_SOUTH_1 = new Region("me-south-1", "MES1", "ME", "Middle East (Bahrain)");

    private static ConcurrentMap<String, Region> regionsByName = Maps.newConcurrentMap();
    private static ConcurrentMap<String, Region> regionsByShortName = Maps.newConcurrentMap();

    static {
        regionsByShortName.put(US_EAST_1.shortName, US_EAST_1);
        regionsByShortName.put(US_EAST_2.shortName, US_EAST_2);
        regionsByShortName.put(US_WEST_1.shortName, US_WEST_1);
        regionsByShortName.put(US_WEST_2.shortName, US_WEST_2);
        regionsByShortName.put(US_WEST_2_LAX_1.shortName, US_WEST_2_LAX_1);
        regionsByShortName.put(CA_CENTRAL_1.shortName, CA_CENTRAL_1);
        regionsByShortName.put(EU_WEST_1.shortName, EU_WEST_1);
        regionsByShortName.put(EU_CENTRAL_1.shortName, EU_CENTRAL_1);
        regionsByShortName.put(EU_WEST_2.shortName, EU_WEST_2);
        regionsByShortName.put(EU_WEST_3.shortName, EU_WEST_3);
        regionsByShortName.put(EU_NORTH_1.shortName, EU_NORTH_1);
        regionsByShortName.put(AP_EAST_1.shortName, AP_EAST_1);
        regionsByShortName.put(AP_NORTHEAST_1.shortName, AP_NORTHEAST_1);
        regionsByShortName.put(AP_NORTHEAST_2.shortName, AP_NORTHEAST_2);
        regionsByShortName.put(AP_NORTHEAST_3.shortName, AP_NORTHEAST_3);
        regionsByShortName.put(AP_SOUTHEAST_1.shortName, AP_SOUTHEAST_1);
        regionsByShortName.put(AP_SOUTHEAST_2.shortName, AP_SOUTHEAST_2);
        regionsByShortName.put(AP_SOUTH_1.shortName, AP_SOUTH_1);
        regionsByShortName.put(SA_EAST_1.shortName, SA_EAST_1);
        regionsByShortName.put(ME_SOUTH_1.shortName, ME_SOUTH_1);

        // Only populate unique values
        regionsByShortName.put(US_EAST_1.cloudFrontName, US_EAST_1);			/* US */
        regionsByShortName.put(CA_CENTRAL_1.cloudFrontName, CA_CENTRAL_1);		/* CA */
        regionsByShortName.put(EU_WEST_1.cloudFrontName, EU_WEST_1);			/* EU */
        regionsByShortName.put(AP_NORTHEAST_1.cloudFrontName, AP_NORTHEAST_1);	/* JP */
        regionsByShortName.put(AP_SOUTHEAST_1.cloudFrontName, AP_SOUTHEAST_1);	/* AP */
        regionsByShortName.put(AP_SOUTHEAST_2.cloudFrontName, AP_SOUTHEAST_2);	/* AU */
        regionsByShortName.put(AP_SOUTH_1.cloudFrontName, AP_SOUTH_1);			/* IN */
        regionsByShortName.put(SA_EAST_1.cloudFrontName, SA_EAST_1);			/* SA */
        regionsByShortName.put(ME_SOUTH_1.cloudFrontName, ME_SOUTH_1);			/* ME */

        regionsByName.put(US_EAST_1.name, US_EAST_1);
        regionsByName.put(US_EAST_2.name, US_EAST_2);
        regionsByName.put(US_WEST_1.name, US_WEST_1);
        regionsByName.put(US_WEST_2.name, US_WEST_2);
        regionsByName.put(US_WEST_2_LAX_1.name, US_WEST_2_LAX_1);
        regionsByName.put(CA_CENTRAL_1.name, CA_CENTRAL_1);
        regionsByName.put(EU_WEST_1.name, EU_WEST_1);
        regionsByName.put(EU_CENTRAL_1.name, EU_CENTRAL_1);
        regionsByName.put(EU_WEST_2.name, EU_WEST_2);
        regionsByName.put(EU_WEST_3.name, EU_WEST_3);
        regionsByName.put(EU_NORTH_1.name, EU_NORTH_1);
        regionsByName.put(AP_EAST_1.name, AP_EAST_1);
        regionsByName.put(AP_NORTHEAST_1.name, AP_NORTHEAST_1);
        regionsByName.put(AP_NORTHEAST_2.name, AP_NORTHEAST_2);
        regionsByName.put(AP_NORTHEAST_3.name, AP_NORTHEAST_3);
        regionsByName.put(AP_SOUTHEAST_1.name, AP_SOUTHEAST_1);
        regionsByName.put(AP_SOUTHEAST_2.name, AP_SOUTHEAST_2);
        regionsByName.put(AP_SOUTH_1.name, AP_SOUTH_1);
        regionsByName.put(SA_EAST_1.name, SA_EAST_1);
        regionsByName.put(ME_SOUTH_1.name, ME_SOUTH_1);
    }

    public final String shortName;
    public final String cloudFrontName;
    public final String priceListName;
    Map<String, Zone> zones = Maps.newConcurrentMap();

    private Region(String name, String shortName, String cloudFrontName, String priceListName) {
        super(name);
        this.shortName = shortName;
        this.cloudFrontName = cloudFrontName;
        this.priceListName = priceListName;
    }

    public Collection<Zone> getZones() {
        return zones.values();
    }
    
    public Zone getZone(String name) throws BadZone {
    	Zone zone = zones.get(name);
    	if (zone == null) {
        	zone = Zone.addZone(this, name);
        	if (zone != null)
        		zones.put(name, zone);
    	}
    	
    	return zone;
    }

    public static Region getRegionByShortName(String shortName) {
        return regionsByShortName.get(shortName);
    }

    public static Region getRegionByName(String name) {
        Region region = regionsByName.get(name);
        if (region == null)
        	logger.error("Unknown region name: " + name);
        return region;
    }

    public static List<Region> getRegions(List<String> names) {
        List<Region> result = Lists.newArrayList();
        for (String name: names)
            result.add(regionsByName.get(name));
        return result;
    }

    public static List<Region> getAllRegions() {
        return Lists.newArrayList(regionsByName.values());
    }
}
