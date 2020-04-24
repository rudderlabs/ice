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
	
    public static List<String> cloudFrontRegions = Lists.newArrayList(new String[]{"AP","AU","CA","EU","IN","JP","ME","SA","US","ZA"});
	
	public static final Region GLOBAL = new Region("global", "", "Global");
	public static final Region US_EAST_1 = new Region("us-east-1", "USE1", "US East (N. Virginia)");
    public static final Region US_EAST_2 = new Region("us-east-2", "USE2", "US East (Ohio)");
    public static final Region US_WEST_1 = new Region("us-west-1", "USW1", "US West (N. California)");
    public static final Region US_WEST_2 = new Region("us-west-2", "USW2", "US West (Oregon)");
    public static final Region US_WEST_2_LAX_1 = new Region("us-west-2-lax-1", "LAX1", "US West (Los Angeles)");
    public static final Region CA_CENTRAL_1 = new Region("ca-central-1", "CAN1", "Canada (Central)");
    public static final Region EU_WEST_1 = new Region("eu-west-1", "EU", "EU (Ireland)");
    public static final Region EU_CENTRAL_1 = new Region("eu-central-1", "EUC1", "EU (Frankfurt)");
    public static final Region EU_WEST_2 = new Region("eu-west-2", "EUW2", "EU (London)");
    public static final Region EU_WEST_3 = new Region("eu-west-3", "EUW3", "EU (Paris)");
    public static final Region EU_NORTH_1 = new Region("eu-north-1", "EUN1", "EU (Stockholm)");
    public static final Region AP_EAST_1 = new Region("ap-east-1", "APE1", "Asia Pacific (Hong Kong)");
    public static final Region AP_NORTHEAST_1 = new Region("ap-northeast-1","APN1", "Asia Pacific (Tokyo)");
    public static final Region AP_NORTHEAST_2 = new Region("ap-northeast-2","APN2", "Asia Pacific (Seoul)");
    public static final Region AP_NORTHEAST_3 = new Region("ap-northeast-3","APN3", "Asia Pacific (Osaka-Local)");
    public static final Region AP_SOUTHEAST_1 = new Region("ap-southeast-1", "APS1", "Asia Pacific (Singapore)");
    public static final Region AP_SOUTHEAST_2 = new Region("ap-southeast-2", "APS2", "Asia Pacific (Sydney)");
    public static final Region AP_SOUTH_1 = new Region("ap-south-1", "APS3", "Asia Pacific (Mumbai)");
    public static final Region SA_EAST_1 = new Region("sa-east-1", "SAE1", "South America (Sao Paulo)");
    public static final Region ME_SOUTH_1 = new Region("me-south-1", "MES1", "Middle East (Bahrain)");
    public static final Region AF_SOUTH_1 = new Region("af-south-1", "AFS1", "Africa (Cape Town)");

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
        regionsByShortName.put(AF_SOUTH_1.shortName, AF_SOUTH_1);

        // Populate regions used to serve edge locations for CloudFront
        regionsByShortName.put("US", US_EAST_1);		/* US United States*/
        regionsByShortName.put("CA", CA_CENTRAL_1);		/* CA Canada */
        regionsByShortName.put("EU", EU_WEST_1);		/* EU Europe */
        regionsByShortName.put("JP", AP_NORTHEAST_1);	/* JP Japan */
        regionsByShortName.put("AP", AP_SOUTHEAST_1);	/* AP Asia Pacific */
        regionsByShortName.put("AU", AP_SOUTHEAST_2);	/* AU Australia */
        regionsByShortName.put("IN", AP_SOUTH_1);		/* IN India */
        regionsByShortName.put("SA", SA_EAST_1);		/* SA South America */
        regionsByShortName.put("ME", EU_CENTRAL_1);		/* ME Middle East */
        regionsByShortName.put("ZA", EU_WEST_2);		/* ZA South Africa */

        regionsByName.put(GLOBAL.name, GLOBAL);
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
        regionsByName.put(AF_SOUTH_1.name, AF_SOUTH_1);
    }

    public final String shortName;
    public final String priceListName;
    Map<String, Zone> zones = Maps.newConcurrentMap();

    private Region(String name, String shortName, String priceListName) {
        super(name);
        this.shortName = shortName;
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
