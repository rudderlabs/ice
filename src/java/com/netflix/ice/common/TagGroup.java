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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.tag.*;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class TagGroup implements Comparable<TagGroup>, Serializable {
	private static final long serialVersionUID = 3L;
    private final Logger logger = LoggerFactory.getLogger(getClass());
	
	public final Account account;
    public final Product product;
    public final Operation operation;
    public final UsageType usageType;
    public final Region region;
    public final Zone zone;
    public final ResourceGroup resourceGroup;
    
    protected TagGroup(Account account, Region region, Zone zone, Product product, Operation operation, UsageType usageType, ResourceGroup resourceGroup) {
        this.account = account;
        this.region = region;
        this.zone = zone;
        this.product = product;
        this.operation = operation;
        this.usageType = usageType;
        this.resourceGroup = resourceGroup;
    }

    @Override
    public String toString() {
        return "\"" + account + "\",\"" + region + "\",\"" + zone + "\",\"" + product + "\",\"" + operation + "\",\"" + usageType + "\",\"" + resourceGroup + "\"";
    }

    public int compareTo(TagGroup t) {
        int result = this.account.compareTo(t.account);
        if (result != 0)
            return result;
        result = this.region.compareTo(t.region);
        if (result != 0)
            return result;
        result = this.zone == t.zone ? 0 : (this.zone == null ? 1 : (t.zone == null ? -1 : t.zone.compareTo(this.zone)));
        if (result != 0)
            return result;
        result = this.product.compareTo(t.product);
        if (result != 0)
            return result;
        result = this.operation.compareTo(t.operation);
        if (result != 0)
            return result;
        result = this.usageType.compareTo(t.usageType);
        if (result != 0)
            return result;
        result = this.resourceGroup == t.resourceGroup ? 0 : (this.resourceGroup == null ? 1 : (t.resourceGroup == null ? -1 : t.resourceGroup.compareTo(this.resourceGroup)));
            return result;
    }

    @Override
    public boolean equals(Object o) {
    	if (this == o)
    		return true;
        if (o == null)
            return false;
        TagGroup other = (TagGroup)o;

        boolean match = 
                this.zone == other.zone &&
                this.account == other.account &&
                this.region == other.region &&
                this.product == other.product &&
                this.operation == other.operation &&
                this.usageType == other.usageType &&
                this.resourceGroup == other.resourceGroup;
        
        if (!match) {
        	// Check for value match
            boolean valueMatch = 
            		this.account.equals(other.account) &&
            		this.region.equals(other.region) &&
            		(this.zone == null && other.zone == null) || (this.zone != null && other.zone != null && this.zone.equals(other.zone)) &&
            		this.product.equals(other.product) &&
            		this.operation.equals(other.operation) &&
            		this.usageType.equals(other.usageType) &&
            		(this.resourceGroup == null && other.resourceGroup == null) || (this.resourceGroup != null && other.resourceGroup != null && this.resourceGroup.equals(other.resourceGroup));
            
            if (match != valueMatch) {
            	List<String> mismatches = Lists.newArrayList();
            	if (this.account != other.account)
            		mismatches.add("account");
            	if (this.region != other.region)
            		mismatches.add("region");
            	if (this.zone != other.zone)
            		mismatches.add("zone");
            	if (this.product != other.product)
            		mismatches.add("product");
            	if (this.operation != other.operation)
            		mismatches.add("operation");
            	if (this.usageType != other.usageType)
            		mismatches.add("usageType("+ this.usageType.hashCode() + "," + other.usageType.hashCode() + ")");
            	if (this.resourceGroup != other.resourceGroup)
            		mismatches.add("resourceGroup(" + this.resourceGroup.hashCode() + "," + other.resourceGroup.hashCode() + ")");
            	
            	logger.error("non-equivalent tag sets in TagGroup comparison: " + this + ", " + mismatches);
            	logger.error("usageTypes: " + this.usageType.name + "," + this.usageType.unit + ";" + other.usageType.name + "," + other.usageType.unit);
            	logger.error("this.usageType lookup: " + UsageType.getUsageType(this.usageType.name, this.usageType.unit).hashCode());
            	logger.error("other.usageType lookup: " + UsageType.getUsageType(other.usageType.name, other.usageType.unit).hashCode());
            	
            	
            	match = valueMatch;
            }          
        }
        
        return match;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        if (this.zone != null)
            result = prime * result + this.zone.hashCode();
        result = prime * result + this.account.hashCode();
        result = prime * result + this.region.hashCode();
        result = prime * result + this.product.hashCode();
        result = prime * result + this.operation.hashCode();
        result = prime * result + this.usageType.hashCode();
        if (this.resourceGroup != null)
            result = prime * result + this.resourceGroup.hashCode();

        return result;
    }

    private static Map<TagGroup, TagGroup> tagGroups = Maps.newConcurrentMap();

    public static TagGroup getTagGroup(String account, String region, String zone, String product, String operation, String usageTypeName, String usageTypeUnit, String resourceGroup, AccountService accountService, ProductService productService) {
        return getTagGroup(
    		accountService.getAccountByName(account),
        	Region.getRegionByName(region),
        	StringUtils.isEmpty(zone) ? null : Zone.getZone(zone, Region.getRegionByName(region)),
        	productService.getProductByName(product),
        	Operation.getOperation(operation),
            UsageType.getUsageType(usageTypeName, usageTypeUnit),
            StringUtils.isEmpty(resourceGroup) ? null : ResourceGroup.getResourceGroup(resourceGroup, resourceGroup.equals(product)));   	
    }
    
    public static TagGroup getTagGroup(Account account, Region region, Zone zone, Product product, Operation operation, UsageType usageType, ResourceGroup resourceGroup) {
        TagGroup newOne = new TagGroup(account, region, zone, product, operation, usageType, resourceGroup);
        TagGroup oldOne = tagGroups.get(newOne);
        if (oldOne != null) {
            return oldOne;
        }
        else {
            tagGroups.put(newOne, newOne);
            return newOne;
        }
    }

    public static class Serializer {

        public static void serializeTagGroups(DataOutput out, TreeMap<Long, Collection<TagGroup>> tagGroups) throws IOException {
            out.writeInt(tagGroups.size());
            for (Long monthMilli: tagGroups.keySet()) {
                out.writeLong(monthMilli);
                Collection<TagGroup> keys = tagGroups.get(monthMilli);
                out.writeInt(keys.size());
                for (TagGroup tagGroup: keys) {
                    serialize(out, tagGroup);
                }
            }
        }

        public static void serialize(DataOutput out, TagGroup tagGroup) throws IOException {
            out.writeUTF(tagGroup.account.toString());
            out.writeUTF(tagGroup.region.toString());
            out.writeUTF(tagGroup.zone == null ? "" : tagGroup.zone.toString());
            // Always use the Product AWS name - the tag name can be updated to change how it's displayed
            out.writeUTF(tagGroup.product.getCanonicalName());
            out.writeUTF(tagGroup.operation.toString());
            UsageType.serialize(out, tagGroup.usageType);
            out.writeUTF(tagGroup.resourceGroup == null ? "" : tagGroup.resourceGroup.isProductName() ? tagGroup.product.getCanonicalName() : tagGroup.resourceGroup.toString());
        }
        
        public static void serializeCsvHeader(OutputStreamWriter out) throws IOException {
        	out.write("account,region,zone,product,operation,");
        	UsageType.serializeCsvHeader(out);
        	out.write(",resourceGroup");
        }

        public static void serializeCsv(OutputStreamWriter out, TagGroup tagGroup) throws IOException {
            out.write(tagGroup.account.toString() + ",");
            out.write(tagGroup.region.toString() + ",");
            out.write(tagGroup.zone == null ? "," : (tagGroup.zone.toString() + ","));
            // Always use the Product AWS name - the tag name can be updated to change how it's displayed
            out.write(tagGroup.product.getCanonicalName() + ",");
            out.write(tagGroup.operation.toString() + ",");
            UsageType.serializeCsv(out, tagGroup.usageType);
            out.write(",");
            out.write(tagGroup.resourceGroup == null ? "" : tagGroup.resourceGroup.toString());
        }

        public static TreeMap<Long, Collection<TagGroup>> deserializeTagGroups(AccountService accountService, ProductService productService, DataInput in) throws IOException {
            int numCollections = in.readInt();
            TreeMap<Long, Collection<TagGroup>> result = Maps.newTreeMap();
            for (int i = 0; i < numCollections; i++) {
                long monthMilli = in.readLong();
                int numKeys = in.readInt();
                List<TagGroup> keys = Lists.newArrayList();
                for (int j = 0; j < numKeys; j++) {
                    keys.add(deserialize(accountService, productService, in));
                }
                result.put(monthMilli, keys);
            }

            return result;
        }

        public static TagGroup deserialize(AccountService accountService, ProductService productService, DataInput in) throws IOException {
            Account account = accountService.getAccountByName(in.readUTF());
            Region region = Region.getRegionByName(in.readUTF());
            String zoneStr = in.readUTF();
            Zone zone = StringUtils.isEmpty(zoneStr) ? null : Zone.getZone(zoneStr, region);
            String prodStr = in.readUTF();
            Product product = productService.getProductByName(prodStr);
            Operation operation = Operation.getOperation(in.readUTF());
            UsageType usageType = UsageType.deserialize(in);
            String resourceGroupStr = in.readUTF();
            ResourceGroup resourceGroup = StringUtils.isEmpty(resourceGroupStr) ? null : ResourceGroup.getResourceGroup(resourceGroupStr.equals(prodStr) ? product.name : resourceGroupStr, resourceGroupStr.equals(prodStr));

            return TagGroup.getTagGroup(account, region, zone, product, operation, usageType, resourceGroup);
        }
                
        // Serialize to CSV for general debugging
        public static void serializeTagGroupsCsv(DataOutput out, TreeMap<Long, Collection<TagGroup>> tagGroups) throws IOException {
            out.writeChars("Month,Account,Region,Zone,Product,Operation,UsageType,UsageTypeUnit,ResourceGroup\n");
            DateTimeFormatter dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(DateTimeZone.UTC);

            for (Long monthMilli: tagGroups.keySet()) {
                Collection<TagGroup> keys = tagGroups.get(monthMilli);
                for (TagGroup tagGroup: keys) {
                	StringBuilder sb = new StringBuilder(256);
                	sb.append(dateFormatter.print(monthMilli));
                	sb.append(",");
                	sb.append(tagGroup.account.toString());
                	sb.append(",");
                	sb.append(tagGroup.region.toString());
                	sb.append(",");
                	sb.append(tagGroup.zone == null ? "" : tagGroup.zone.toString());
                	sb.append(",");
                	sb.append(tagGroup.product.getCanonicalName());
                	sb.append(",");
                	sb.append(tagGroup.operation.toString());
                	sb.append(",");
                	sb.append(tagGroup.usageType.name);
                	sb.append(",");
                	sb.append(tagGroup.usageType.unit);
                	sb.append(",");
                    sb.append(tagGroup.resourceGroup == null ? "" : tagGroup.resourceGroup.toString());
                    sb.append("\n");
                    
                	out.writeChars(sb.toString());
                }
            }
        }
    }
}
