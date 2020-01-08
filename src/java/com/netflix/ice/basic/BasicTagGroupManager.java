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
package com.netflix.ice.basic;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.Config.WorkBucketConfig;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.StalePoller;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.TagGroupWriter;
import com.netflix.ice.reader.TagGroupManager;
import com.netflix.ice.reader.TagLists;
import com.netflix.ice.tag.*;
import com.netflix.ice.tag.Zone.BadZone;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class BasicTagGroupManager extends StalePoller implements TagGroupManager {
    public static final String compressExtension = ".gz";

    private final WorkBucketConfig workBucketConfig;
    private final AccountService accountService;
    private final ProductService productService;
    private final String dbName;
    private final File file;
    private TreeMap<Long, Collection<TagGroup>> tagGroups;
    private TreeMap<Long, Collection<TagGroup>> tagGroupsWithResourceGroups;
    private Interval totalInterval;
    private boolean compress;

    BasicTagGroupManager(Product product, boolean compress, WorkBucketConfig workBucketConfig, AccountService accountService, ProductService productService) {
    	this.compress = compress;
    	this.workBucketConfig = workBucketConfig;
    	this.accountService = accountService;
    	this.productService = productService;
        this.dbName = TagGroupWriter.DB_PREFIX + (product == null ? "all" : product.getServiceCode());
        file = new File(workBucketConfig.localDir, dbName + (compress ? compressExtension : ""));
        try {
            poll();
        }
        catch (Exception e) {
            logger.error("cannot poll data for " + file, e);
        }
        start(DefaultStalePollIntervalSecs, DefaultStalePollIntervalSecs, false);
    }
    
    // For unit testing
    BasicTagGroupManager(TreeMap<Long, Collection<TagGroup>> tagGroupsWithResourceGroups) {
    	this.tagGroupsWithResourceGroups = tagGroupsWithResourceGroups;
    	this.tagGroups = removeResourceGroups(tagGroupsWithResourceGroups);
    	this.workBucketConfig = null;
    	this.accountService = null;
    	this.productService = null;
    	this.dbName = null;
    	this.file = null;
    }
    
    public TreeMap<Long, Integer> getSizes() {
    	TreeMap<Long, Integer> sizes = Maps.newTreeMap();
    	
    	for (Long millis: tagGroupsWithResourceGroups.keySet())
    		sizes.put(millis, tagGroupsWithResourceGroups.get(millis).size());
    	
    	return sizes;
    }
    
    public TreeMap<Long, List<Integer>> getTagValueSizes(int numUserTags) {
    	TreeMap<Long, List<Integer>> sizes = Maps.newTreeMap();
    	for (Long millis: tagGroupsWithResourceGroups.keySet()) {
    		Collection<TagGroup> tagGroups = tagGroupsWithResourceGroups.get(millis);
    		
    		Set<String> accountValues = Sets.newHashSet();
    		Set<String> regionValues = Sets.newHashSet();
    		Set<String> zoneValues = Sets.newHashSet();
    		Set<String> productValues = Sets.newHashSet();
    		Set<String> operationValues = Sets.newHashSet();
    		Set<String> usageTypeValues = Sets.newHashSet();
    		List<Set<String>> userTagValues = Lists.newArrayList();
    		for (int i = 0; i < numUserTags; i++)
    			userTagValues.add(Sets.<String>newHashSet());
    			
    		for (TagGroup tg: tagGroups) {
    			accountValues.add(tg.account.getIceName());
    			regionValues.add(tg.region.name);
    			if (tg.zone != null)
    				zoneValues.add(tg.zone.name);
    			productValues.add(tg.product.name);
    			operationValues.add(tg.operation.name);
    			usageTypeValues.add(tg.usageType.name);
        		if (numUserTags > 0 && tg.resourceGroup != null && !tg.resourceGroup.isProductName()) {
		    		UserTag[] userTags = tg.resourceGroup.getUserTags();
		    		for (int j = 0; j < numUserTags; j++) {
		    			if (!userTags[j].name.isEmpty())
		    				userTagValues.get(j).add(userTags[j].name);
		    		}
        		}
    		}
    		List<Integer> counts = Lists.newArrayList();
    		counts.add(accountValues.size());
    		counts.add(regionValues.size());
    		counts.add(zoneValues.size());
    		counts.add(productValues.size());
    		counts.add(operationValues.size());
    		counts.add(usageTypeValues.size());
    		for (int i = 0; i < numUserTags; i++)
    			counts.add(userTagValues.get(i).size());
    		sizes.put(millis, counts);    		
    	}    	
    	return sizes;
    }
    
    @Override
    protected boolean stalePoll() throws IOException, BadZone {
        boolean downloaded = AwsUtils.downloadFileIfChanged(workBucketConfig.workS3BucketName, workBucketConfig.workS3BucketPrefix, file);
        if (downloaded || tagGroups == null) {
            logger.info("trying to read from " + file);
            InputStream is = new FileInputStream(file);
            if (compress)
            	is = new GZIPInputStream(is);
            DataInputStream in = new DataInputStream(is);
            try {
                TreeMap<Long, Collection<TagGroup>> tagGroupsWithResourceGroups = TagGroup.Serializer.deserializeTagGroups(accountService, productService, in);
                TreeMap<Long, Collection<TagGroup>> tagGroups = removeResourceGroups(tagGroupsWithResourceGroups);
                Interval totalInterval = null;
                if (tagGroups.size() > 0) {
                    totalInterval = new Interval(tagGroups.firstKey(), new DateTime(tagGroups.lastKey()).plusMonths(1).getMillis(), DateTimeZone.UTC);
                }
                this.totalInterval = totalInterval;
                this.tagGroups = tagGroups;
                this.tagGroupsWithResourceGroups = tagGroupsWithResourceGroups;
                logger.info("done reading " + file);
            }
            catch (IOException e) {
            	throw e;
            }
            finally {
                in.close();
            }
        }
        return false;
    }

    @Override
    protected String getThreadName() {
        return this.dbName;
    }

    private TreeMap<Long, Collection<TagGroup>> removeResourceGroups(TreeMap<Long, Collection<TagGroup>> tagGroups) {
        TreeMap<Long, Collection<TagGroup>> result = Maps.newTreeMap();
        for (Long key: tagGroups.keySet()) {
            Collection<TagGroup> from = tagGroups.get(key);
            Set<TagGroup> to = Sets.newHashSet();
            for (TagGroup tagGroup: from) {
                if (tagGroup.resourceGroup != null)
                    to.add(TagGroup.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, tagGroup.operation, tagGroup.usageType, null));
                else
                    to.add(tagGroup);
            }

            result.put(key, to);
        }
        return result;
    }

    private Set<TagGroup> getTagGroupsInRange(Collection<Long> monthMillis) {
        Set<TagGroup> tagGroupsInRange = Sets.newHashSet();
        for (Long monthMilli: monthMillis) {
            tagGroupsInRange.addAll(this.tagGroups.get(monthMilli));
        }
        return tagGroupsInRange;
    }

    private Set<TagGroup> getTagGroupsWithResourceGroupsInRange(Collection<Long> monthMillis) {
        Set<TagGroup> tagGroupsInRange = Sets.newHashSet();
        for (Long monthMilli: monthMillis) {
            tagGroupsInRange.addAll(this.tagGroupsWithResourceGroups.get(monthMilli));
        }
        return tagGroupsInRange;
    }

    private Collection<Long> getMonthMillis(Interval interval) {
        Set<Long> result = Sets.newTreeSet();
        for (Long milli: tagGroups.keySet()) {
            DateTime monthDate = new DateTime(milli, DateTimeZone.UTC);
            if (new Interval(monthDate, monthDate.plusMonths(1)).overlap(interval) != null)
                result.add(milli);
        }

        return result;
    }

    public Collection<Account> getAccounts(Interval interval, TagLists tagLists) {
        Set<Account> result = Sets.newTreeSet();
        Set<TagGroup> tagGroupsInRange = getTagGroupsInRange(getMonthMillis(interval));

        for (TagGroup tagGroup: tagGroupsInRange) {
            if (tagLists.contains(tagGroup))
                result.add(tagGroup.account);
        }

        return result;
    }

    public Collection<Region> getRegions(Interval interval, TagLists tagLists) {
    	class RegionComparator implements Comparator<Region> {

			@Override
			public int compare(Region o1, Region o2) {
				if (o1 == o2)
					return 0;
				if (o1 == Region.GLOBAL)
					return -1;
				if (o2 == Region.GLOBAL)
					return 1;
				
				return o1.compareTo(o2);
			}    		
    	}
    	
        Set<Region> result = Sets.newTreeSet(new RegionComparator());
        Set<TagGroup> tagGroupsInRange = getTagGroupsInRange(getMonthMillis(interval));

        for (TagGroup tagGroup: tagGroupsInRange) {
            if (tagLists.contains(tagGroup))
                result.add(tagGroup.region);
        }

        return result;
    }

    public Collection<Zone> getZones(Interval interval, TagLists tagLists) {
        Set<Zone> result = Sets.newTreeSet();
        Set<TagGroup> tagGroupsInRange = getTagGroupsInRange(getMonthMillis(interval));

        for (TagGroup tagGroup: tagGroupsInRange) {
            if (tagLists.contains(tagGroup) && tagGroup.zone != null)
                result.add(tagGroup.zone);
        }

        return result;
    }

    public Collection<Product> getProducts(Interval interval, TagLists tagLists) {
        Set<Product> result = Sets.newTreeSet();
        Set<TagGroup> tagGroupsInRange = getTagGroupsInRange(getMonthMillis(interval));

        for (TagGroup tagGroup: tagGroupsInRange) {
            if (tagLists.contains(tagGroup))
                result.add(tagGroup.product);
        }

        return result;
    }

    public Collection<Operation> getOperations(Interval interval, TagLists tagLists) {
        Set<Operation> result = Sets.newTreeSet();
        Set<TagGroup> tagGroupsInRange = getTagGroupsInRange(getMonthMillis(interval));

        for (TagGroup tagGroup: tagGroupsInRange) {
            if (tagLists.contains(tagGroup))
                result.add(tagGroup.operation);
        }

        return result;
    }

    public Collection<UsageType> getUsageTypes(Interval interval, TagLists tagLists) {
        Set<UsageType> result = Sets.newTreeSet();
        Set<TagGroup> tagGroupsInRange = getTagGroupsInRange(getMonthMillis(interval));

        for (TagGroup tagGroup: tagGroupsInRange) {
            if (tagLists.contains(tagGroup))
                result.add(tagGroup.usageType);
        }

        return result;
    }

    public Collection<ResourceGroup> getResourceGroups(Interval interval, TagLists tagLists) {
        Set<ResourceGroup> result = Sets.newTreeSet();
        Set<TagGroup> tagGroupsInRange = getTagGroupsWithResourceGroupsInRange(getMonthMillis(interval));

        // Add ResourceGroup tags that are non-null, just the product name, or userTag CSVs.
        for (TagGroup tagGroup: tagGroupsInRange) {
            if (tagLists.contains(tagGroup) && tagGroup.resourceGroup != null) {
                result.add(tagGroup.resourceGroup);
            }
        }

        return result;
    }

    public Collection<UserTag> getResourceGroupTags(Interval interval, TagLists tagLists, int userTagGroupByIndex) {
        Set<UserTag> result = Sets.newTreeSet();
        Set<TagGroup> tagGroupsInRange = getTagGroupsWithResourceGroupsInRange(getMonthMillis(interval));
        
        UserTag emptyUserTag = UserTag.get("");

        // Add ResourceGroup tags that are null, just the product name, or userTag CSVs.
        for (TagGroup tagGroup: tagGroupsInRange) {
        	//logger.info("tag group <" + tagLists.contains(tagGroup) + ">: " + tagGroup);
            if (tagLists.contains(tagGroup)) {
            	try {
            		UserTag t = (tagGroup.resourceGroup == null || tagGroup.resourceGroup.isProductName()) ? emptyUserTag : tagGroup.resourceGroup.getUserTags()[userTagGroupByIndex];
            		result.add(t);
            	}
            	catch (Exception e) {
            		logger.error("Bad resourceGroup: " + tagGroup.resourceGroup + ", " + e);
            	}
            }
        }

        return result;
    }

    public Collection<Account> getAccounts(TagLists tagLists) {
        return this.getAccounts(totalInterval, tagLists);
    }

    public Collection<Region> getRegions(TagLists tagLists) {
        return this.getRegions(totalInterval, tagLists);
    }

    public Collection<Zone> getZones(TagLists tagLists) {
        return this.getZones(totalInterval, tagLists);
    }

    public Collection<Product> getProducts(TagLists tagLists) {
        return this.getProducts(totalInterval, tagLists);
    }

    public Collection<Operation> getOperations(TagLists tagLists) {
        return this.getOperations(totalInterval, tagLists);
    }

    public Collection<UsageType> getUsageTypes(TagLists tagLists) {
        return this.getUsageTypes(totalInterval, tagLists);
    }

    public Collection<ResourceGroup> getResourceGroups(TagLists tagLists) {
        return this.getResourceGroups(totalInterval, tagLists);
    }

    public Interval getOverlapInterval(Interval interval) {
        return totalInterval == null ? null : totalInterval.overlap(interval);
    }

    public Map<Tag, TagLists> getTagListsMap(Interval interval, TagLists tagLists, TagType groupBy, boolean forReservation) {
    	return getTagListsMap(interval, tagLists, groupBy, forReservation, 0);
    }
    public Map<Tag, TagLists> getTagListsMap(Interval interval, TagLists tagLists, TagType groupBy, boolean forReservation, int userTagGroupByIndex) {
        Map<Tag, TagLists> result = Maps.newHashMap();
        
        // Get all the GroupBy tags. If we're not grouping by ResourceGroup or Tag, then work with a TagLists that doesn't contain resourceGroups.
        // Filtering of results against resourceGroup values is handled later.
        TagLists tagListsForTag = tagLists;
        boolean tagListsHasResourceGroups = tagLists.resourceGroups != null && tagLists.resourceGroups.size() > 0;
        if ((groupBy == null || !(groupBy == TagType.ResourceGroup || groupBy == TagType.Tag)) && tagListsHasResourceGroups) {
        	//logger.info("getTagListsWithNullResourceGroup");
            tagListsForTag = tagLists.getTagListsWithNullResourceGroup();
        }
        
        // If not the reservations dashboard, we must always specify all the operations so that we can remove
        // EC2 Instance Savings and Lent Operations
        if (!forReservation && (tagListsForTag.operations == null || tagListsForTag.operations.size() == 0)) {
        	List<Operation> ops = Lists.newArrayList();
        	for (Operation op: getOperations(interval, tagListsForTag)) {
        		if (op.isLent() || op.isSavings() || op.isSavingsPlanLent())
        			continue;
        		ops.add(op);
        	}
        	//logger.info("getTagListsWithOperations");
        	tagListsForTag = tagListsForTag.getTagListsWithOperations(ops);
        }

        if (groupBy == null || groupBy == TagType.TagKey) {
            result.put(Tag.aggregated, tagListsForTag);
        	//logger.info("groupBy == null || groupBy == TagType.TagKey");
            return result;
        }

        List<Tag> groupByTags = Lists.newArrayList();
        switch (groupBy) {
            case Account:
                groupByTags.addAll(getAccounts(interval, tagListsForTag));
                break;
            case Region:
                groupByTags.addAll(getRegions(interval, tagListsForTag));
                break;
            case Zone:
                groupByTags.addAll(getZones(interval, tagListsForTag));
                break;
            case Product:
                groupByTags.addAll(getProducts(interval, tagListsForTag));
                break;
            case Operation:
                groupByTags.addAll(getOperations(interval, tagListsForTag));
                break;
            case UsageType:
                groupByTags.addAll(getUsageTypes(interval, tagListsForTag));
                break;
            case ResourceGroup:
                groupByTags.addAll(getResourceGroups(interval, tagListsForTag));
                break;
            case Tag:
                groupByTags.addAll(getResourceGroupTags(interval, tagListsForTag, userTagGroupByIndex));
                break;
            default:
            	break;
        }
//        logger.info("TagLists: " + tagLists);
//        logger.info("found " + groupByTags.size() + " groupByTags, taglists instanceof " + (tagLists instanceof TagListsWithUserTags ? "TagListsWithUserTags" : "TagLists"));
//        if (tagLists instanceof TagListsWithUserTags) {
//        	for (Tag tag: groupByTags)
//        		logger.info("groupBy tag<" + tagLists.contains(tag, groupBy, userTagGroupByIndex) + ">: " + tag);
//        }
        
        boolean groupByOperationOnReservationDashboard = groupBy == TagType.Operation && forReservation;
        
        if (!groupByOperationOnReservationDashboard) {
            for (Operation.ReservationOperation lentOp: Operation.getLentOperations())
                groupByTags.remove(lentOp);
			for (Operation.ReservationOperation savingsOp: Operation.getSavingsOperations())
				groupByTags.remove(savingsOp);
			for (Operation.SavingsPlanOperation lentOp: Operation.getSavingsPlanLentOperations())
				groupByTags.remove(lentOp);
        }
        for (Tag tag: groupByTags) {
            if (tagLists.contains(tag, groupBy, userTagGroupByIndex)) {
                //logger.info("get tag lists for " + tag + ", " + groupByOperationOnReservationDashboard);
                TagLists tmp = tagLists.getTagLists(tag, groupBy, userTagGroupByIndex);
                if (!groupByOperationOnReservationDashboard) {
                	// Don't include savings or lent operations if we're not doing groupBy Operation on the Reservation Dashboard
                    if (tmp.operations == null || tmp.operations.size() == 0) {
                    	//logger.info("       get new operations list and remove lent and savings ops");
                    	TagLists tl = new TagLists(tmp.accounts, tmp.regions, tmp.zones, tmp.products, tmp.operations, tmp.usageTypes);
                        List<Operation> operations = Lists.newArrayList(getOperations(interval, tl));
                        //logger.info("     operations: " + operations);
                        tmp = tmp.copyWithOperations(operations);
                    }
                    for (Operation.ReservationOperation lentOp: Operation.getLentOperations())
                        tmp.operations.remove(lentOp);
        			for (Operation.ReservationOperation savingsOp: Operation.getSavingsOperations())
        				tmp.operations.remove(savingsOp);
        			for (Operation.SavingsPlanOperation lentOp: Operation.getSavingsPlanLentOperations())
        				tmp.operations.remove(lentOp);
        			
        			//logger.info("          taglists: " + tmp);
                }
                result.put(tag, tmp);
            }
        }
        return result;
    }
    
}
