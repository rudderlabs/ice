package com.netflix.ice.basic;

import static org.junit.Assert.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.reader.TagLists;
import com.netflix.ice.reader.TagListsWithUserTags;
import com.netflix.ice.tag.Operation.ReservationOperation;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.Tag;
import com.netflix.ice.tag.TagType;
import com.netflix.ice.tag.UserTag;

public class BasicTagGroupManagerTest {
	private static ProductService productService = new BasicProductService();
	private static AccountService accountService = new BasicAccountService();
	public final static DateTime testMonth = new DateTime(2018, 1, 1, 0, 0, DateTimeZone.UTC);

	@BeforeClass
	public static void init() {
		accountService.getAccountByName("Account1");
		accountService.getAccountByName("Account2");
	}
	
	@Test
	public void testGetTagListsMap() {
		TagGroup[] tagGroups = new TagGroup[]{
				TagGroup.getTagGroup("Account1", "us-east-1", "us-east-1a", "ProductA", "OperationA", "UsageTypeA", "", "ProductA", accountService, productService),
				TagGroup.getTagGroup("Account1", "us-east-1", "us-east-1a", "ProductA", "OperationA", "UsageTypeA", "", "TagA|", 	accountService, productService),
				TagGroup.getTagGroup("Account1", "us-east-1", "us-east-1a", "ProductA", "OperationA", "UsageTypeA", "", "TagB|", 	accountService, productService),
				TagGroup.getTagGroup("Account1", "us-east-1", "us-east-1a", "ProductA", "OperationA", "UsageTypeA", "", "|TagX", 	accountService, productService),
				TagGroup.getTagGroup("Account1", "us-east-1", "us-east-1a", "ProductA", "OperationA", "UsageTypeA", "", "|TagY", 	accountService, productService),
				TagGroup.getTagGroup("Account1", "us-east-1", "us-east-1a", "ProductA", "OperationA", "UsageTypeA", "", "TagA|TagX", accountService, productService),
				TagGroup.getTagGroup("Account1", "us-east-1", "us-east-1a", "ProductA", "OperationA", "UsageTypeA", "", "TagB|TagY", accountService, productService),
				TagGroup.getTagGroup("Account1", "us-east-1", "us-east-1a", "ProductA", "OperationA", "UsageTypeA", "", "|", 		accountService, productService),
				TagGroup.getTagGroup("Account2", "us-east-1", "us-east-1a", "ProductA", "OperationA", "UsageTypeA", "", "ProductA", accountService, productService),
				TagGroup.getTagGroup("Account2", "us-east-1", "us-east-1a", "ProductA", "OperationA", "UsageTypeA", "", "TagA|", 	accountService, productService),
				TagGroup.getTagGroup("Account2", "us-east-1", "us-east-1a", "ProductA", "OperationA", "UsageTypeA", "", "|TagX", 	accountService, productService),
				// Savings and Lent operation tags that should be filtered if not for reservations
				TagGroup.getTagGroup("Account1", "us-east-1", "us-east-1a", "EC2 Instance", "Savings - Spot", "m1.small", "hour", "EC2 Instance", 	accountService, productService),
				TagGroup.getTagGroup("Account1", "us-east-1", "us-east-1a", "EC2 Instance", "Savings - All Upfront", "m1.small", "hour", "EC2 Instance", 	accountService, productService),
				TagGroup.getTagGroup("Account1", "us-east-1", "us-east-1a", "EC2 Instance", "Lent RIs - All Upfront", "m1.small", "hour", "EC2 Instance", 	accountService, productService),				
		};
		
		TreeMap<Long, Collection<TagGroup>> tagGroupsWithResourceGroups = Maps.newTreeMap();
		List<TagGroup> tagGroupList = Lists.newArrayList();
		for (TagGroup tg: tagGroups)
			tagGroupList.add(tg);
		tagGroupsWithResourceGroups.put(testMonth.getMillis(), tagGroupList);
		
		BasicTagGroupManager manager = new BasicTagGroupManager(tagGroupsWithResourceGroups);
		Interval interval = new Interval(testMonth.getMillis(), testMonth.plusMonths(1).getMillis());		
		
		//
		// Test non-resource tags with no tagLists filtering
		//
		TagLists tagLists = new TagLists();

		// Group by account
		Map<Tag, TagLists> groupByLists = manager.getTagListsMap(interval, tagLists, TagType.Account, false);
		assertEquals("wrong number of groupBy tags for account", 2, groupByLists.size());
		
		// Group by region
		groupByLists = manager.getTagListsMap(interval, tagLists, TagType.Region, false);
		assertEquals("wrong number of groupBy tags for region", 1, groupByLists.size());
		assertEquals("wrong number of operations in tagLists for groupBy region", 1, groupByLists.get(Region.US_EAST_1).operations.size());
		
		// Group by none
		groupByLists = manager.getTagListsMap(interval, tagLists, null, false);
		assertEquals("wrong number of groupBy tags for none", 1, groupByLists.size());
		assertEquals("wrong number of operations in tagLists for groupBy none", 1, groupByLists.get(Tag.aggregated).operations.size());
		
		// Group by operation for reservation
		groupByLists = manager.getTagListsMap(interval, tagLists, TagType.Operation, true);
		assertEquals("wrong number of groupBy tags for operation", 4, groupByLists.size());
		assertEquals("wrong number of operations in tagLists for groupBy operation", 1, groupByLists.get(ReservationOperation.spotInstanceSavings).operations.size());
		assertEquals("wrong number of operations in tagLists for groupBy operation", 1, groupByLists.get(ReservationOperation.savingsAll).operations.size());
		assertEquals("wrong number of operations in tagLists for groupBy operation", 1, groupByLists.get(ReservationOperation.lentInstancesAll).operations.size());
		
		//
		// Test non-resource tags with tagLists filtering
		//
		tagLists = new TagLists(Lists.newArrayList(accountService.getAccountByName("Account1")));
		// Group by account one match
		groupByLists = manager.getTagListsMap(interval, tagLists, TagType.Account, false);
		assertEquals("wrong number of groupBy tags for account", 1, groupByLists.size());
		
		tagLists = new TagLists(Lists.newArrayList(accountService.getAccountByName("Account3")));
		// Group by account - no matches
		groupByLists = manager.getTagListsMap(interval, tagLists, TagType.Account, false);
		assertEquals("wrong number of groupBy tags for account", 0, groupByLists.size());

		//
		// Test Resources with user tags but no tagLists filtering
		//
    	List<List<UserTag>> resourceTagLists = Lists.newArrayList();
    	resourceTagLists.add(null);
    	resourceTagLists.add(null);
		tagLists = new TagListsWithUserTags(null, null, null, null, null, null, resourceTagLists);
		
		// Group by first user tag
		groupByLists = manager.getTagListsMap(interval, tagLists, TagType.Tag, false, 0);
		assertEquals("wrong number of groupBy tags for user tag 0", 3, groupByLists.size());
		
		// Group by second user tag
		groupByLists = manager.getTagListsMap(interval, tagLists, TagType.Tag, false, 1);
		assertEquals("wrong number of groupBy tags for user tag 1", 3, groupByLists.size());

	
		//
		// Test Resources with user tags and tagLists filtering
		//
		resourceTagLists = Lists.newArrayList();
		
		// Group by first tag and only return empties		
		resourceTagLists.add(Lists.<UserTag>newArrayList());
    	resourceTagLists.add(Lists.<UserTag>newArrayList());
    	// Add all the possible second user tag values so we only test filtering against the first
		resourceTagLists.get(1).add(UserTag.get("TagX"));
		resourceTagLists.get(1).add(UserTag.get("TagY"));		
		// Should now be: resourceTagLists[[],["TagX","TagY"]]
		
		tagLists = new TagListsWithUserTags(null, null, null, null, null, null, resourceTagLists);
		groupByLists = manager.getTagListsMap(interval, tagLists, TagType.Tag, true, 0);
		assertEquals("wrong number of groupBy tags for user tag - filter all but empties", 3, groupByLists.size());
		for (TagLists tl: groupByLists.values()) {
			assertTrue("wrong instance type for tagLists", tl instanceof TagListsWithUserTags);
		}
		
		// Now Add empty string to both the first and second tags
		resourceTagLists.get(0).add(UserTag.get(""));
		resourceTagLists.get(1).add(UserTag.get(""));
		// Should now be: resourceTagLists[[""],["","TagX","TagY"]]
    	
		groupByLists = manager.getTagListsMap(interval, tagLists, TagType.Tag, true, 0);
		assertEquals("wrong number of groupBy tags for user tag - filter all but empties", 1, groupByLists.size());
		for (TagLists tl: groupByLists.values()) {
			assertTrue("wrong instance type for tagLists", tl instanceof TagListsWithUserTags);
		}
		
		// Add one of the non-empty values
		resourceTagLists.get(0).add(UserTag.get("TagB"));
		// Should now be: resourceTagLists[["","TagB"],["","TagX","TagY"]]
		// Test for the first user tag
		groupByLists = manager.getTagListsMap(interval, tagLists, TagType.Tag, false, 0);
		assertEquals("wrong number of groupBy tags for user tag - wanted empties and TagB", 2, groupByLists.size());
		for (TagLists tl: groupByLists.values()) {
			assertTrue("wrong instance type for tagLists", tl instanceof TagListsWithUserTags);
		}
		// Make sure we have the three values for the second user tag on both group lists
		TagListsWithUserTags tl = (TagListsWithUserTags) groupByLists.get(UserTag.get(""));
		assertEquals("wrong number of values in empty tag second user tags list", 3, tl.resourceUserTagLists.get(1).size());
		tl = (TagListsWithUserTags) groupByLists.get(UserTag.get("TagB"));
		assertEquals("wrong number of values in TagB tag second user tags list", 3, tl.resourceUserTagLists.get(1).size());
		
		
		for (Tag groupBy: groupByLists.keySet()) {
			tagLists = groupByLists.get(groupBy);
			if (groupBy.name.equals("TagB")) {
		        for (TagGroup tagGroup: tagGroups) {
					boolean contains = tagLists.contains(tagGroup, true);
					assertEquals("contains() returned wrong state on tag for: " + tagGroup, tagGroup.resourceGroup.getUserTags()[0].name.equals("TagB"), contains);
		        }
			}
			else if (groupBy.name.isEmpty()) {
		        for (TagGroup tagGroup: tagGroups) {
					boolean contains = tagLists.contains(tagGroup, true);
					boolean firstUserTagEmpty = tagGroup.resourceGroup.getUserTags()[0].name.isEmpty();
		        	if (tagGroup.operation.isLent() || tagGroup.operation.isSavings())
		        		assertEquals("contains returned wrong state on reservation op: " + tagGroup, false, contains);
		        	else
		        		assertEquals("contains returned wrong state on empty tag for: " + tagGroup, tagGroup.resourceGroup.isProductName() || firstUserTagEmpty, contains);
		        }
			}
			else {
				fail("unexpected tag in groupBy map");
			}
		}
	}

}
