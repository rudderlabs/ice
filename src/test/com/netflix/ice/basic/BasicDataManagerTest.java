package com.netflix.ice.basic;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ConsolidateType;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.reader.AggregateType;
import com.netflix.ice.reader.ReadOnlyData;
import com.netflix.ice.reader.TagGroupManager;
import com.netflix.ice.reader.TagLists;
import com.netflix.ice.reader.TagListsWithUserTags;
import com.netflix.ice.reader.UsageUnit;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Tag;
import com.netflix.ice.tag.TagType;
import com.netflix.ice.tag.UserTag;

public class BasicDataManagerTest {
    protected Logger logger = LoggerFactory.getLogger(getClass());
	private static final String dataDir = "src/test/data/";

	class TestDataFilePoller extends BasicDataManager {
		private ReadOnlyData data;
		
		TestDataFilePoller(DateTime startDate, final String dbName, ConsolidateType consolidateType, TagGroupManager tagGroupManager, boolean compress,
	    		int monthlyCacheSize, AccountService accountService, ProductService productService, ReadOnlyData data) {
			super(startDate, dbName, consolidateType, tagGroupManager, compress, monthlyCacheSize, accountService, productService, null);
			this.data = data;
		}
		
		@Override
		protected void buildCache(int monthlyCacheSize) {				
		}
		
		@Override
		public void start() {				
		}
		
		@Override
	    protected ReadOnlyData getReadOnlyData(DateTime key) throws ExecutionException {
	        return data;
	    }
	}
	
	@Test
	public void loadHourlyDataFromFile() throws Exception {
		AccountService as = new BasicAccountService();
		ProductService ps = new BasicProductService();
		
		BasicDataManager data = new TestDataFilePoller(DateTime.now(), null, null, null, true, 0, as, ps, null);
	    
	    File f = new File(dataDir + "cost_hourly_EC2_Instance_2018-06.gz");
	    if (!f.exists())
	    	return;
	    
	    ReadOnlyData rod = data.loadDataFromFile(f);
	    
	    logger.info("File: " + f + " has " + rod.getTagGroups().size() + " tag groups and "+ rod.getNum() + " hours of data");
	    
	    SortedSet<Product> products = new TreeSet<Product>();
	    for (TagGroup tg: rod.getTagGroups())
	    	products.add(tg.product);
	
	    logger.info("Products:");
	    for (Product p: products)
	    	logger.info("  " + p.name);
	}
	
	@Test
	public void loadMonthlyDataFromFile() throws Exception {
		AccountService as = new BasicAccountService();
		ProductService ps = new BasicProductService();
		
		BasicDataManager data = new TestDataFilePoller(DateTime.now(), null, null, null, true, 0, as, ps, null);
	    
	    File f = new File(dataDir + "cost_monthly_all.gz");
	    if (!f.exists())
	    	return;
	    
	    ReadOnlyData rod = data.loadDataFromFile(f);
	    
	    logger.info("File: " + f + " has " + rod.getTagGroups().size() + " tag groups and "+ rod.getNum() + " months of data");
	    
	    SortedSet<Product> products = new TreeSet<Product>();
	    for (TagGroup tg: rod.getTagGroups())
	    	products.add(tg.product);
	
	    logger.info("Products:");
	    for (Product p: products)
	    	logger.info("  " + p.name);
	    
	    for (int i = 0; i < rod.getNum(); i++) {
	    	List<TagGroup> tagGroups = (List<TagGroup>) rod.getTagGroups();
	    	double total = 0;
	    	Double[] values = rod.getData(i);
	    	if (values == null) {
	    		logger.info("No data for month " + i);
	    		continue;
	    	}
	    	
	    	for (int j = 0; j < tagGroups.size(); j++) {
	    		if (tagGroups.get(j).product.isEc2Instance()) {
	    			total += values[j] == null ? 0.0 : values[j];
	    		}
	    	}
	    	logger.info("EC2 Instance total for month " + i + ": " + total);
	    	assertTrue("No data for month " + i, total > 0.0);
	    }
	    		
	}
	
	private TagGroupManager makeTagGroupManager(DateTime testMonth, Collection<TagGroup> tagGroups) {
		TreeMap<Long, Collection<TagGroup>> tagGroupsWithResourceGroups = Maps.newTreeMap();
		List<TagGroup> tagGroupList = Lists.newArrayList();
		for (TagGroup tg: tagGroups)
			tagGroupList.add(tg);
		tagGroupsWithResourceGroups.put(testMonth.getMillis(), tagGroupList);
		
		return new BasicTagGroupManager(tagGroupsWithResourceGroups);
	}
	
	@Test
	public void groupByUserTagAndFilterByUserTag() {
		AccountService as = new BasicAccountService();
		ProductService ps = new BasicProductService();
		
		Double[][] rawData = new Double[][]{
				new Double[]{ 1.0, 2.0 },
		};
		Collection<TagGroup> tagGroups = Lists.newArrayList();
		tagGroups.add(TagGroup.getTagGroup("account", "us-east-1", null, "product", "operation", "usgaeType", "usageTypeUnit", "TagA|TagB", as, ps));
		tagGroups.add(TagGroup.getTagGroup("account", "us-east-1", null, "product", "operation", "usgaeType", "usageTypeUnit", "TagA|", as, ps));
		
		
		ReadOnlyData rod = new ReadOnlyData(rawData, tagGroups);
		DateTime testMonth = DateTime.parse("2018-01-01");
		TagGroupManager tagGroupManager = makeTagGroupManager(testMonth, tagGroups);
		
		BasicDataManager dataManager = new TestDataFilePoller(testMonth, null, ConsolidateType.monthly, tagGroupManager, true, 0, as, ps, rod);
		
		Interval interval = new Interval(testMonth, testMonth.plusMonths(1));
		List<List<UserTag>> userTagLists = Lists.newArrayList();
		List<UserTag> listA = Lists.newArrayList();
		List<UserTag> listB = Lists.newArrayList();
		userTagLists.add(listA);
		userTagLists.add(listB);
		
		listA.add(UserTag.get("TagA"));
		
		TagLists tagLists = new TagListsWithUserTags(null, null, null, null, null, null, userTagLists);
		
		Map<Tag, double[]> data = dataManager.getData(interval, tagLists, TagType.Tag, AggregateType.data, false, UsageUnit.Instances, 1);
		
		for (Tag t: data.keySet()) {
			logger.info("Tag: " + t + ": " + data.get(t)[0]);
		}
		assertEquals("Wrong number of groupBy tags", 3, data.size());
		assertNotNull("No aggregated tag", data.get(Tag.aggregated));
		assertNotNull("No (none) tag", data.get(UserTag.get(UserTag.none)));
		assertNotNull("No TagB tag", data.get(UserTag.get("TagB")));
		
	}
	
	@Test
	public void groupByUserTagAndFilterByEmptyUserTag() {
		AccountService as = new BasicAccountService();
		ProductService ps = new BasicProductService();
		
		Double[][] rawData = new Double[][]{
				new Double[]{ 1.0, 2.0 },
		};
		List<TagGroup> tagGroups = Lists.newArrayList();
		tagGroups.add(TagGroup.getTagGroup("account", "us-east-1", null, "product", "operation", "usgaeType", "usageTypeUnit", "TagA|TagB", as, ps));
		tagGroups.add(TagGroup.getTagGroup("account", "us-east-1", null, "product", "operation", "usgaeType", "usageTypeUnit", null, as, ps));		
		
		ReadOnlyData rod = new ReadOnlyData(rawData, tagGroups);
		DateTime testMonth = DateTime.parse("2018-01-01");
		TagGroupManager tagGroupManager = makeTagGroupManager(testMonth, tagGroups);
		
		BasicDataManager dataManager = new TestDataFilePoller(testMonth, null, ConsolidateType.monthly, tagGroupManager, true, 0, as, ps, rod);
		
		Interval interval = new Interval(testMonth, testMonth.plusMonths(1));
		List<List<UserTag>> userTagLists = Lists.newArrayList();
		List<UserTag> listA = Lists.newArrayList();
		List<UserTag> listB = Lists.newArrayList();
		userTagLists.add(listA);
		userTagLists.add(listB);
		
		listB.add(UserTag.get(""));
		
		TagLists tagLists = new TagListsWithUserTags(null, null, null, null, null, null, userTagLists);
		
		Map<Tag, double[]> data = dataManager.getData(interval, tagLists, TagType.Tag, AggregateType.data, false, UsageUnit.Instances, 1);
		
		for (Tag t: data.keySet()) {
			logger.info("Tag: " + t + ": " + data.get(t)[0]);
		}
		assertEquals("Wrong number of groupBy tags", 2, data.size());
		assertNotNull("No aggregated tag", data.get(Tag.aggregated));
		assertNotNull("No (none) tag", data.get(UserTag.get(UserTag.none)));
	}
	
	@Test
	public void groupByNoneWithUserTagFilters() {
		AccountService as = new BasicAccountService();
		ProductService ps = new BasicProductService();
		
		Double[][] rawData = new Double[][]{
				new Double[]{ 1.0, 2.0 },
		};
		Collection<TagGroup> tagGroups = Lists.newArrayList();
		tagGroups.add(TagGroup.getTagGroup("account", "us-east-1", null, "product", "On-Demand Instances", "usgaeType", "usageTypeUnit", "TagA|TagB", as, ps));
		tagGroups.add(TagGroup.getTagGroup("account", "us-east-1", null, "product", "On-Demand Instances", "usgaeType", "usageTypeUnit", "TagA|", as, ps));
		
		
		ReadOnlyData rod = new ReadOnlyData(rawData, tagGroups);
		DateTime testMonth = DateTime.parse("2018-01-01");
		TagGroupManager tagGroupManager = makeTagGroupManager(testMonth, tagGroups);
		
		BasicDataManager dataManager = new TestDataFilePoller(testMonth, null, ConsolidateType.monthly, tagGroupManager, true, 0, as, ps, rod);
		
		Interval interval = new Interval(testMonth, testMonth.plusMonths(1));
		List<List<UserTag>> userTagLists = Lists.newArrayList();
		List<UserTag> listA = Lists.newArrayList();
		List<UserTag> listB = Lists.newArrayList();
		userTagLists.add(listA);
		userTagLists.add(listB);
		
		listB.add(UserTag.get("TagB"));
		
		// First test with operations specified
		List<Operation> operations = Lists.newArrayList((Operation) Operation.ondemandInstances);
		TagLists tagLists = new TagListsWithUserTags(null, null, null, null, operations, null, userTagLists);
		
		Map<Tag, double[]> data = dataManager.getData(interval, tagLists, null, AggregateType.data, false, UsageUnit.Instances, 1);
		
		for (Tag t: data.keySet()) {
			logger.info("Tag: " + t + ": " + data.get(t)[0]);
		}
		assertEquals("With operation specified, wrong number of groupBy tags", 1, data.size());
		assertNotNull("With operation specified, has aggregated tag", data.get(Tag.aggregated));
		assertEquals("With operation specified, wrong value for aggregation", 1.0, data.get(Tag.aggregated)[0], 0.001);
		
		// Now test without operations specified
		tagLists = new TagListsWithUserTags(null, null, null, null, null, null, userTagLists);
		
		data = dataManager.getData(interval, tagLists, null, AggregateType.data, false, UsageUnit.Instances, 1);
		
		for (Tag t: data.keySet()) {
			logger.info("Tag: " + t + ": " + data.get(t)[0]);
		}
		assertEquals("Without operation specified, wrong number of groupBy tags", 1, data.size());
		assertNotNull("Without operation specified, has aggregated tag", data.get(Tag.aggregated));
		assertEquals("Without operation specified, wrong value for aggregation", 1.0, data.get(Tag.aggregated)[0], 0.001);
	}
}
