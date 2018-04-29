package com.netflix.ice.processor;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.ProcessorConfig.JsonFiles;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.UserTag;

public class DataJsonWriter extends DataFile {
	private final DateTime monthDateTime;
	protected OutputStreamWriter writer;
	private List<String> tagNames;
	private JsonFiles fileType;
    private final Map<Product, ReadWriteData> costDataByProduct;
    private final Map<Product, ReadWriteData> usageDataByProduct;
	
	public DataJsonWriter(String name, DateTime monthDateTime, List<String> tagNames, JsonFiles fileType,
			Map<Product, ReadWriteData> costDataByProduct,
			Map<Product, ReadWriteData> usageDataByProduct)
			throws Exception {
		super(name, true);
		this.monthDateTime = monthDateTime;
		this.tagNames = tagNames;
		this.fileType = fileType;
		this.costDataByProduct = costDataByProduct;
		this.usageDataByProduct = usageDataByProduct;
	}
	
	// For unit testing
	protected DataJsonWriter(DateTime monthDateTime, List<String> tagNames,
			Map<Product, ReadWriteData> costDataByProduct,
			Map<Product, ReadWriteData> usageDataByProduct) {
		super();
		this.monthDateTime = monthDateTime;
		this.tagNames = tagNames;
		this.costDataByProduct = costDataByProduct;
		this.usageDataByProduct = usageDataByProduct;
	}
	
	@Override
    public void open() throws IOException {
		super.open();
    	writer = new OutputStreamWriter(os);
    }

	@Override
    public void close() throws IOException {
		writer.close();
		super.close();
    }


	@Override
	protected void write() throws IOException {
        for (Product product: costDataByProduct.keySet()) {
        	// Skip the "null" product map that doesn't have resource tags
        	if (product == null)
        		continue;
        	
        	write(costDataByProduct.get(product), usageDataByProduct.get(product));
        }
	}
	
	private void write(ReadWriteData cost, ReadWriteData usage) throws IOException {
		Gson gson = new GsonBuilder().registerTypeAdapter(ResourceGroup.class, new ResourceGroupSerializer()).create();
		DateTimeFormatter dtf = ISODateTimeFormat.dateTimeNoMillis();
        for (int i = 0; i < cost.getNum(); i++) {
            Map<TagGroup, Double> costMap = cost.getData(i);
            if (costMap.size() == 0)
            	continue;
            
            Map<TagGroup, Double> usageMap = usage.getData(i);
            for (Entry<TagGroup, Double> costEntry: costMap.entrySet()) {
            	Double usageValue = usageMap == null ? null : usageMap.get(costEntry.getKey());
            	Item item = new Item(dtf.print(monthDateTime.plusHours(i)), costEntry.getKey(), costEntry.getValue(), usageValue);
            	String json = gson.toJson(item);
            	if (fileType == JsonFiles.bulk)
            		writer.write("{\"index\":{}}\n");
            	writer.write(json + "\n");
            }
        }
	}
	
	public class ResourceGroupSerializer implements JsonSerializer<ResourceGroup> {
		public JsonElement serialize(ResourceGroup rg, Type typeOfSrc, JsonSerializationContext context) {
			if (rg.isProductName())
				return null;
			
			JsonObject tags = new JsonObject();
			
			UserTag[] userTags = rg.getUserTags();
			for (int i = 0; i < userTags.length; i++) {
				if (userTags[i] == null || userTags[i].name.isEmpty())
					continue;
				tags.addProperty(tagNames.get(i), userTags[i].name);
			}
			
			return tags;
		}
	}
	
	public class Item {
		String hour;
		String accountId;
		String account;
		String region;
		String zone;
		String product;
		String operation;
		String usageType;
		ResourceGroup tags;
		Double cost;
		Double usage;
		
		public Item(String hour, TagGroup tg, Double cost, Double usage) {
			this.hour = hour;
			this.cost = cost;
			this.usage = usage;
			
			accountId = tg.account.id;
			account = tg.account.name;
			region = tg.region.name;
			zone = tg.zone == null ? null : tg.zone.name;
			product = tg.product.name;
			operation = tg.operation.name;
			usageType = tg.usageType.name;
			tags = tg.resourceGroup;
		}
	}

}
