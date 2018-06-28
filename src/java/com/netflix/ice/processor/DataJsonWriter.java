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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.ProcessorConfig.JsonFiles;
import com.netflix.ice.processor.pricelist.InstancePrices;
import com.netflix.ice.processor.pricelist.InstancePrices.OfferingClass;
import com.netflix.ice.processor.pricelist.InstancePrices.PurchaseOption;
import com.netflix.ice.processor.pricelist.InstancePrices.Rate;
import com.netflix.ice.processor.pricelist.InstancePrices.RateKey;
import com.netflix.ice.processor.pricelist.PriceListService;
import com.netflix.ice.processor.pricelist.InstancePrices.LeaseContractLength;
import com.netflix.ice.processor.pricelist.InstancePrices.ServiceCode;
import com.netflix.ice.reader.InstanceMetrics;
import com.netflix.ice.tag.FamilyTag;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.UserTag;

public class DataJsonWriter extends DataFile {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    
	private final DateTime monthDateTime;
	protected OutputStreamWriter writer;
	private List<String> tagNames;
	private JsonFiles fileType;
    private final Map<Product, ReadWriteData> costDataByProduct;
    private final Map<Product, ReadWriteData> usageDataByProduct;
    protected InstanceMetrics instanceMetrics;
    protected InstancePrices ec2Prices;
    protected InstancePrices rdsPrices;
    
	public DataJsonWriter(String name, DateTime monthDateTime, List<String> tagNames, JsonFiles fileType,
			Map<Product, ReadWriteData> costDataByProduct,
			Map<Product, ReadWriteData> usageDataByProduct,
			InstanceMetrics instanceMetrics, PriceListService priceListService)
			throws Exception {
		super(name, true);
		this.monthDateTime = monthDateTime;
		this.tagNames = tagNames;
		this.fileType = fileType;
		this.costDataByProduct = costDataByProduct;
		this.usageDataByProduct = usageDataByProduct;
	    this.instanceMetrics = instanceMetrics;
	    this.ec2Prices = priceListService.getPrices(monthDateTime, ServiceCode.AmazonEC2);
	    this.rdsPrices = priceListService.getPrices(monthDateTime, ServiceCode.AmazonRDS);
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
            	
            	if (i == 0 && costEntry.getKey().operation.isUnused())
            		logger.info("Unused RI: " + dtf.print(monthDateTime.plusHours(i)) + ", " + costEntry.getKey() + ", " + costEntry.getValue() + ", " + usageValue);
            	
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
	
	public class NormalizedRate {
		Double noUpfrontHourly;
		Double partialUpfrontFixed;
		Double partialUpfrontHourly;
		Double allUpfrontFixed;
		
		public NormalizedRate(InstancePrices.Product product, LeaseContractLength lcl, OfferingClass oc) {
			double nsf = product.normalizationSizeFactor;
			InstancePrices.Rate rate = product.getReservationRate(new RateKey(lcl, PurchaseOption.noUpfront, oc));
			this.noUpfrontHourly = rate != null && rate.hourly > 0 ? rate.hourly / nsf : null;
			
			rate = product.getReservationRate(new RateKey(lcl, PurchaseOption.partialUpfront, oc));
			if (rate != null) {
				this.partialUpfrontFixed = rate.fixed > 0 ? rate.fixed / nsf : null;
				this.partialUpfrontHourly = rate.hourly > 0 ? rate.hourly / nsf : null;
			}
			
			rate = product.getReservationRate(new RateKey(lcl, PurchaseOption.allUpfront, oc));
			this.allUpfrontFixed = rate != null && rate.fixed > 0 ? rate.fixed / nsf : null;
		}
		
		boolean isNull() {
			return noUpfrontHourly == null &&
					partialUpfrontFixed == null &&
					partialUpfrontHourly == null &&
					allUpfrontFixed == null;
		}
	}
	
	public class NormalizedPrices {
		Double onDemandRate;
		NormalizedRate oneYearStdRate;
		NormalizedRate oneYearConvRate;
		NormalizedRate threeYearStdRate;
		NormalizedRate threeYearConvRate;
		
		public NormalizedPrices(TagGroup tg) {
			InstancePrices prices = tg.product.isEc2Instance() ? ec2Prices : tg.product.isRdsInstance() ? rdsPrices : null;
			if (prices == null)
				return;
			
			InstancePrices.Product product = prices.getProduct(tg.region, tg.usageType);
			if (product == null) {
				logger.info("no product for " + prices.getServiceCode() + ", " + tg);
				return;
			}
			
			double nsf = product.normalizationSizeFactor;
			onDemandRate = product.getOnDemandRate() / nsf;
			oneYearStdRate = new NormalizedRate(product, LeaseContractLength.oneyear, OfferingClass.standard);
			oneYearStdRate = oneYearStdRate.isNull() ? null : oneYearStdRate;
			oneYearConvRate = new NormalizedRate(product, LeaseContractLength.oneyear, OfferingClass.convertible);
			oneYearConvRate = oneYearConvRate.isNull() ? null : oneYearConvRate;
			threeYearStdRate = new NormalizedRate(product, LeaseContractLength.threeyear, OfferingClass.standard);
			threeYearStdRate = threeYearStdRate.isNull() ? null : threeYearStdRate;
			threeYearConvRate = new NormalizedRate(product, LeaseContractLength.threeyear, OfferingClass.convertible);
			threeYearConvRate = threeYearConvRate.isNull() ? null : threeYearConvRate;
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
		String instanceFamily;
		Double normalizedUsage;
		NormalizedPrices normalizedPrices;
		
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
			
			// EC2 & RDS instances
			if (tg.product.isEc2Instance() || tg.product.isRdsInstance()) {
				instanceFamily = FamilyTag.getFamilyName(tg.usageType.name);
				normalizedUsage = usage == null ? null : usage * instanceMetrics.getNormalizationFactor(tg.usageType);
				
				if (tg.operation.isOnDemand() || tg.operation.isUsed()) {
					normalizedPrices = new NormalizedPrices(tg);
				}
			}
		}
	}

}
