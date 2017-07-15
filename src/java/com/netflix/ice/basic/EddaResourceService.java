package com.netflix.ice.basic;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.processor.ProcessorConfig;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;


/**
 * A ResourceService which queries an https://github.com/Netflix/edda instance for the 'Usage' tag of instances for breaking down
 * costs based on this tag.
 *
 * Recognizes configuration values "ice.eddaresourceservice.url" and "ice.eddaresourceservice.tag", i.e.
 * 
  # Settings for our own Resource-Service            
  ice.eddaresourceservice.url=http://172.16.110.80:8080
  ice.eddaresourceservice.tag=Usage
  
 * Note: You will need to register the service in Bootstrap.groovy when ProcessorConfig and ReaderConfig are instantiated.
 *
 * TODO: There is currently no caching done, so there might be a lot of requests fired off to Edda!
 */
public class EddaResourceService extends ResourceService {
	@SuppressWarnings("unchecked")
	private final List<List<String>> productNamesWithResources = Lists.<List<String>>newArrayList(
              Lists.newArrayList(Product.ec2, Product.ec2Instance, Product.ebs)
         //   , Lists.newArrayList(Product.rds, Product.rdsInstance)
         //   , Lists.newArrayList(Product.s3)
           );

	private final static Logger logger = LoggerFactory.getLogger(EddaResourceService.class);

	private List<List<Product>> productsWithResources = Lists.newArrayList();

    // read from properties
    protected String EDDA_ROOT_URL;
    protected String EDDA_TAG_NAME;

    //private final Properties prop;

    public EddaResourceService(Properties prop) {
		super();
		//this.prop = prop;

		EDDA_ROOT_URL = prop.getProperty("ice.eddaresourceservice.url", "http://localhost:18081/edda/api/v2/");
		EDDA_TAG_NAME = prop.getProperty("ice.eddaresourceservice.tag", "Usage");
	}

	/* (non-Javadoc)
	 * @see com.netflix.ice.common.ResourceService#init()
	 */
	@Override
	public void init(String[] customTags) {
        logger.info("Initializing...");
        ProcessorConfig processorConfig = ProcessorConfig.getInstance();
        
        for (List<String> l: productNamesWithResources) {
        	List<Product> lp = Lists.newArrayList();
        	for (String name: l) {
        		lp.add(processorConfig.productService.getProductByName(name));
        	}
        	productsWithResources.add(lp);
        }
	}


	@Override
	public String getResource(Account account, Region region, Product product, String resourceId, String[] lineItem,
			long millisStart) {
		// currently we support ec2
		if(product.isEc2() || product.isEc2Instance()) {
			if(StringUtils.isEmpty(resourceId)) {
				logger.warn("Had empty resourceId");
				return "Error";
			}

			try {
				JSONArray instances = readInstanceArray();
				boolean found = false;
				for(int i = 0;i < instances.length();i++) {
					String instance = instances.getString(i);
					if(resourceId.equals(instance)) {
						found = true;
						break;
					}
				}
				if(!found) {
					logger.warn("Did not find resourceId in edda: " + resourceId);
					return "Unknown";
				}

				InputStream stream = new URL(EDDA_ROOT_URL + "view/instances/" + resourceId).openStream();
				final String json;
				try {
					json = IOUtils.toString(stream);
				} finally {
					stream.close();
				}

				JSONObject object = new JSONObject(json);
				JSONArray tags = object.getJSONArray("tags");
				for(int i = 0;i < tags.length();i++) {
					JSONObject tag = tags.getJSONObject(i);
					String key = tag.getString("key");
					if(key.equals(EDDA_TAG_NAME)) {
						String usage = tag.getString("value");
						logger.debug("Found usage: " + usage + " for resource " + resourceId);
						return usage;
					}
				}

				logger.debug("Did not find tag 'Usage' for resource " + resourceId);
				return "Unknown";
			} catch (JSONException e) {
				logger.warn("error parsing json", e);
				return "Error";
			} catch (MalformedURLException e) {
				logger.warn("error parsing url", e);
				return "Error";
			} catch (IOException e) {
				logger.warn("error fetching data from edda at " + EDDA_ROOT_URL, e);
				return "Error";
			}
		}

		logger.debug("Product: " + product + " not handled, resourceId: " + resourceId);
		//logger.info("get resource for account " + account + " region " + region + " product " + product + " resource: " + resourceId + " lineItem: " + Arrays.toString(lineItem));
		return super.getResource(account, region, product, resourceId, lineItem, millisStart);
	}


	/* (non-Javadoc)
	 * @see com.netflix.ice.common.ResourceService#getProductsWithResources()
	 */
	@Override
	public List<List<Product>> getProductsWithResources() {
		logger.info("Register for products: " + productsWithResources + "...");
        return productsWithResources;
	}

	/* (non-Javadoc)
	 * @see com.netflix.ice.common.ResourceService#commit()
	 */
	@Override
	public void commit() {
		logger.info("Commit...");
	}

	/* (non-Javadoc)
	 * @see com.netflix.ice.common.ResourceService#initHeader()
	 */
	@Override
	public void initHeader(List<String> header, int userTagStartIndex) {
		logger.info("initHeader...");
	}

	protected JSONArray readInstanceArray() throws IOException, MalformedURLException, JSONException {
		InputStream stream = new URL(EDDA_ROOT_URL + "view/instances").openStream();
		final String json;
		try {
			json = IOUtils.toString(stream);
		} finally {
			stream.close();
		}
		JSONArray instances = new JSONArray(json);
		return instances;
	}
}
