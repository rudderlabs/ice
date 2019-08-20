package com.netflix.ice.processor;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;

import com.google.common.collect.Maps;
import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.Zone;

public class InstancesTest {

	@Test
	public void testReadWriteCsv() throws IOException {
		Map<String, String> tags = Maps.newHashMap();
		String tagValue = "= I have equal signs, a pipe(|), and a comma =";
		tags.put("Name", tagValue);
		AccountService as = new BasicAccountService();
		ProductService ps = new BasicProductService(new Properties());
		Account account = as.getAccountById("123456789012");
		
		String id = "i-17f85eef87efb7a53";
		
		Instances instances = new Instances(null, null, null);
		instances.add(id, 0, "c4.2xlarge", tags, account, Region.US_EAST_1, Zone.getZone("us-east-1a"), ps.getProductByName(Product.ec2));
		String[] originalValues = instances.get("i-17f85eef87efb7a53").values();
		StringWriter writer = new StringWriter();
		
		String expected = id + ",c4.2xlarge,123456789012," + account.name + ",us-east-1,us-east-1a,EC2,\"Name=" + tagValue + "\"\r";
		instances.writeCsv(writer);
		String[] lines = writer.toString().split("\n");
		assertEquals("wrong number of lines", 2, lines.length);
		assertEquals("serialized form wrong", expected, lines[1]);
		
		StringReader reader = new StringReader(writer.toString());
		instances = new Instances(null, null, null);
		instances.readCsv(reader, as, ps);
		assertArrayEquals("wrong instance values", originalValues, instances.get(id).values());
	}
}
