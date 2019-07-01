package com.netflix.ice.processor;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.netflix.ice.processor.config.AccountConfig;
import com.netflix.ice.processor.config.BillingDataConfig;
import com.netflix.ice.processor.config.KubernetesConfig;
import com.netflix.ice.processor.config.KubernetesNamespaceMapping;

public class BillingDataConfigTest {

	@Test
	public void testConstructFromJson() throws JsonParseException, JsonMappingException, IOException {
		String json = "{\n" + 
				"	\"accounts\": [\n" + 
				"		{\n" + 
				"			\"id\": \"123456789012\",\n" + 
				"			\"name\": \"act1\",\n" + 
				"			\"tags\": {\n" + 
				"				\"Tag1\": \"tag1value\"\n" + 
				"			},\n" + 
				"			\"riProducts\": [ \"ec2\" ],\n" + 
				"			\"role\": \"ice\",\n" + 
				"			\"externalId\": \"12345\"\n" + 
				"		}\n" + 
				"	],\n" + 
				"	\"tags\":[\n" + 
				"		{\n" +
				"           \"name\": \"Environment\",\n" + 
				"			\"aliases\": [ \"env\" ],\n" + 
				"			\"values\": {\n" + 
				"				\"Prod\": [ \"production\", \"prd\" ]\n" + 
				"			}\n" + 
				"		}\n" + 
				"	],\n" +
				"	\"kubernetes\":[\n" + 
				"		{\n" +
				"           \"bucket\": \"k8s-report-bucket\",\n" + 
				"           \"prefix\": \"hourly/kubernetes\",\n" + 
				"           \"clusterNameFormulae\": [ 'Cluster.toLower()', 'Cluster.regex(\"k8s-(.*)\")' ],\n" + 
				"           \"computeTag\": \"Role\",\n" + 
				"           \"computeValue\": \"compute\",\n" + 
				"           \"namespaceTag\": \"K8sNamespace\",\n" + 
				"           \"namespaceMappings\": [\n" + 
				"		        {\n" +
				"                   \"tag\": \"Environment\",\n" + 
				"                   \"value\": \"Prod\",\n" + 
				"                   \"patterns\": [ \".*prod.*\", \".*production.*\", \".*prd.*\" ]\n" + 
				"		        }\n" + 
				"	        ],\n" +
				"           \"tags\": [ \"userTag1\", \"userTag2\" ]\n" + 
				"		}\n" + 
				"	]\n" +
				"}";
		
		BillingDataConfig c = new BillingDataConfig(json);
		assertEquals("Wrong number of accounts", 1, c.accounts.size());
		AccountConfig account = c.accounts.get(0);
		assertEquals("Wrong account id", "123456789012", account.id);
		assertEquals("Wrong account name", "act1", account.name);
		
		assertEquals("Wrong number of account tags", 1, account.tags.size());
		assertTrue("Map doesn't have tag", account.tags.containsKey("Tag1"));
		String tag = account.tags.get("Tag1");
		assertEquals("Wrong tag value", "tag1value", tag);
		assertEquals("Wrong number of RI Products", 1, account.riProducts.size());
		assertEquals("Wrong RI Product", "ec2", account.riProducts.get(0));
		assertEquals("Wrong role", "ice", account.role);
		assertEquals("Wrong externalId", "12345", account.externalId);

		KubernetesConfig kc = c.getKubernetes().get(0);
		assertNotNull("No Kubernetes config", kc);
		assertEquals("Wrong report s3bucket", "k8s-report-bucket", kc.getBucket());
		assertEquals("Wrong report prefix", "hourly/kubernetes", kc.getPrefix());
		assertEquals("Wrong number of cluster name formulae", 2, kc.getClusterNameFormulae().size());
		assertEquals("Wrong first clusterNameFormula", "Cluster.toLower()", kc.getClusterNameFormulae().get(0));
		assertEquals("Wrong second clusterNameFormula", "Cluster.regex(\"k8s-(.*)\")", kc.getClusterNameFormulae().get(1));
		assertEquals("Wrong compute", "Role", kc.getComputeTag());
		assertEquals("Wrong compute", "compute", kc.getComputeValue());
		assertEquals("Wrong namespaceTag", "K8sNamespace", kc.getNamespaceTag());
		
		KubernetesNamespaceMapping m = kc.getNamespaceMappings().get(0);
		assertEquals("Wrong namespace mapping tag", "Environment", m.getTag());
		assertEquals("Wrong namespace mapping value", "Prod", m.getValue());
		assertEquals("Wrong namespace mapping pattern count", 3, m.getPatterns().size());
		assertEquals("Wrong namespace mapping pattern 0", ".*prod.*", m.getPatterns().get(0));
		assertEquals("Wrong namespace mapping pattern 1", ".*production.*", m.getPatterns().get(1));
		assertEquals("Wrong namespace mapping pattern 2", ".*prd.*", m.getPatterns().get(2));
		
		assertEquals("Wrong report usageTags 0", "userTag1", kc.getTags().get(0));		

		// Test without an account name
		json = json.replace("\"name\": \"act1\",\n", "");
		c = new BillingDataConfig(json);
		assertNull("Should have null account name", c.accounts.get(0).name);
	}
	
	@Test
	public void testConstructFromYaml() throws JsonParseException, JsonMappingException, IOException {
		String yaml = 
				"accounts:\n" + 
				"  - id: 123456789012\n" + 
				"    name: act1\n" + 
				"    tags:\n" + 
				"        Tag1: tag1value\n" + 
				"    riProducts: [ec2]\n" + 
				"    role: ice\n" + 
				"    externalId: 12345\n" + 
				"tags:\n" + 
				"  - name: Environment\n" + 
				"    aliases: [env]\n" + 
				"    values:\n" + 
				"        Prod: [production, prd]\n" +
				"kubernetes:\n" + 
				"  - bucket: k8s-report-bucket\n" + 
				"    prefix: hourly/kubernetes\n" + 
				"    clusterNameFormulae: [ 'Cluster.toLower()', 'Cluster.regex(\"k8s-(.*)\")' ]\n" + 
				"    computeTag: Role\n" + 
				"    computeValue: compute\n" + 
				"    namespaceTag: K8sNamespace\n" + 
				"    namespaceMappings:\n" + 
				"      - tag: Environment\n" + 
				"        value: Prod\n" + 
				"        patterns: [ \".*prod.*\", \".*production.*\", \".*prd.*\" ]\n" + 
				"    tags: [ userTag1, userTag2 ]\n" + 
				"";
						
		BillingDataConfig c = new BillingDataConfig(yaml);
		
		assertEquals("Wrong number of accounts", 1, c.accounts.size());
		AccountConfig account = c.accounts.get(0);
		assertEquals("Wrong account id", "123456789012", account.id);
		assertEquals("Wrong account name", "act1", account.name);
		
		assertEquals("Wrong number of account default tags", 1, account.tags.size());
		assertTrue("Map doesn't have default tag", account.tags.containsKey("Tag1"));
		String tag = account.tags.get("Tag1");
		assertEquals("Wrong default tag value", "tag1value", tag);
		assertEquals("Wrong number of RI Products", 1, account.riProducts.size());
		assertEquals("Wrong RI Product", "ec2", account.riProducts.get(0));
		assertEquals("Wrong role", "ice", account.role);
		assertEquals("Wrong externalId", "12345", account.externalId);
		
		KubernetesConfig kc = c.getKubernetes().get(0);
		assertNotNull("No Kubernetes config", kc);
		assertEquals("Wrong report s3bucket", "k8s-report-bucket", kc.getBucket());
		assertEquals("Wrong report prefix", "hourly/kubernetes", kc.getPrefix());
		assertEquals("Wrong number of cluster name formulae", 2, kc.getClusterNameFormulae().size());
		assertEquals("Wrong first clusterNameFormula", "Cluster.toLower()", kc.getClusterNameFormulae().get(0));
		assertEquals("Wrong second clusterNameFormula", "Cluster.regex(\"k8s-(.*)\")", kc.getClusterNameFormulae().get(1));
		assertEquals("Wrong compute", "Role", kc.getComputeTag());
		assertEquals("Wrong compute", "compute", kc.getComputeValue());
		assertEquals("Wrong namespaceTag", "K8sNamespace", kc.getNamespaceTag());
		
		KubernetesNamespaceMapping m = kc.getNamespaceMappings().get(0);
		assertEquals("Wrong namespace mapping tag", "Environment", m.getTag());
		assertEquals("Wrong namespace mapping value", "Prod", m.getValue());
		assertEquals("Wrong namespace mapping pattern count", 3, m.getPatterns().size());
		assertEquals("Wrong namespace mapping pattern 0", ".*prod.*", m.getPatterns().get(0));
		assertEquals("Wrong namespace mapping pattern 1", ".*production.*", m.getPatterns().get(1));
		assertEquals("Wrong namespace mapping pattern 2", ".*prd.*", m.getPatterns().get(2));
		
		assertEquals("Wrong report usageTags 0", "userTag1", kc.getTags().get(0));
		
		// Test without an account name
		yaml = yaml.replace("    name: act1\n", "");
		c = new BillingDataConfig(yaml);
		assertNull("Should have null account name", c.accounts.get(0).name);
		assertEquals("Wrong report usageTags 0", "userTag1", kc.getTags().get(0));		
	}
}
