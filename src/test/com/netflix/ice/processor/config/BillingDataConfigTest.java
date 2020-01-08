/*
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
package com.netflix.ice.processor.config;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class BillingDataConfigTest {

	@Test
	public void testDeserializeFromYaml() throws JsonParseException, JsonMappingException, IOException {
		// Test the primary nodes.
		String yaml = "" +
		"accounts:\n" + 
		"  - id: 123456789012\n" + 
		"    name: act1\n" + 
		"    parents: [root, ou]\n" + 
		"    tags:\n" + 
		"      TagName: tag-value\n" + 
		"    riProducts: [ec2, rds]\n" + 
		"    role: ice\n" + 
		"    externalId:\n" + 
		"    \n" + 
		"tags:\n" + 
		"  - name: Environment\n" + 
		"    aliases: [env]\n" + 
		"    values:\n" + 
		"      Prod: [production, prd]\n" + 
		"      \n" + 
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
		"postprocrules:\n" + 
		"  - name: ComputedCost\n" + 
		"    start: 2019-11\n" + 
		"    end: 2022-11\n" + 
		"    operands:\n" + 
		"      data:\n" + 
		"        type: usage\n" + 
		"        usageType: ${group}-DataTransfer-Out-Bytes\n" + 
		"    in:\n" + 
		"      type: usage\n" + 
		"      product: Product\n" + 
		"      usageType: (..)-Requests-[12].*\n" + 
		"    results:\n" + 
		"      - result:\n" + 
		"          type: cost\n" + 
		"          product: ComputedCost\n" + 
		"          usageType: ${group}-Requests\n" + 
		"        value: '(${in} - (${data} * 4 * 8 / 2)) * 0.01 / 1000'\n" + 
		"      - result:\n" + 
		"          type: usage\n" + 
		"          product: ComputedCost\n" + 
		"          usageType: ${group}-Requests\n" + 
		"        value: '${in} - (${data} * 4 * 8 / 2)'\n";
		
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		BillingDataConfig bdc = new BillingDataConfig();
		bdc = mapper.readValue(yaml, bdc.getClass());
		
		assertEquals("Wrong number of accounts", 1, bdc.getAccounts().size());
		assertEquals("Wrong number of tags", 1, bdc.getTags().size());
		assertEquals("Wrong number of kubernetes", 1, bdc.getKubernetes().size());
		assertEquals("Wrong number of postprocrules", 1, bdc.getPostprocrules().size());
	}

}
