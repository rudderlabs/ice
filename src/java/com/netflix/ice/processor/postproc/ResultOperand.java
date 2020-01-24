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
package com.netflix.ice.processor.postproc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.AggregationTagGroup;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.Zone;
import com.netflix.ice.tag.Zone.BadZone;

public class ResultOperand extends Operand {
    protected Logger logger = LoggerFactory.getLogger(getClass());

	private AccountService accountService;
	
	public ResultOperand(OperandConfig opConfig, AccountService accountService) {
		super(opConfig, accountService);
		this.accountService = accountService;
	}
	
	/**
	 * Get the result TagGroup based on the supplied AggregationTagGroup. Values present in the operand config are used to
	 * override the values in the supplied TagGroup. For tags that have lists, there should only be one entry, so
	 * the first is always chosen.
	 */
	public TagGroup getTagGroup(AggregationTagGroup atg, ProductService productService) {
		Account account = atg.getAccount();
		if (accountTagFilter != null) {
			account = accountService.getAccountById(accountTagFilter.getTag(account == null ? "" : account.name));
		}
		else if (accounts != null && accounts.size() > 0) {
			account = accounts.get(0);
		}
		
		Region region = atg.getRegion();
		if (regionTagFilter != null) {
			region = Region.getRegionByName(regionTagFilter.getTag(atg.getRegion() == null ? "" : atg.getRegion().name));
		}
		else if (regions != null && regions.size() > 0) {
			region = regions.get(0);
		}
		
		Zone zone = atg.getZone();
		if (zoneTagFilter != null) {
			try {
				zone = region.getZone(zoneTagFilter.getTag(atg.getZone() == null ? "" : atg.getZone().name));
			} catch (BadZone e) {
				logger.error("Bad zone: " + e);
			}
		}
		else if (zones != null && zones.size() > 0) {
			zone = zones.get(0);
		}
		
		Product product = atg.getProduct();
		Operation operation = atg.getOperation();
		UsageType usageType = atg.getUsageType();
		
		return TagGroup.getTagGroup(
				account,
				region,
				zone,
				productTagFilter != null ? productService.getProductByServiceCode(productTagFilter.getTag(product == null ? "" : product.name)) : product,
				operationTagFilter != null ? Operation.getOperation(operationTagFilter.getTag(operation == null ? "" : operation.name)) : operation,
				usageTypeTagFilter != null ? UsageType.getUsageType(usageTypeTagFilter.getTag(usageType == null ? "" : usageType.name), usageType == null ? "" : usageType.unit) : usageType,
				atg.getResourceGroup());
	}
	

}
