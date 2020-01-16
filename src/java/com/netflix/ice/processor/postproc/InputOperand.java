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

import java.util.List;

import com.google.common.collect.Lists;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.Aggregation;
import com.netflix.ice.common.AggregationTagGroup;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.TagType;
import com.netflix.ice.tag.UsageType;

public class InputOperand extends Operand {
	protected Aggregation aggregation;
	private boolean excludeAccount = false;
	private boolean excludeRegion = false;
	private boolean excludeZone = false;

	public InputOperand(OperandConfig opConfig, AccountService accountService) {
		super(opConfig, accountService);

		
		List<TagType> groupBy = Lists.newArrayList(new TagType[]{
				TagType.Account, TagType.Region, TagType.Zone, TagType.Product, TagType.Operation, TagType.UsageType, TagType.ResourceGroup
		});
		List<String> aggregate = opConfig.getAggregate();
		if (aggregate != null) {
			for (String tagType: aggregate) {
				groupBy.remove(TagType.valueOf(tagType));
			}
		}
		
		this.aggregation = new Aggregation(groupBy);
		
		List<String> exclude = opConfig.getExclude();
		if (exclude != null) {
			for (String tagType: exclude) {
				switch (TagType.valueOf(tagType)) {
				case Account: excludeAccount = true; break;
				case Region: excludeRegion = true; break;
				case Zone: excludeZone = true; break;
				default: break;
				}
			}
		}
	}
	
	/**
	 * Used by the In operand when aggregating the input data
	 */
	public AggregationTagGroup aggregateTagGroup(TagGroup tg, ProductService productService) throws Exception {
		Product product = tg.product;
		Operation operation = tg.operation;
		UsageType usageType = tg.usageType;
		
		// Handle filtering
        if (accounts != null && accounts.size() > 0 && !(accounts.contains(tg.account) ^ excludeAccount)) {
        		return null;
        }
        if (regions != null && regions.size() > 0 && !(regions.contains(tg.region) ^ excludeRegion)) {
        	return null;
        }
        if (zones != null && zones.size() > 0 && !(zones.contains(tg.zone) ^ excludeZone)) {
        	return null;
        }
        if (productTagFilter != null) {
        	String p = productTagFilter.getGroup(tg.product.name);
        	if (p == null)
        		return null;
        	product = p.isEmpty() ? null : productService.getProductByServiceCode(p);
        }
        if (operationTagFilter != null) {
        	String op = operationTagFilter.getGroup(tg.operation.name);
        	if (op == null)
        		return null;
        	
        	operation = op.isEmpty() ? null : Operation.getOperation(op);
        }
        if (usageTypeTagFilter != null) {
        	String ut = usageTypeTagFilter.getGroup(tg.usageType.name);
        	if (ut == null)
        		return null;
        	
        	usageType = ut.isEmpty() ? null : UsageType.getUsageType(ut, tg.usageType.unit);
        }
        
		return aggregation.getAggregationTagGroup(tg.account, tg.region, tg.zone, product, operation, usageType, tg.resourceGroup);
	}
	
	/**
	 *  Determine if the supplied TagGroup is a match for the given input aggregation.
	 */
	public boolean matches(AggregationTagGroup atg, TagGroup tg) {
        if (accounts != null && accounts.size() > 0 && !accounts.contains(tg.account)) {
        	return false;
        }
        if (regions != null && regions.size() > 0 && !regions.contains(tg.region)) {
        	return false;
        }
        if (zones != null && zones.size() > 0 && !zones.contains(tg.zone)) {
        	return false;
        }
        if (productTagFilter != null) {
        	Product p = atg.getProduct();
        	if (p != null && !productTagFilter.getTag(p.name).equals(tg.product.name))
        		return false;
        }
        else if (atg.getProduct() != tg.product) {
        	return false;
        }
		if (operationTagFilter != null) {
			Operation o = atg.getOperation();
			if (o != null && !productTagFilter.getTag(o.name).equals(tg.operation.name))
				return false;
		}
		else if (atg.getOperation() != tg.operation) {
			return false;
		}
		if (usageTypeTagFilter != null) {
			UsageType ut = atg.getUsageType();
			if (ut != null && !usageTypeTagFilter.getTag(ut.name).equals(tg.usageType.name))
				return false;
		}
		else if (atg.getUsageType() != tg.usageType) {
			return false;
		}
		return true;
	}
}
