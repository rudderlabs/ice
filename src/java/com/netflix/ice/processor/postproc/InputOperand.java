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
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.TagType;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.Zone;

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
	public AggregationTagGroup aggregateTagGroup(TagGroup tg, AccountService accountService, ProductService productService) throws Exception {
		Account account = tg.account;
		Region region = tg.region;
		Zone zone = tg.zone;
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
        if (accountTagFilter != null) {
        	String a = accountTagFilter.getGroup(tg.account.getId());
        	if (a == null)
        		return null;
        	account = a.isEmpty() ? null : accountService.getAccountById(a);
        }
        if (regionTagFilter != null) {
        	String r = regionTagFilter.getGroup(tg.region.name);
        	if (r == null)
        		return null;
        	region = r.isEmpty() ? null : Region.getRegionByName(r);
        }
        if (zoneTagFilter != null) {
        	String z = zoneTagFilter.getGroup(tg.zone.name);
        	if (z == null)
        		return null;
        	zone = z.isEmpty() ? null : region.getZone(z);
        }
        if (productTagFilter != null) {
        	String p = productTagFilter.getGroup(tg.product.getServiceCode());
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
        
		return aggregation.getAggregationTagGroup(account, region, zone, product, operation, usageType, tg.resourceGroup);
	}
	
	/**
	 *  Determine if the supplied TagGroup is a match for the given input aggregation.
	 */
	public boolean matches(AggregationTagGroup atg, TagGroup tg) {
        if (accounts != null && accounts.size() > 0 && !(accounts.contains(tg.account) ^ excludeAccount)) {
        	return false;
        }
        if (regions != null && regions.size() > 0 && !(regions.contains(tg.region) ^ excludeRegion)) {
        	return false;
        }
        if (zones != null && zones.size() > 0 && !(zones.contains(tg.zone) ^ excludeZone)) {
        	return false;
        }
        if (accountTagFilter != null) {
        	Account a = atg.getAccount();
        	if (a != null && !accountTagFilter.getTag(a.getId()).equals(tg.account.getId()))
        		return false;
        }
        else if (accounts == null && atg.getAccount() != tg.account) {
        	return false;
        }
        if (regionTagFilter != null) {
        	Region r = atg.getRegion();
        	if (r != null && !regionTagFilter.getTag(r.name).equals(tg.region.name))
        		return false;
        }
        else if (regions == null && atg.getRegion() != tg.region) {
        	return false;
        }
        if (zoneTagFilter != null) {
        	Zone z = atg.getZone();
        	if (z != null && !productTagFilter.getTag(z.name).equals(tg.zone.name))
        		return false;
        }
        else if (zones == null && atg.getZone() != tg.zone) {
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
	
	public String key(AggregationTagGroup atg) {
		StringBuilder sb = new StringBuilder(32);
		sb.append(type.name() + ",");
        if (accounts != null && accounts.size() > 0) {
        	sb.append(accounts.toString() + "," + (excludeAccount ? "true," : "false,"));
        }
        else if (accountTagFilter != null && atg.getAccount() != null) {
			sb.append(accountTagFilter.getTag(atg.getAccount().getId()) + ",");
		}
        else if (atg.types.contains(TagType.Account)) {
        	sb.append(atg.getAccount().getId() + ",");
        }
        
        if (regions != null && regions.size() > 0) {
        	sb.append(regions.toString() + "," + (excludeRegion ? "true," : "false,"));
        }
        else if (regionTagFilter != null && atg.getRegion() != null) {
			sb.append(regionTagFilter.getTag(atg.getRegion().name) + ",");
		}
        else if (atg.types.contains(TagType.Region)) {
        	sb.append(atg.getRegion().name + ",");
        }
        
        if (zones != null && zones.size() > 0) {
        	sb.append(zones.toString() + "," + (excludeZone ? "true," : "false,"));
        }
        else if (zoneTagFilter != null && atg.getZone() != null) {
			sb.append(zoneTagFilter.getTag(atg.getZone().name) + ",");
		}
        else if (atg.types.contains(TagType.Zone)) {
        	sb.append(atg.getZone() == null ? "null," : atg.getZone().name + ",");
        }
        
		if (productTagFilter != null && atg.getProduct() != null) {
			sb.append(productTagFilter.getTag(atg.getProduct().name) + ",");
		}
		else if (atg.types.contains(TagType.Product)) {
			sb.append(atg.getProduct().getServiceCode() + ",");
		}
		
		if (operationTagFilter != null && atg.getOperation() != null) {
			sb.append(operationTagFilter.getTag(atg.getOperation().name) + ",");
		}
		else if (atg.types.contains(TagType.Operation)) {
			sb.append(atg.getOperation().name + ",");
		}
		
		if (usageTypeTagFilter != null && atg.getUsageType() != null) {
			sb.append(usageTypeTagFilter.getTag(atg.getUsageType().name) + ",");
		}
		else if (atg.types.contains(TagType.UsageType)) {
			sb.append(atg.getUsageType().name + ",");
		}
		
		if (atg.types.contains(TagType.ResourceGroup)) {
			sb.append(atg.getResourceGroup() == null ? "null" : atg.getResourceGroup().name);
		}
		
		return sb.toString();
	}
	
}
