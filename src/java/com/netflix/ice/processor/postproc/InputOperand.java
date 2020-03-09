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
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.TagType;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.Zone;

public class InputOperand extends Operand {
	private static final List<TagType> allTagTypes = Lists.newArrayList(new TagType[]{
			TagType.Account, TagType.Region, TagType.Zone, TagType.Product, TagType.Operation, TagType.UsageType, TagType.ResourceGroup});
	
	protected final Aggregation aggregation;
	private boolean excludeAccount = false;
	private boolean excludeRegion = false;
	private boolean excludeZone = false;
	private boolean aggregates = false;
	private final boolean monthly;

	public InputOperand(OperandConfig opConfig, AccountService accountService) {
		super(opConfig, accountService);

		
		List<TagType> groupBy = Lists.newArrayList();
		if (opConfig.getGroupBy() == null) {
			groupBy = allTagTypes;
			aggregates = false;
		}
		else {
			for (String tagType: opConfig.getGroupBy()) {
				groupBy.add(TagType.valueOf(tagType));
			}
			aggregates = !groupBy.containsAll(allTagTypes);
		}
		
		this.aggregation = new Aggregation(groupBy);
		this.monthly = opConfig.getMonthly();
		
		List<String> exclude = opConfig.getExclude();
		if (exclude != null) {
			// If we're using the exclude feature on any of the lists, then we're aggregating
			for (String tagType: exclude) {
				switch (TagType.valueOf(tagType)) {
				case Account: excludeAccount = true; aggregates = true; break;
				case Region: excludeRegion = true; aggregates = true; break;
				case Zone: excludeZone = true; aggregates = true; break;
				default: break;
				}
			}
		}
		
		// If any of the lists has more than one value, then we're aggregating
		if (accounts != null && accounts.size() > 1)
			aggregates = true;
		if (regions != null && regions.size() > 1)
			aggregates = true;
		if (zones != null && zones.size() > 1)
			aggregates = true;
	}
	
	public boolean hasAggregation() {
		return aggregates;
	}
	
	public boolean isMonthly() {
		return monthly;
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
	 * Get the TagGroup based on the AggregationTagGroup. Used when the operand is not an aggregation to directly
	 * determine the proper TagGroup.
	 */
	public TagGroup tagGroup(AggregationTagGroup atg, AccountService accountService, ProductService productService) throws Exception {
		if (hasAggregation())
			throw new Exception("Cannot compute TagGroup directly with an aggregation operand");
		
		Account account = (accounts != null && accounts.size() > 0) ? accounts.get(0) : atg.getAccount();
		Region region = (regions != null && regions.size() > 0) ? regions.get(0) : atg.getRegion();
		Zone zone = (zones != null && zones.size() > 0) ? zones.get(0) : atg.getZone();
		Product product = atg.getProduct();
		Operation operation = atg.getOperation();
		UsageType usageType = atg.getUsageType();
		ResourceGroup resourceGroup = atg.getResourceGroup();
		
		if (accountTagFilter != null) {
			String a = accountTagFilter.getTag(atg.getAccount() == null ? "" : atg.getAccount().name);
			account = accountService.getAccountById(a);
		}
		if (regionTagFilter != null) {
			String r = regionTagFilter.getTag(atg.getRegion() == null ? "" : atg.getRegion().name);
			region = Region.getRegionByName(r);
		}
		if (zoneTagFilter != null) {
			String z = zoneTagFilter.getTag(atg.getZone() == null ? "" : atg.getZone().name);
			zone = region.getZone(z);
		}
		if (productTagFilter != null) {
			String p = productTagFilter.getTag(atg.getProduct() == null ? "" : atg.getProduct().name);
			product = productService.getProductByServiceCode(p);
		}
		if (operationTagFilter != null) {
			String o = operationTagFilter.getTag(atg.getOperation() == null ? "" : atg.getOperation().name);
			operation = Operation.getOperation(o);
		}
		if (usageTypeTagFilter != null) {
			String ut = usageTypeTagFilter.getTag(atg.getUsageType() == null ? "" : atg.getUsageType().name);
			usageType = UsageType.getUsageType(ut, atg.getUsageType() == null ? null : atg.getUsageType().unit);
		}

		return TagGroup.getTagGroup(account, region, zone, product, operation, usageType, resourceGroup);
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
        else if (accounts == null && aggregation.contains(TagType.Account) && atg.getAccount() != tg.account) {
        	return false;
        }
        if (regionTagFilter != null) {
        	Region r = atg.getRegion();
        	if (r != null && !regionTagFilter.getTag(r.name).equals(tg.region.name))
        		return false;
        }
        else if (regions == null && aggregation.contains(TagType.Region) && atg.getRegion() != tg.region) {
        	return false;
        }
        if (zoneTagFilter != null) {
        	Zone z = atg.getZone();
        	if (z != null && !productTagFilter.getTag(z.name).equals(tg.zone.name))
        		return false;
        }
        else if (zones == null && aggregation.contains(TagType.Zone) && atg.getZone() != tg.zone) {
        	return false;
        }
        if (productTagFilter != null) {
        	Product p = atg.getProduct();
        	if (p != null && !productTagFilter.getTag(p.getServiceCode()).equals(tg.product.getServiceCode()))
        		return false;
        	if (!productTagFilter.matches(tg.product.getServiceCode()))
        		return false;
        }
        else if (aggregation.contains(TagType.Product) && atg.getProduct() != tg.product) {
        	return false;
        }
		if (operationTagFilter != null) {
			Operation o = atg.getOperation();
			if (o != null && !productTagFilter.getTag(o.name).equals(tg.operation.name))
				return false;
			if (!operationTagFilter.matches(tg.operation.name))
				return false;
		}
		else if (aggregation.contains(TagType.Operation) && atg.getOperation() != tg.operation) {
			return false;
		}
		if (usageTypeTagFilter != null) {
			UsageType ut = atg.getUsageType();
			if (ut != null && !usageTypeTagFilter.getTag(ut.name).equals(tg.usageType.name))
				return false;
			if (!usageTypeTagFilter.matches(tg.usageType.name))
				return false;
		}
		else if (aggregation.contains(TagType.UsageType) && atg.getUsageType() != tg.usageType) {
			return false;
		}
		if (aggregation.contains(TagType.ResourceGroup) && atg.getResourceGroup() != tg.resourceGroup)
			return false;
		return true;
	}
	
	public String cacheKey(AggregationTagGroup atg) {
		StringBuilder sb = new StringBuilder(32);
		sb.append(type.name() + ",");
        if (accounts != null && accounts.size() > 0) {
        	sb.append(accounts.toString() + "," + (excludeAccount ? "true," : "false,"));
        }
        else if (accountTagFilter != null && atg.getAccount() != null) {
			sb.append(accountTagFilter.getTag(atg.getAccount().getId()) + ",");
		}
        else if (aggregation.contains(TagType.Account) && atg.types.contains(TagType.Account)) {
        	sb.append(atg.getAccount().getId() + ",");
        }
        
        if (regions != null && regions.size() > 0) {
        	sb.append(regions.toString() + "," + (excludeRegion ? "true," : "false,"));
        }
        else if (regionTagFilter != null && atg.getRegion() != null) {
			sb.append(regionTagFilter.getTag(atg.getRegion().name) + ",");
		}
        else if (aggregation.contains(TagType.Region) && atg.types.contains(TagType.Region)) {
        	sb.append(atg.getRegion().name + ",");
        }
        
        if (zones != null && zones.size() > 0) {
        	sb.append(zones.toString() + "," + (excludeZone ? "true," : "false,"));
        }
        else if (zoneTagFilter != null && atg.getZone() != null) {
			sb.append(zoneTagFilter.getTag(atg.getZone().name) + ",");
		}
        else if (aggregation.contains(TagType.Zone) && atg.types.contains(TagType.Zone)) {
        	sb.append(atg.getZone() == null ? "null," : atg.getZone().name + ",");
        }
        
		if (productTagFilter != null && atg.getProduct() != null) {
			sb.append(productTagFilter.getTag(atg.getProduct().name) + ",");
		}
		else if (aggregation.contains(TagType.Product) && atg.types.contains(TagType.Product)) {
			sb.append(atg.getProduct().getServiceCode() + ",");
		}
		
		if (operationTagFilter != null && atg.getOperation() != null) {
			sb.append(operationTagFilter.getTag(atg.getOperation().name) + ",");
		}
		else if (aggregation.contains(TagType.Operation) && atg.types.contains(TagType.Operation)) {
			sb.append(atg.getOperation().name + ",");
		}
		
		if (usageTypeTagFilter != null && atg.getUsageType() != null) {
			sb.append(usageTypeTagFilter.getTag(atg.getUsageType().name) + ",");
		}
		else if (aggregation.contains(TagType.UsageType) && atg.types.contains(TagType.UsageType)) {
			sb.append(atg.getUsageType().name + ",");
		}
		
		if (aggregation.contains(TagType.ResourceGroup) && atg.types.contains(TagType.ResourceGroup)) {
			sb.append(atg.getResourceGroup() == null ? "null" : atg.getResourceGroup().name);
		}
		
		return sb.toString();
	}
	
}
