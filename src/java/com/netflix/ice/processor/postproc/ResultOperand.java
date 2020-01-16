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

import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.AggregationTagGroup;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.UsageType;

public class ResultOperand extends Operand {

	public ResultOperand(OperandConfig opConfig, AccountService accountService) {
		super(opConfig, accountService);
	}
	
	/**
	 * Get the result TagGroup based on the supplied AggregationTagGroup. Values present in the operand config are used to
	 * override the values in the supplied TagGroup. For tags that have lists, there should only be one entry, so
	 * the first is always chosen.
	 */
	public TagGroup getTagGroup(AggregationTagGroup atg, ProductService productService) {
		Product product = atg.getProduct();
		Operation operation = atg.getOperation();
		UsageType usageType = atg.getUsageType();
		
		return TagGroup.getTagGroup(
				accounts.size() > 0 ? accounts.get(0) : atg.getAccount(),
				regions.size() > 0 ? regions.get(0) : atg.getRegion(),
				zones.size() > 0 ? zones.get(0) : atg.getZone(),
				productTagFilter != null ? productService.getProductByServiceCode(productTagFilter.getTag(product == null ? "" : product.name)) : product,
				operationTagFilter != null ? Operation.getOperation(operationTagFilter.getTag(operation == null ? "" : operation.name)) : operation,
				usageTypeTagFilter != null ? UsageType.getUsageType(usageTypeTagFilter.getTag(usageType == null ? "" : usageType.name), usageType == null ? "" : usageType.unit) : usageType,
				atg.getResourceGroup());
	}
	

}
