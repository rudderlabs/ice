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
package com.netflix.ice.processor;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.PurchaseOption;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.common.TagGroupSP;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;

public class SavingsPlanProcessor {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    private CostAndUsageData data;
    private AccountService accountService;
	
	public SavingsPlanProcessor(CostAndUsageData costAndUsageData, AccountService accountService) {
		this.data = costAndUsageData;
		this.accountService = accountService;
	}
	
	public void process(Product product) {
		if (!data.hasSavingsPlans())
			return;

    	logger.info("---------- Process " + data.getSavingsPlans().size() + " Savings Plans for " + (product == null ? "Non-resource" : product));

		ReadWriteData usageData = data.getUsage(product);
		ReadWriteData costData = data.getCost(product);

		if (usageData == null || costData == null) {
			logger.warn("   No data for " + product);
			return;
		}
		
		for (int i = 0; i < usageData.getNum(); i++) {
			// For each hour of usage...
			processHour(product, i, usageData, costData);
		}		
	}
	
	private void processHour(Product product, int hour, ReadWriteData usageData, ReadWriteData costData) {
	    Map<TagGroup, Double> usageMap = usageData.getData(hour);
		Map<String, SavingsPlan> savingsPlans = data.getSavingsPlans();

		List<TagGroupSP> spTagGroups = Lists.newArrayList();
	    for (TagGroup tagGroup: usageMap.keySet()) {
	    	if (!(tagGroup instanceof TagGroupSP) || (product != null && product != tagGroup.product) || !tagGroup.operation.isBonus())
	    		continue;
	    	
	    	spTagGroups.add((TagGroupSP) tagGroup);
	    }
	    	    
	    for (TagGroupSP bonusTg: spTagGroups) {	    	
	    	// Split the effective cost into recurring and amortization pieces if appropriate.
	    	SavingsPlan sp = savingsPlans.get(bonusTg.arn.name);
	    	
	    	if (sp == null) {
	    		logger.error("No savings plan in the map at hour " + hour + " for tagGroup: " + bonusTg);
	    		continue;
	    	}
	    	double cost = costData.remove(hour, bonusTg);
	    	double usage = usageData.remove(hour, bonusTg);
	    	
    		String accountId = sp.arn.getAccountId();
	    	if (sp.paymentOption != PurchaseOption.NoUpfront) {
	    		// Add amortization
	    		Operation amortOp = null;
	    		if (accountId.equals(bonusTg.account.getId())) {
	    			amortOp = Operation.getSavingsPlanAmortized(sp.paymentOption);
	    		}
	    		else {
	    			amortOp = Operation.getSavingsPlanBorrowedAmortized(sp.paymentOption);
	    			// Create Lent records for account that owns the savings plan
	        		TagGroup tg = TagGroup.getTagGroup(accountService.getAccountById(accountId), bonusTg.region, bonusTg.zone, bonusTg.product, Operation.getSavingsPlanLentAmortized(sp.paymentOption), bonusTg.usageType, bonusTg.resourceGroup);
	    	    	add(costData, hour, tg, cost * sp.normalizedAmortization);
	    		}	    		
	    		
	    		TagGroup tg = TagGroup.getTagGroup(bonusTg.account, bonusTg.region, bonusTg.zone, bonusTg.product, amortOp, bonusTg.usageType, bonusTg.resourceGroup);
	    		add(costData, hour, tg, cost * sp.normalizedAmortization);
	    	}
	    	
    		Operation op = null;
    		if (accountId.equals(bonusTg.account.getId())) {
    			op = Operation.getSavingsPlanUsed(sp.paymentOption);
    		}
    		else {
    			op = Operation.getSavingsPlanBorrowed(sp.paymentOption);
    			
    			// Create Lent records for account that owns the savings plan
        		TagGroup tg = TagGroup.getTagGroup(accountService.getAccountById(accountId), bonusTg.region, bonusTg.zone, bonusTg.product, Operation.getSavingsPlanLent(sp.paymentOption), bonusTg.usageType, bonusTg.resourceGroup);
        		add(usageData, hour, tg, usage);
        		// Output cost for all payment types (including all upfront which is 0 so that they get into the tag db)
    	    	add(costData, hour, tg, cost * sp.normalizedRecurring);
    		}
    		
    		TagGroup tg = TagGroup.getTagGroup(bonusTg.account, bonusTg.region, bonusTg.zone, bonusTg.product, op, bonusTg.usageType, bonusTg.resourceGroup);
    		add(usageData, hour, tg, usage);
    		// Output cost for all payment types (including all upfront which is 0 so that they get into the tag db)
	    	add(costData, hour, tg, cost * sp.normalizedRecurring);
	    }
	}
	
	private void add(ReadWriteData data, int hour, TagGroup tg, double value) {
		Double amount = data.get(hour, tg);
		if (amount == null)
			amount = 0.0;
		amount += value;
		data.put(hour, tg, amount);
	}
}
