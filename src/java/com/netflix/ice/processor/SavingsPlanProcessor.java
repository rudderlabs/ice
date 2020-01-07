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
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.common.TagGroupSP;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Operation.SavingsPlanPaymentOption;

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

		for (int i = 0; i < usageData.getNum(); i++) {
			// For each hour of usage...
		    Map<TagGroup, Double> usageMap = usageData.getData(i);
		    Map<TagGroup, Double> costMap = costData.getData(i);

			processHour(product, i, usageMap, costMap);
		}
	}
	
	private void processHour(Product product, int hour, Map<TagGroup, Double> usageMap, Map<TagGroup, Double> costMap) {
		Map<String, SavingsPlan> savingsPlans = data.getSavingsPlans();

		List<TagGroupSP> spTagGroups = Lists.newArrayList();
	    for (TagGroup tagGroup: usageMap.keySet()) {
	    	if (!(tagGroup instanceof TagGroupSP) || (product != null && product != tagGroup.product) || !tagGroup.operation.isSavingsPlanBonus())
	    		continue;
	    	
	    	spTagGroups.add((TagGroupSP) tagGroup);
	    }
	    	    
	    for (TagGroupSP bonusTg: spTagGroups) {	    	
	    	// Split the effective cost into recurring and amortization pieces if appropriate.
	    	SavingsPlan sp = savingsPlans.get(bonusTg.getArn().name);
	    	
	    	if (sp == null) {
	    		logger.error("No savings plan in the map at hour " + hour + " for tagGroup: " + bonusTg);
	    		continue;
	    	}
	    	double cost = costMap.get(bonusTg);
	    	double usage = usageMap.get(bonusTg);
	    	
	    	if (sp.paymentOption != SavingsPlanPaymentOption.NoUpfront) {
	    		TagGroup tg = TagGroup.getTagGroup(bonusTg.account, bonusTg.region, bonusTg.zone, bonusTg.product, Operation.getSavingsPlanAmortized(sp.paymentOption), bonusTg.usageType, bonusTg.resourceGroup);
	    		add(costMap, tg, cost * sp.normalizedAmortization);
	    	}
	    	
    		Operation op = null;
    		String accountId = sp.arn.getAccountId();
    		if (accountId.equals(bonusTg.account.name)) {
    			op = Operation.getSavingsPlanUsed(sp.paymentOption);
    		}
    		else {
    			op = Operation.getSavingsPlanBorrowed(sp.paymentOption);
    			// Create Lent record for account that owns the savings plan
        		TagGroup tg = TagGroup.getTagGroup(accountService.getAccountById(accountId), bonusTg.region, bonusTg.zone, bonusTg.product, Operation.getSavingsPlanLent(sp.paymentOption), bonusTg.usageType, bonusTg.resourceGroup);
        		add(usageMap, tg, usage);
    	    	if (sp.paymentOption != SavingsPlanPaymentOption.AllUpfront) {
    	    		add(costMap, tg, cost * sp.normalizedRecurring);
    	    	}
    		}
	    	
    		TagGroup tg = TagGroup.getTagGroup(bonusTg.account, bonusTg.region, bonusTg.zone, bonusTg.product, op, bonusTg.usageType, bonusTg.resourceGroup);
    		add(usageMap, tg, usage);
	    	if (sp.paymentOption != SavingsPlanPaymentOption.AllUpfront) {
	    		add(costMap, tg, cost * sp.normalizedRecurring);
	    	}
    		
	    	costMap.remove(bonusTg);
	    	usageMap.remove(bonusTg);
	    }
	}
	
	private void add(Map<TagGroup, Double> map, TagGroup tg, double value) {
		Double amount = map.get(tg);
		if (amount == null)
			amount = 0.0;
		amount += value;
		map.put(tg, amount);
	}
}
