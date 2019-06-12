/*
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.ice.basic;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.processor.AccountConfig;
import com.netflix.ice.tag.Account;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class BasicAccountService implements AccountService {

    Logger logger = LoggerFactory.getLogger(getClass());

    // Keep the accounts in static maps. TagGroups have a cache and
    // we want fast object comparisons.
    private static Map<String, Account> accountsById = Maps.newConcurrentMap();
    private static Map<String, Account> accountsByName = Maps.newConcurrentMap();
    private Map<Account, Set<String>> reservationAccounts = Maps.newHashMap();
    private Map<Account, String> reservationAccessRoles = Maps.newHashMap();
    private Map<Account, String> reservationAccessExternalIds = Maps.newHashMap();
    
    // Constructor used by the processor
    public BasicAccountService(Map<String, AccountConfig> configs) {
    	for (AccountConfig a: configs.values()) {
			Account account = new Account(a.id, a.name, a.awsName);
			accountsByName.put(a.name, account);
			accountsById.put(a.id, account);
			if (a.riProducts != null && a.riProducts.size() > 0) {
				reservationAccounts.put(account, Sets.newHashSet(a.riProducts));
			}
			if (!StringUtils.isEmpty(a.role)) {
				reservationAccessRoles.put(account,  a.role);
			}
			if (!StringUtils.isEmpty(a.externalId)) {
				reservationAccessExternalIds.put(account, a.externalId);
			}
    	}
    }

    // Constructor used by the reader - initialized from the work bucket data config
    // Also used by unit test code.
    public BasicAccountService(List<Account> accounts) {
    	for (Account a: accounts) {
    		accountsById.put(a.id, a);
    		accountsByName.put(a.name, a);
    	}
    	// Reservation maps are not used by the reader.
    }
    
    // Used by test code
    public BasicAccountService() {}
    
    // Accounts for the reader are refreshed from the work bucket data config after each processor run
    public void updateAccounts(List<Account> accounts) {
     	// Run through the account list and update our maps
    	for (Account a: accounts) {
    		Account existingId = accountsById.get(a.id);
    		if (existingId == null || !existingId.name.equals(a.name)) {
    			// Add the new account
    			accountsById.put(a.id, a);
    			accountsByName.put(a.name, a);
    		}
    	}
		// We should never get conflicts on IDs and account IDs should never go away,
		// but we do want to clean up old names due to name changes
    	for (String name: accountsByName.keySet()) {
    		boolean found = false;
    		for (Account a: accounts) {
    			if (name.equals(a.name)) {
    				found = true;
    				break;
    			}
    		}
    		if (!found) {
    			accountsByName.remove(name);
    		}
    	}
    }

    public Account getAccountById(String accountId) {
        Account account = accountsById.get(accountId);
        if (account == null) {
            account = new Account(accountId, accountId);
            accountsByName.put(account.name, account);
            accountsById.put(account.id, account);
            logger.info("getAccountById() created account " + accountId + "=\"" + account.name + "\".");
        }
        return account;
    }
    
    public Account getAccountByName(String accountName) {
        Account account = accountsByName.get(accountName);
        // for accounts that were not mapped to names in ice.properties (ice.account.xxx), this check will make sure that
        // data/tags are updated properly once the mapping is established in ice.properties
        if (account == null) {
            account = accountsById.get(accountName);
        }
        if (account == null) {
            account = new Account(accountName, accountName);
            accountsByName.put(account.name, account);
            accountsById.put(account.id, account);
            logger.info("getAccountByName() created account " + accountName + ".");
        }
        return account;
    }

    public List<Account> getAccounts() {
        List<Account> result = Lists.newArrayList();
        for (Account a: accountsByName.values())
            result.add(a);
        return result;
    }

    public List<Account> getAccounts(List<String> accountNames) {
        List<Account> result = Lists.newArrayList();
        for (String name: accountNames)
            result.add(accountsByName.get(name));
        return result;
    }

    public Map<Account, Set<String>> getReservationAccounts() {
        return reservationAccounts;
    }

    public Map<Account, String> getReservationAccessRoles() {
        return reservationAccessRoles;
    }


    public Map<Account, String> getReservationAccessExternalIds() {
        return reservationAccessExternalIds;
    }
}
