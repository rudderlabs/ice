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
import com.netflix.ice.common.AccountService;
import com.netflix.ice.tag.Account;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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

    // Used only for testing
    public BasicAccountService(List<Account> accounts,
    			Map<Account, Set<String>> reservationAccounts,
                Map<Account, String> reservationAccessRoles,
                Map<Account, String> reservationAccessExternalIds) {
        this.reservationAccounts = reservationAccounts;
        this.reservationAccessRoles = reservationAccessRoles;
        this.reservationAccessExternalIds = reservationAccessExternalIds;
        if (accounts != null) {
	        for (Account account: accounts) {
	            accountsByName.put(account.name, account);
	            accountsById.put(account.id, account);
	        }
        }
    }
    
    /*
     * Create an AccountService instance based on values from a Properties object (typically ice.properties file)
     * 
     * The following property key values are used:
     * 
     * Account definition:
     * 		name:	account name
     * 		id:		account id
     * 
     *	ice.account.{name}={id}
     *
     *		example: ice.account.myAccount=123456789012
     * 
     * Reservation Owner Account
     * 		name: account name
	 *		product: codes for products with purchased reserved instances. Possible values are ec2, rds, redshift
     * 
     *	ice.owneraccount.{name}={products}
     *
     *		example: ice.owneraccount.resHolder=ec2,rds
     *
     * Reservation Owner Account Role
     * 		name: account name
     * 		role: IAM role name to assume when pulling reservations from an owner account
     * 
     * 	ice.owneraccount.{name}.role={role}
     * 
     * 		example: ice.owneraccount.resHolder.role=ice
     * 
     * Reservation Owner Account ExternalId
     * 		name: account name
     * 		externalId: external ID for the reservation owner account
     * 
     * 	ice.owneraccount.{name}.externalId={externalId}
     * 
     * 		example: ice.owneraccount.resHolder.externalId=112233445566
     */

    public BasicAccountService(Properties properties, Map<String, String> defaultNames) {
    	init(properties);
    	// Add any additional accounts not specified in the properties
    	for (String id: defaultNames.keySet()) {
    		if (!accountsById.containsKey(id)) {
    			Account a = new Account(id, defaultNames.get(id));
    			accountsByName.put(a.name, a);
    			accountsById.put(a.id, a);
    		}
    	}
    }

    // Used by test code
    public BasicAccountService(Properties properties) {
    	init(properties);
    }
    
    private void init(Properties properties) {
        for (String name: properties.stringPropertyNames()) {
            if (name.startsWith("ice.account.")) {
                String accountName = name.substring("ice.account.".length());
                Account account = new Account(properties.getProperty(name), accountName);
                
                // Only add accounts if they don't exist already so that we can
                // support concurrent JUnit tests with same account data.
                // TagGroup cache needs this.
                Account existing = accountsByName.get(accountName);
                if (existing == null || !existing.equals(account)) {
                	accountsByName.put(accountName, account);
                	accountsById.put(account.id, account);
                }
            }
        }
        for (String name: properties.stringPropertyNames()) {
            if (name.startsWith("ice.owneraccount.") && !name.endsWith(".role") && !name.endsWith(".externalId")) {					
                String accountName = name.substring("ice.owneraccount.".length());
				String[] products = properties.getProperty(name).split(",");
				Set<String> productSet = new HashSet<String>();
				for (String product: products) {
					productSet.add(product);
				}
				reservationAccounts.put(accountsByName.get(accountName), productSet);
				
                String role = properties.getProperty(name + ".role", "");
                reservationAccessRoles.put(accountsByName.get(accountName), role);

                String externalId = properties.getProperty(name + ".externalId", "");
                reservationAccessExternalIds.put(accountsByName.get(accountName), externalId);					
            }
        }
    }
    
    // Constructor used by the reader - initialized from the work bucket data config
    public BasicAccountService(List<Account> accounts) {
    	for (Account a: accounts) {
    		accountsById.put(a.id, a);
    		accountsByName.put(a.name, a);
    	}
    	// Reservation maps are not used by the reader.
    }
    
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
