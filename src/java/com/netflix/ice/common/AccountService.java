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
package com.netflix.ice.common;

import com.netflix.ice.tag.Account;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface AccountService {
    /**
     * Get account by AWS id. The AWS id is usually an un-readable 12 digit string.
     * Will create an account object if it doesn't already exist.
     * @param accountId
     * @return Account object associated with the account id
     */
    Account getAccountById(String accountId, String root);

    /**
     * Get account by AWS id. The AWS id is usually an un-readable 12 digit string.
     * @param accountId
     * @return Account object associated with the account id if it already exists, null otherwise.
     */
    Account getAccountById(String accountId);

    /**
     * Get account by account name. The account name is a user defined readable string.
     * @param accountName
     * @return Account object associated with the account name
     */
    Account getAccountByName(String accountName);

    /**
     * Get a list of accounts from given account names.
     * @param accountNames
     * @return List of accounts
     */
    List<Account> getAccounts(List<String> accountNames);

    /**
     * Get the list of accounts.
     * @return List of accounts
     */
    List<Account> getAccounts();

    /**
     * Get a map of accounts containing the products that each holds reservations for.
     * @return Map of account products with reservations. The keys are reservation owner accounts,
     * the values are product names for which the account holds reservations.
     */
    Map<Account, Set<String>> getReservationAccounts();

    /**
     * If you don't need to poll reservation capacity through ec2 API for other accounts, you can return an empty map.
     * @return Map of account access roles. The keys are reservation owner accounts,
     * the values are assumed roles to call ec2 describeReservedInstances on each reservation owner account.
     */
    Map<Account, String> getReservationAccessRoles();

    /**
     * If you don't need to poll reservation capacity through ec2 API for other accounts, or if you don't use external ids,
     * you can return an empty map.
     * @return Map of account access external ids. The keys are reservation owner accounts,
     * the values are external ids to call ec2 describeReservedInstances on each reservation owner account.
     */
    Map<Account, String> getReservationAccessExternalIds();
}
