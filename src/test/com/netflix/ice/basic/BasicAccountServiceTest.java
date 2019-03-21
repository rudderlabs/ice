package com.netflix.ice.basic;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.tag.Account;

public class BasicAccountServiceTest {

	@Test
	public void testConstructor() {
		Properties props = new Properties();
		props.setProperty("ice.account.account1", "123456789012");
		Map<String, String> defaultNames = Maps.newHashMap();
		defaultNames.put("123456789012", "org_account1");
		defaultNames.put("234567890123", "org_account2");
		BasicAccountService bas = new BasicAccountService(props, defaultNames);
		
		assertEquals("Wrong name for account1 by ID", "account1", bas.getAccountById("123456789012").name);
		assertEquals("Wrong name for account2 by ID", "org_account2", bas.getAccountById("234567890123").name);
		assertEquals("Wrong name for account1 by name", "123456789012", bas.getAccountByName("account1").id);
		assertEquals("Wrong name for account2 by name", "234567890123", bas.getAccountByName("org_account2").id);
	}
	
	@Test
	public void testUpdateAccounts() {
		List<Account> accounts = Lists.newArrayList();
		String id = "123456789012";
		accounts.add(new Account(id, "OldName"));
		
		BasicAccountService bas = new BasicAccountService(accounts);
		
		assertEquals("Wrong number of accounts before update", 1, bas.getAccounts().size());
		assertNotNull("Missing account before update fetch by ID", bas.getAccountById(id));
		assertNotNull("Missing account before update fetch by Name", bas.getAccountByName("OldName"));
		assertEquals("Wrong account name before update", "OldName", bas.getAccountById(id).name);
		assertEquals("Wrong account id before update", id, bas.getAccountById(id).id);
		
		accounts = Lists.newArrayList();		
		accounts.add(new Account(id, "NewName"));
		
		bas.updateAccounts(accounts);
		assertEquals("Wrong number of accounts after update", 1, bas.getAccounts().size());
		assertNotNull("Missing account after update fetch by ID", bas.getAccountById(id));
		assertNotNull("Missing account after update fetch by Name", bas.getAccountByName("NewName"));
		assertEquals("Wrong account name after update", "NewName", bas.getAccountById(id).name);
		assertEquals("Wrong account id after update", id, bas.getAccountById(id).id);
		
	}

}
