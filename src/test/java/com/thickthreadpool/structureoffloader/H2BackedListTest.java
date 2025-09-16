package com.thickthreadpool.structureoffloader;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class H2BackedListTest
{

	@BeforeAll
	public static void cleanup()
	{
		H2BackedList<String> list = new H2BackedList<>("./testdb");
		list.clear();
	}

	@Test
	public void addAndGetTest()
	{
		H2BackedList<String> list = new H2BackedList<>("./testdb");
		list.add("value1");
		list.add("value2");

		assert (list.get(0).equals("value1"));
		assert (list.get(1).equals("value2"));
	}

	@Test
	public void removeTest()
	{
		H2BackedList<String> list = new H2BackedList<>("./testdb");
		list.add("value1");
		list.add("value2");
		list.add("value3");

		list.remove(1);

		assert (list.size() == 2);
		assert (list.get(0).equals("value1"));
		assert (list.get(1).equals("value3"));
	}

	@Test
	public void retainAllTest()
	{
		H2BackedList<String> list = new H2BackedList<>("./testdb");

		list.add("value1");
		list.add("value2");
		list.add("value3");

		H2BackedList<String> list2 = new H2BackedList<>("./secondb");

		list2.add("value2");

		list.retainAll(list2);

		assert (list.get(0).equals("value2"));
	}
}
