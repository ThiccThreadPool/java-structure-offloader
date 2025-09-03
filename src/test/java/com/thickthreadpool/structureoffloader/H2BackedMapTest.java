package com.thickthreadpool.structureoffloader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class H2BackedMapTest
{

	@AfterEach
	public void cleanup()
	{
		H2BackedMap<String, String> map = new H2BackedMap<>("./testdb");
		map.clear();
	}

	@Test
	public void getFromEmptyMapTest()
	{
		H2BackedMap<String, String> map = new H2BackedMap<>("./testdb");
		assertNull(map.get("whatever"));
	}

	@Test
	public void getFromNotEmptyMapTest()
	{
		H2BackedMap<Integer, String> map = new H2BackedMap<>("./testdb");
		map.put(100, "test");
		map.put(101, "test1");

		assertEquals("test1", map.get(101));
	}

	@Test
	public void putAndGetTest()
	{
		H2BackedMap<String, String> map = new H2BackedMap<>("./testdb");
		map.put("key", "value");
		assertEquals("value", map.get("key"));
	}

	@Test
	public void clearTest()
	{
		H2BackedMap<String, String> map = new H2BackedMap<>("./testdb");

		map.put("testk", "testv");
		map.put("testk1", "tesktv2");

		map.clear();

		assertNull(map.get("testk"));
		assertNull(map.get("testk1"));
	}

	@Test
	public void getSizeTest(){
		H2BackedMap<String, String> map = new H2BackedMap<>("./testdb");

		map.put("testk", "testv");
		map.put("testk1", "tesktv2");

		assertEquals(2, map.size());
	}

	@Test
	public void removeTest(){
		H2BackedMap<String, String> map = new H2BackedMap<>("./testdb");

		map.put("testk", "testv");
		map.put("testk1", "tesktv2");

		assertEquals("testv", map.remove("testk"));
		assertNull( map.get("testk"));
		assertEquals(1, map.size());
	}

	@Test
	public void putAllTest(){
		H2BackedMap<String, String> map = new H2BackedMap<>("./testdb");

		Map<String, String> toPut = new HashMap<>();
		toPut.put("testk", "testv");
		toPut.put("testk1", "tesktv2");
		toPut.put("testk3", "tesktv3");


		map.putAll(toPut);

		assertEquals("testv", map.get("testk"));
		assertEquals("tesktv2", map.get("testk1"));
		assertEquals(3, map.size());
	}


}
