package com.thickthreadpool.structureoffloader;

import java.util.HashMap;
import java.util.Map;

public class TestClass
{
	public static void main(String[] args)
	{
		try
		{
			H2BackedMap<String, String> map = new H2BackedMap<>("./testdb");
			Map<String, String> map2 = new HashMap<>();

			Thread.sleep(1000 * 30);

			for (int i = 0; i <= 100000; i++) {
				map2.put("key" + i, "value" + i);
			}

			Thread.sleep(1000 * 30);

			for(int i=0; i<=100000; i++){
				map.put("key"+i, "value"+i);
			}

			System.out.println("Fucking get the vaaals");

			System.out.println(map.get("key1"));

		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}



	}
}
