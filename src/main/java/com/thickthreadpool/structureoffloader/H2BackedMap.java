package com.thickthreadpool.structureoffloader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class H2BackedMap<K, V> implements Map<K, V>
{
	private int size;
	private final Connection connection;

	public H2BackedMap(String dbPath)
	{
		try
		{
			this.connection = DriverManager.getConnection("jdbc:h2:" + dbPath);
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}

		initH2();
	}

	private void initH2()
	{
		try
		{
			connection.createStatement().execute(
					"CREATE TABLE IF NOT EXISTS map_table (k VARCHAR PRIMARY KEY, v " + "VARCHAR)");
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	@Override
	public int size()
	{
		return size;
	}

	@Override
	public boolean isEmpty()
	{
		return size == 0;
	}

	@Override
	public boolean containsKey(final Object key)
	{
		return get(key) != null;
	}

	@Override
	public boolean containsValue(final Object value)
	{
		try{
			var ps = connection.prepareStatement("SELECT COUNT(*) AS count FROM map_table WHERE v = ?");
			ps.setObject(1, value);
			var rs = ps.executeQuery();
			if(rs.next()){
				return rs.getInt("count") > 0;
			}
			return false;
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	@Override
	public V get(final Object key)
	{
		try
		{
			var ps = connection.prepareStatement("SELECT v FROM map_table WHERE k = ?");
			ps.setObject(1, key);
			var rs = ps.executeQuery();
			if (rs.next())
			{
				return (V) rs.getObject("v");
			}
			return null;
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	@Override
	public Object put(final Object key, final Object value)
	{
		try
		{
			var ps = connection.prepareStatement("MERGE INTO map_table (k, v) VALUES (?, ?)");
			ps.setObject(1, key);
			ps.setObject(2, value);
			ps.execute();

			size++;
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
		return key;
	}

	@Override
	public V remove(final Object key)
	{
		var v = get(key);

		try
		{
			var ps = connection.prepareStatement("DELETE FROM map_table WHERE k = ?");
			ps.setObject(1, key);
			ps.execute();

			size--;
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
		return v;
	}

	@Override
	public void putAll(final Map m)
	{
		for (Object key : m.keySet())
		{
			put(key, m.get(key));
		}
	}

	@Override
	public void clear()
	{
		try
		{
			connection.createStatement().execute("DELETE FROM map_table");
			size = 0;
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	@Override
	public Set keySet()
	{
		try
		{
			var ps = connection.prepareStatement("SELECT k FROM map_table");
			var rs = ps.executeQuery();
			Set<K> keys = new java.util.HashSet<>();
			while (rs.next())
			{
				keys.add((K) rs.getObject("k"));
			}
			return keys;
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	@Override
	public Collection<V> values()
	{
		try{
			var ps = connection.prepareStatement("SELECT v FROM map_table");
			var rs = ps.executeQuery();
			List<V> values = new java.util.ArrayList<>();
			while (rs.next())
			{
				values.add((V) rs.getObject("v"));
			}
			return values;
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	@Override
	public Set<Entry<K, V>> entrySet()
	{
		return Set.of();
	}

	@Override
	public boolean equals(final Object o)
	{
		return this == o;
	}

	@Override
	public int hashCode()
	{
		return System.identityHashCode(this);
	}
}