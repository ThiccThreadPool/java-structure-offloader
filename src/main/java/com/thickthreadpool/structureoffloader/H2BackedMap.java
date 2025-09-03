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

	public H2BackedMap(String dbPath) throws SQLException
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
					"CREATE TABLE IF NOT EXISTS map_table (k VARCHAR PRIMARY KEY, v " + "VARCHAR)"); //
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
		return false;
	}

	@Override
	public boolean containsValue(final Object value)
	{
		return false;
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
		return null;
	}

	@Override
	public void putAll(final Map m)
	{

	}

	@Override
	public void clear()
	{

	}

	@Override
	public Set keySet()
	{
		return Set.of();
	}

	@Override
	public Collection values()
	{
		return List.of();
	}

	@Override
	public Set<Entry<K, V>> entrySet()
	{
		return Set.of();
	}

	@Override
	public boolean equals(final Object o)
	{
		return false;
	}

	@Override
	public int hashCode()
	{
		return 0;
	}
}