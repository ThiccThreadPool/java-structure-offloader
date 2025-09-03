package com.thickthreadpool.structureoffloader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class H2BackedList<E> implements List<E>
{
	private Connection connection;
	private int size;

	public H2BackedList(String dbPath)
	{
		try
		{
			connection = DriverManager.getConnection("jdbc:h2:" + dbPath);
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
					"CREATE TABLE IF NOT EXISTS list_table (id INT PRIMARY KEY AUTO_INCREMENT, v " + "VARCHAR)");
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
		return size==0;
	}

	@Override
	public boolean contains(final Object o)
	{
		try{
			var ps = connection.prepareStatement("SELECT COUNT(*) FROM list_table WHERE v = ?");
			ps.setObject(1, o);
			var rs = ps.executeQuery();
			if(rs.next())
			{
				return rs.getInt(1) > 0;
			}
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
		return false;
	}

	@Override
	public Iterator<E> iterator()
	{
		return null;
	}

	@Override
	public Object[] toArray()
	{
		return new Object[0];
	}

	@Override
	public <T> T[] toArray(final T[] a)
	{
		return null;
	}

	@Override
	public boolean add(final E e)
	{
		return false;
	}

	@Override
	public boolean remove(final Object o)
	{
		return false;
	}

	@Override
	public boolean containsAll(final Collection<?> c)
	{
		return false;
	}

	@Override
	public boolean addAll(final Collection<? extends E> c)
	{
		return false;
	}

	@Override
	public boolean addAll(final int index, final Collection<? extends E> c)
	{
		return false;
	}

	@Override
	public boolean removeAll(final Collection<?> c)
	{
		return false;
	}

	@Override
	public boolean retainAll(final Collection<?> c)
	{
		return false;
	}

	@Override
	public void clear()
	{

	}

	@Override
	public E get(final int index)
	{
		return null;
	}

	@Override
	public E set(final int index, final E element)
	{
		return null;
	}

	@Override
	public void add(final int index, final E element)
	{

	}

	@Override
	public E remove(final int index)
	{
		return null;
	}

	@Override
	public int indexOf(final Object o)
	{
		return 0;
	}

	@Override
	public int lastIndexOf(final Object o)
	{
		return 0;
	}

	@Override
	public ListIterator<E> listIterator()
	{
		return null;
	}

	@Override
	public ListIterator<E> listIterator(final int index)
	{
		return null;
	}

	@Override
	public List<E> subList(final int fromIndex, final int toIndex)
	{
		return List.of();
	}
}
