package com.thinkaurelius.titan.diskstorage.hbase;

import java.io.Closeable;
import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class CloseableTreeMap<K, V> extends TreeMap<K, V> implements Closeable {

	private static final long serialVersionUID = 1L;

	private final Closeable[] thingsToClose;
	
	public CloseableTreeMap(Closeable...closeables) {
		super();
		this.thingsToClose = closeables;
	}

	public CloseableTreeMap(Comparator<? super K> comparator, Closeable...closeables) {
		super(comparator);
		this.thingsToClose = closeables;
	}

	public CloseableTreeMap(Map<? extends K, ? extends V> m, Closeable...closeables) {
		super(m);
		this.thingsToClose = closeables;
	}

	public CloseableTreeMap(SortedMap<K, ? extends V> m, Closeable...closeables) {
		super(m);
		this.thingsToClose = closeables;
	}

	@Override
	public void close() throws IOException {
		if (thingsToClose == null) {
			return;
		}
		
		IOException ioEx = null;
		RuntimeException rEx = null;
		for (Closeable thing : thingsToClose) {
			try {
				thing.close();
			} catch (IOException e) {
				e.printStackTrace();
				ioEx = e;
			}
			catch (RuntimeException e) {
				e.printStackTrace();
				rEx = e;
			}
		}
		
		if (ioEx != null) {
			throw ioEx;
		}
		
		if (rEx != null) {
			throw rEx;
		}
	}
	
}
