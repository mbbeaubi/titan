package com.thinkaurelius.titan.diskstorage.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

public class HTable1 implements TableMask
{
	private final Connection connection;
	private volatile RegionLocator locator = null;
    private final Table table;

    public HTable1(Connection connection, Table table) {
    	this.table = table;
    	this.connection = connection;
    }

    @Override
    public ResultScanner getScanner(Scan filter) throws IOException
    {
        return table.getScanner(filter);
    }

    @Override
    public Result[] get(List<Get> gets) throws IOException
    {
        return table.get(gets);
    }

    @Override
    public void batch(List<Row> writes, Object[] results) throws IOException, InterruptedException
    {
        table.batch(writes, results);
        /* table.flushCommits(); not needed anymore */
    }

    @Override
    public void close() throws IOException
    {
    	IOException ioEx = null;
    	RuntimeException rEx = null;
    	try {
    		if (locator != null)
    			locator.close();
    	} catch (IOException e) {
    		e.printStackTrace();
    		ioEx = e;
    	} catch (RuntimeException e) {
    		e.printStackTrace();
    		rEx = e;
    	}
    	
    	try {
    		table.close();
    	} catch (IOException e) {
    		e.printStackTrace();
    		ioEx = e;
    	} catch (RuntimeException e) {
    		e.printStackTrace();
    		rEx = e;
    	}
    	
    	if (ioEx != null) {
    		throw ioEx;
    	}
    	
    	if (rEx != null) {
    		throw rEx;
    	}
    }

	@Override
	public CloseableTreeMap<HRegionInfo, ServerName> getRegionLocations() throws IOException {
		CloseableTreeMap<HRegionInfo, ServerName> map = new CloseableTreeMap<HRegionInfo, ServerName>();
		
		for (HRegionLocation loc : getLocator().getAllRegionLocations()) {
			HRegionInfo regionInfo = loc.getRegionInfo();
			ServerName serverName = loc.getServerName();
			map.put(regionInfo, serverName);
		}
		
		return map;
	}
	
	private RegionLocator getLocator() throws IOException {
		if (locator == null) {
			synchronized(this) {
				if (locator == null) {
					locator = connection.getRegionLocator(table.getName());
				}
			}
		}
		return locator;
	}
    
}
