package com.thinkaurelius.titan.diskstorage.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;

public class HTable0_94 implements TableMask
{
    private final HTableInterface table;
    private final Configuration hconf;

    public HTable0_94(HTableInterface table, Configuration hconf)
    {
        this.table = table;
        this.hconf = hconf;
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
        table.flushCommits();
    }

    @Override
    public void close() throws IOException
    {
        table.close();
    }

	@Override
	public CloseableTreeMap<HRegionInfo, ServerName> getRegionLocations() throws IOException {
		HTable htable = new HTable(hconf, table.getTableName());
		try {
			return new CloseableTreeMap<HRegionInfo, ServerName>(htable.getRegionLocations(), htable);
		} catch (IOException e) {
			try {
				htable.close();
			} catch (IOException e1) {}
			throw e;
		} catch (RuntimeException e) {
			try {
				htable.close();
			} catch (IOException e1) {}
			throw e;
		}
	}
    
    
}
