package com.alibaba.datax.plugin.reader.hbasereader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;

public final class HTableFactory {

	private static HTable HTABLE = null;

	private static HBaseAdmin ADMIN = null;

	public static synchronized HTable createHTable(Configuration config, String tableName)
			throws IOException {
		if (HTABLE == null) {
			HTABLE = new HTable(config, tableName);
		}
		return HTABLE;
	}

	public static synchronized HBaseAdmin createHBaseAdmin(Configuration config) throws IOException {
		if (ADMIN == null) {
			ADMIN = new HBaseAdmin(config);
		}
		return ADMIN;
	}

	public synchronized static void closeHtable() throws IOException {
		if (null != HTABLE) {
			HTABLE.close();
			HTABLE = null;
		}
	}
}
