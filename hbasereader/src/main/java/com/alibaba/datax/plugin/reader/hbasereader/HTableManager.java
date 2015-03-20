package com.alibaba.datax.plugin.reader.hbasereader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.plugin.reader.hbasereader.util.HbaseUtil;

import java.io.IOException;

public final class HTableManager {
	private static Logger LOG = LoggerFactory.getLogger(HTableManager.class);
    public static HTable createHTable(Configuration config, String tableName)
            throws IOException {

        return new HTable(config, tableName);
    }

    public static HBaseAdmin createHBaseAdmin(Configuration config)
            throws IOException {
        return new HBaseAdmin(config);
    }

    public static void closeHTable(HTable hTable) throws IOException {
        if (hTable != null) {
            hTable.close();
            LOG.info("close table, table counter now is " + HbaseUtil.htableCounter.decrementAndGet());
            hTable = null;
        }
    }

    public static void closeHBaseAdmin(HBaseAdmin hBaseAdmin) throws IOException {
        if (hBaseAdmin != null) {
            hBaseAdmin.close();
            LOG.info("close admin, admin counter now is " + HbaseUtil.adminCounter.decrementAndGet());
            hBaseAdmin = null;
        }
    }
}
