package com.alibaba.datax.plugin.reader.hbasereader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        }
    }
}
