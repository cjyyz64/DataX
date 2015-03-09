package com.alibaba.datax.plugin.reader.hbasereader.util;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.hbasereader.Key;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

public class MultiVersionDynamicColumnReader extends MultiVersionReader {
    private List<String> columnFamilies = null;

    public MultiVersionDynamicColumnReader(Configuration configuration) {
        super(configuration);

        this.columnFamilies = configuration.getList(Key.COLUMN_FAMILY, String.class);
    }

    @Override
    public void initScan(Scan scan) {
        for (String columnFamily : columnFamilies) {
            scan.addFamily(Bytes.toBytes(columnFamily.trim()));
        }

        super.setMaxVersions(scan);
    }
}
