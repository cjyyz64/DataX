package com.alibaba.datax.plugin.reader.hbasereader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.hbasereader.Constant;
import com.alibaba.datax.plugin.reader.hbasereader.HbaseReader;
import com.alibaba.datax.plugin.reader.hbasereader.HbaseReaderErrorCode;
import com.alibaba.datax.plugin.reader.hbasereader.Key;

import java.util.List;
import java.util.Map;

public final class HbaseUtil {

    public static void doPretreatment(Configuration originalConfig) {
        originalConfig.getNecessaryValue(Key.HBASE_CONFIG,
                HbaseReaderErrorCode.TEMP);

        String mode = HbaseUtil.dealMode(originalConfig);
        originalConfig.set(Key.MODE, mode);

        originalConfig.getNecessaryValue(Key.TABLE, HbaseReaderErrorCode.TEMP);
        List<Map> column = originalConfig.getList(Key.COLUMN, Map.class);

        if (column == null) {
            throw DataXException.asDataXException(HbaseReaderErrorCode.REQUIRED_VALUE, "必须配置 Hbasereader 的 column 配置项.");
        }

        HbaseUtil.checkColumn(column);

        String encoding = originalConfig.getString(Key.ENCODING, "utf-8");
        originalConfig.set(Key.ENCODING, encoding);

        Boolean isBinaryRowkey = originalConfig.getBool(Key.IS_BINARY_ROWKEY);
        if (isBinaryRowkey == null) {
            throw DataXException.asDataXException(HbaseReaderErrorCode.TEMP, "Hbasereader 中必须配置 isBinaryRowkey 项，用于指定主键自身是否为二进制结构。isBinaryRowkey 本项可以配置为 true 或者 false. 分别对应于 DataX 内部调用Bytes.toBytesBinary(String rowKey) 或者Bytes.toBytes(String rowKey) 两个不同的 API.");
        }

        String startRowkey = originalConfig.getString(Constant.RANGE + "." + Key.START_ROWKEY);
        if (startRowkey != null) {
            originalConfig.set(Key.START_ROWKEY, startRowkey);
        }


        String endRowkey = originalConfig.getString(Constant.RANGE + "." + Key.END_ROWKEY);
        if (endRowkey != null) {
            originalConfig.set(Key.END_ROWKEY, endRowkey);
        }
    }

    private static String dealMode(Configuration originalConfig) {
        String mode = originalConfig.getString(Key.MODE, "normal");
        if (!mode.equalsIgnoreCase("normal") && !mode.equalsIgnoreCase("multiVersion")) {
            throw DataXException.asDataXException(HbaseReaderErrorCode.TEMP,
                    "mode 仅能配置为 normal 或者 multiVersion .");
        }

        return mode;
    }

    private static void checkColumn(List<Map> column) {
        HbaseReader.parseColumn(column);
    }
}
