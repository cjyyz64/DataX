package com.alibaba.datax.plugin.reader.hbasereader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.hbasereader.Constant;
import com.alibaba.datax.plugin.reader.hbasereader.HbaseReader;
import com.alibaba.datax.plugin.reader.hbasereader.HbaseReaderErrorCode;
import com.alibaba.datax.plugin.reader.hbasereader.Key;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;

public final class HbaseUtil {

    public static void doPretreatment(Configuration originalConfig) {
        String hbaseConfig = originalConfig.getNecessaryValue(Key.HBASE_CONFIG,
                HbaseReaderErrorCode.TEMP);

        String mode = HbaseUtil.dealMode(originalConfig);
        originalConfig.set(Key.MODE, mode);

        String table = originalConfig.getNecessaryValue(Key.TABLE, HbaseReaderErrorCode.TEMP);
        List<Map> column = originalConfig.getList(Key.COLUMN, Map.class);

        if (column == null) {
            throw DataXException.asDataXException(HbaseReaderErrorCode.REQUIRED_VALUE, "必须配置 Hbasereader 的 column 配置项.");
        }

        HbaseUtil.checkColumn(column);

        String encoding = originalConfig.getString(Key.ENCODING, "utf-8");
        originalConfig.set(Key.ENCODING, encoding);

        Pair<String, String> rangeInfo = HbaseUtil.dealRowkeyRange(originalConfig);
    }

    public static Pair<String, String> dealRowkeyRange(Configuration originalConfig) {
        String startRowkey = originalConfig.getString(Constant.RANGE + "." + Key.START_ROWKEY);
        String endRowkey = originalConfig.getString(Constant.RANGE + "." + Key.END_ROWKEY);

        return ImmutablePair.of(startRowkey, endRowkey);
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
