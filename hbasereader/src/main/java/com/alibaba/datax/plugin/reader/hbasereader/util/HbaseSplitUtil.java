package com.alibaba.datax.plugin.reader.hbasereader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.hbasereader.Key;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class HbaseSplitUtil {

    private final static Logger LOG = LoggerFactory
            .getLogger(HbaseSplitUtil.class);

    private final static boolean IS_DEBUG = LOG.isDebugEnabled();

    private static String getStartKey(byte[] startRowkeyByte, byte[] regionStarKey,
                                      boolean isBinaryRowkey) {
        if (startRowkeyByte == null) {// 由于之前处理过，所以传入的userStartKey不可能为null
            throw new IllegalArgumentException(
                    "userStartKey should not be null!");
        }

        byte[] tempStartRowkeyByte = null;
        String retStartRowkey = null;

        if (Bytes.compareTo(startRowkeyByte, regionStarKey) < 0) {
            tempStartRowkeyByte = regionStarKey;
        } else {
            tempStartRowkeyByte = startRowkeyByte;
        }

        if (isBinaryRowkey) {
            retStartRowkey = Bytes.toStringBinary(tempStartRowkeyByte);
        } else {
            retStartRowkey = Bytes.toString(tempStartRowkeyByte);
        }

        return retStartRowkey;

    }

    private static String getEndKey(byte[] endRowkeyByte, byte[] regionEndKey,
                                    boolean isBinaryRowkey) {
        if (endRowkeyByte == null) {// 由于之前处理过，所以传入的userStartKey不可能为null
            throw new IllegalArgumentException("userEndKey should not be null!");
        }

        byte[] tempEndRowkeyByte = null;
        String retEndRowkey = null;

        if (endRowkeyByte.length == 0) {
            tempEndRowkeyByte = regionEndKey;
        } else if (Bytes.compareTo(regionEndKey, HConstants.EMPTY_BYTE_ARRAY) == 0) {
            // 为最后一个region
            tempEndRowkeyByte = endRowkeyByte;
        } else {
            if (Bytes.compareTo(endRowkeyByte, regionEndKey) > 0) {
                tempEndRowkeyByte = regionEndKey;
            } else {
                tempEndRowkeyByte = endRowkeyByte;
            }
        }

        if (isBinaryRowkey) {
            retEndRowkey = Bytes.toStringBinary(tempEndRowkeyByte);
        } else {
            retEndRowkey = Bytes.toString(tempEndRowkeyByte);
        }

        return retEndRowkey;
    }

    public static List<Configuration> split(Configuration configuration, HbaseProxy hbaseProxy) {
        List<Configuration> ret;

        String startRowkey = configuration.getString(Key.START_ROWKEY);
        String endRowKey = configuration.getString(Key.END_ROWKEY);

        //TODO
        boolean isBinaryRowkey = configuration.getBool("");

        try {
            Pair<byte[][], byte[][]> regionRanges = hbaseProxy.getStartEndKeys();
            if (null == regionRanges) {
                //TODO 报错 return
                throw DataXException.asDataXException(null, "");
            }

            byte[] startRowkeyByte = parseRowKeyByte(startRowkey,
                    isBinaryRowkey);
            byte[] endRowkeyByte = parseRowKeyByte(endRowKey,
                    isBinaryRowkey);

			/* 如果配置了start-rowkey和end-rowkey，需要确保：start-rowkey<=end-rowkey */
            if (startRowkeyByte.length != 0 && endRowkeyByte.length != 0
                    && Bytes.compareTo(startRowkeyByte, endRowkeyByte) > 0) {
                throw new IllegalArgumentException(String.format(
                        "startRowkey %s 不得大于 endRowkey %s .",
                        startRowkey, endRowKey));
            }

            ret = doSplit(configuration, startRowkeyByte, endRowkeyByte,
                    regionRanges, isBinaryRowkey);

            LOG.info("HBaseReader doSplit job into {} tasks .",
                    ret.size());

            return ret;

        } catch (IOException e) {
            throw DataXException.asDataXException(null, "");
        }
    }

    public static List<Configuration> doSplit(Configuration param, byte[] startRowkeyByte,
                                              byte[] endRowkeyByte, Pair<byte[][], byte[][]> regionRanges, boolean isBinaryRowkey) {

        List<Configuration> pluginParamList = new ArrayList<Configuration>();

        for (int i = 0; i < regionRanges.getFirst().length; i++) {

            byte[] regionStartKey = regionRanges.getFirst()[i];
            byte[] regionEndKey = regionRanges.getSecond()[i];

            // 当前的region为最后一个region
            // 如果最后一个region的start Key大于用户指定的userEndKey,则最后一个region，应该不包含在内
            // 注意如果用户指定userEndKey为"",则此判断应该不成立。userEndKey为""表示取得最大的region
            if (Bytes.compareTo(regionEndKey, HConstants.EMPTY_BYTE_ARRAY) == 0
                    && (endRowkeyByte.length != 0 && (Bytes.compareTo(
                    regionStartKey, endRowkeyByte) > 0))) {
                continue;
            }

            // 如果当前的region不是最后一个region，
            // 用户配置的userStartKey大于等于region的endkey,则这个region不应该含在内
            if ((Bytes.compareTo(regionEndKey, HConstants.EMPTY_BYTE_ARRAY) != 0)
                    && (Bytes.compareTo(startRowkeyByte, regionEndKey) >= 0)) {
                continue;
            }

            // 如果用户配置的userEndKey小于等于 region的startkey,则这个region不应该含在内
            // 注意如果用户指定的userEndKey为"",则次判断应该不成立。userEndKey为""表示取得最大的region
            if (endRowkeyByte.length != 0
                    && (Bytes.compareTo(endRowkeyByte, regionStartKey) <= 0)) {
                continue;
            }

            Configuration p = param.clone();

            String thisStartKey = getStartKey(startRowkeyByte, regionStartKey,
                    isBinaryRowkey);

            String thisEndKey = getEndKey(endRowkeyByte, regionEndKey,
                    isBinaryRowkey);

            p.set(Key.START_ROWKEY, thisStartKey);
            p.set(Key.END_ROWKEY, thisEndKey);

            if (IS_DEBUG) {
                LOG.debug("startRowkey:[{}],endRowkey:[{}] .",
                        p.getString(Key.START_ROWKEY, ""),
                        p.getString(Key.END_ROWKEY, ""));
            }

            pluginParamList.add(p);
        }

        return pluginParamList;
    }

    private static byte[] parseRowKeyByte(String rowkey, boolean isBinaryRowkey) {
        byte[] retRowKey;
        if (StringUtils.isBlank(rowkey)) {
            retRowKey = HConstants.EMPTY_BYTE_ARRAY;
        } else {
            if (isBinaryRowkey) {
                retRowKey = Bytes.toBytesBinary(rowkey);
            } else {
                retRowKey = Bytes.toBytes(rowkey);
            }
        }
        return retRowKey;
    }
}
