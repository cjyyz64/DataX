package com.alibaba.datax.plugin.reader.hbasereader.util;

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

    private String table = null;
    private String hbaseConfig = null;
    private String startRowkey = null;
    private String endRowkey = null;
    private boolean isBinaryRowkey = false;
    private HbaseProxy proxy = null;

    private static void init(Configuration configuration) {

    }

    private String getStartKey(byte[] startRowkeyByte, byte[] regionStarKey,
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

    private String getEndKey(byte[] endRowkeyByte, byte[] regionEndKey,
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

    public List<Configuration> split(Configuration param, byte[] startRowkeyByte,
                                     byte[] endRowkeyByte, Pair<byte[][], byte[][]> regionRanges) {

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
                    this.isBinaryRowkey);

            String thisEndKey = getEndKey(endRowkeyByte, regionEndKey,
                    this.isBinaryRowkey);

            p.putValue(Key.START_ROWKEY, thisStartKey);
            p.putValue(Key.END_ROWKEY, thisEndKey);

            if (IS_DEBUG) {
                LOG.debug("start-rowkey:[{}],end-rowkey:[{}] .",
                        p.getValue(Key.START_ROWKEY, ""),
                        p.getValue(Key.END_ROWKEY, ""));
            }

            pluginParamList.add(p);
        }

        return pluginParamList;
    }

    public static  List<Configuration> split(Configuration configuration) {
        this.init(configuration);

        List<Configuration> ret = new ArrayList<Configuration>();

        try {
            this.proxy = HbaseProxy.newProxy(hbaseConfig, table);
            Pair<byte[][], byte[][]> regionRanges = proxy.getStartEndKeys();
            if (null == regionRanges) {
                ret = super.split();
                return ret;
            }

            byte[] startRowkeyByte = parseRowKeyByte(this.startRowkey,
                    this.isBinaryRowkey);
            byte[] endRowkeyByte = parseRowKeyByte(this.endRowkey,
                    this.isBinaryRowkey);

			/* 如果配置了start-rowkey和end-rowkey，需要确保：start-rowkey<=end-rowkey */
            if (startRowkeyByte.length != 0 && endRowkeyByte.length != 0
                    && Bytes.compareTo(startRowkeyByte, endRowkeyByte) > 0) {
                throw new IllegalArgumentException(String.format(
                        "startkey %s cannot be larger than endkey %s .",
                        startRowkey, endRowkey));
            }

            ret = split(getPluginParam(), startRowkeyByte, endRowkeyByte,
                    regionRanges);

            LOG.info(String.format("HBaseReader split job into %d sub-jobs .",
                    ret.size()));

            return ret;

        } catch (IOException e) {
            LOG.warn("HBase try to split table failed, use non-split mechanism.");
            ret = super.split();
        } finally {
            try {
                if (null != proxy) {
                    proxy.close();
                }
            } catch (IOException e) {
				/* swallow exception */
            }
        }

        return ret;
    }

    private byte[] parseRowKeyByte(String rowkey, boolean isBinaryRowkey) {
        byte[] retRowKey = null;
        if (StringUtils.isBlank(rowkey)) {
            retRowKey = HConstants.EMPTY_BYTE_ARRAY;
        } else {
            if (this.isBinaryRowkey) {
                retRowKey = Bytes.toBytesBinary(rowkey);
            } else {
                retRowKey = Bytes.toBytes(rowkey);
            }
        }
        return retRowKey;
    }
}
