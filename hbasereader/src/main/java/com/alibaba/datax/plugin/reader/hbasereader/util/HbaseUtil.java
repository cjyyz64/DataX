package com.alibaba.datax.plugin.reader.hbasereader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.hbasereader.*;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public final class HbaseUtil {
    private static Logger LOG = LoggerFactory.getLogger(HbaseUtil.class);
    private static final String META_SCANNER_CACHING = "100";


    public static void doPretreatment(Configuration originalConfig) {
        originalConfig.getNecessaryValue(Key.HBASE_CONFIG,
                HbaseReaderErrorCode.REQUIRED_VALUE);

        String mode = HbaseUtil.dealMode(originalConfig);

        originalConfig.set(Key.MODE, mode);

        originalConfig.getNecessaryValue(Key.TABLE, HbaseReaderErrorCode.REQUIRED_VALUE);
        List<Map> column = originalConfig.getList(Key.COLUMN, Map.class);

        if (column == null) {
            throw DataXException.asDataXException(HbaseReaderErrorCode.REQUIRED_VALUE, "您需要配置 Hbasereader 的 column 配置项.");
        }

        List<HbaseColumnCell> hbaseColumnCells = HbaseReader.parseColumn(column);
        if (ModeType.isMultiVersionMode(mode)) {
            HbaseUtil.checkHbaseColumnCellForMultiVersionMode(hbaseColumnCells);
        }

        String encoding = originalConfig.getString(Key.ENCODING, "utf-8");
        originalConfig.set(Key.ENCODING, encoding);

        Boolean isBinaryRowkey = originalConfig.getBool(Key.IS_BINARY_ROWKEY);
        if (isBinaryRowkey == null) {
            throw DataXException.asDataXException(HbaseReaderErrorCode.REQUIRED_VALUE, "您需要配置 isBinaryRowkey 项，用于指定主键自身是否为二进制结构。isBinaryRowkey 可以配置为 true 或者 false. 分别对应于 hbasereader 内部调用Bytes.toBytesBinary(String rowKey) 或者Bytes.toBytes(String rowKey) 两个不同的 API.");
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

    /**
     * 检查 多版本 的情况的字段配置：不能有常量，不能配置 rowkey
     */
    private static void checkHbaseColumnCellForMultiVersionMode(List<HbaseColumnCell> hbaseColumnCells) {
        for (HbaseColumnCell cell : hbaseColumnCells) {
            if (cell.isConstant()) {
                throw DataXException.asDataXException(HbaseReaderErrorCode.ILLEGAL_VALUE,
                        String.format("多版本读取时，不能配置常量字段。您配置中出现了常量字段:[%s].", cell.toString()));
            }

            if (HbaseUtil.isRowkeyColumn(cell.getColumnName())) {
                throw DataXException.asDataXException(HbaseReaderErrorCode.ILLEGAL_VALUE,
                        String.format("多版本读取时，不能配置 rowkey。您配置中出现了rowkey:[%s].", cell.toString()));
            }
        }
    }

    private static String dealMode(Configuration originalConfig) {
        String mode = originalConfig.getString(Key.MODE);

        if (ModeType.isNormalMode(mode)) {
            // normal 模式读取数据，不需要配置 maxVersion 和 rowkeyType
            String maxVersion = originalConfig.getString(Key.MAX_VERSION);
            Validate.isTrue(maxVersion == null, "您配置的是 normal 模式读取 hbase 中的数据，所以不能配置无关项：maxVersion");

            String rowkeyType = originalConfig.getString(Key.ROWKEY_TYPE);
            Validate.isTrue(rowkeyType == null, "您配置的是 normal 模式读取 hbase 中的数据，所以不能配置无关项：rowkeyType");
        } else if (ModeType.isMultiVersionMode(mode)) {
            // multiVersion 模式读取数据，需要配置 maxVersion 和 rowkeyType
            Integer maxVersion = originalConfig.getInt(Key.MAX_VERSION);
            Validate.notNull(maxVersion, "您配置的是 multiVersion 模式读取 hbase 中的数据，所以必须配置：maxVersion");

            int maxVersionValue = maxVersion.intValue();
            boolean isMaxVersionValid = maxVersionValue == -1 || maxVersionValue > 1;
            Validate.isTrue(isMaxVersionValid, "您配置的是 multiVersion 模式读取 hbase 中的数据，但是配置 maxVersion 的值错误了. maxVersion规定：-1为读取全部版本，不能配置为0或者1（因为0或者1，我们认为用户是想用 normal 模式读取数据，而非 multiVersion 模式读取，二者差别很大），大于1则表示读取最新的对应个数的版本");

            String rowkeyType = originalConfig.getString(Key.ROWKEY_TYPE);
            Validate.notNull(rowkeyType, "您配置的是 multiVersion 模式读取 hbase 中的数据，所以必须配置：rowkeyType");
        } else {
            throw DataXException.asDataXException(HbaseReaderErrorCode.ILLEGAL_VALUE,
                    "mode 仅能配置为 normal 或者 multiVersion .");
        }

        return mode;
    }


    public static org.apache.hadoop.conf.Configuration getHbaseConf(String hbaseConf) {
        if (StringUtils.isBlank(hbaseConf)) {
            throw DataXException.asDataXException(HbaseReaderErrorCode.REQUIRED_VALUE, "读 Hbase 时需要配置 hbaseConfig，其内容为 Hbase 连接信息，请联系 Hbase PE 获取该信息.");
        }
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();

        Map<String, String> map = null;
        try {
            map = JSON.parseObject(hbaseConf, Map.class);
        } catch (Exception e) {
            // 用户配置的hbase配置文件路径
            LOG.warn("尝试把您配置的 hbaseConfig: {} 当成文件路径进行解析.", hbaseConf);
            conf.addResource(new Path(hbaseConf));

            LOG.warn("您配置的 hbaseConfig 是文件路径, 是不推荐的行为:因为当您的这个任务迁移到其他机器运行时，很可能出现该路径不存在的错误. 建议您把此项配置改成标准的 Hbase 连接信息，请联系 Hbase PE 获取该信息.");
            return conf;
        }

        // / 用户配置的 key-value 对 来表示hbaseConf
        for (Map.Entry<String, String> entry : map.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        return conf;
    }

    public static byte[] getStartRowKey(Configuration configuration) {
        String startRowkey = configuration.getString(Key.START_ROWKEY);
        boolean isBinaryRowkey = configuration.getBool(Key.IS_BINARY_ROWKEY);

        return parseRowKeyByte(startRowkey, isBinaryRowkey);
    }

    public static byte[] getEndRowKey(Configuration configuration) {
        String endRowkey = configuration.getString(Key.END_ROWKEY);
        boolean isBinaryRowkey = configuration.getBool(Key.IS_BINARY_ROWKEY);

        return parseRowKeyByte(endRowkey, isBinaryRowkey);
    }


    public static HTable initHtable(com.alibaba.datax.common.util.Configuration configuration) {
        String hbaseConnConf = configuration.getString(Key.HBASE_CONFIG);
        String tableName = configuration.getString(Key.TABLE);
        try {
            org.apache.hadoop.conf.Configuration conf = HbaseUtil.getHbaseConf(hbaseConnConf);
            conf.set("hbase.meta.scanner.caching", META_SCANNER_CACHING);

            HBaseAdmin admin = HTableFactory.createHBaseAdmin(conf);
            HTable htable = HTableFactory.createHTable(conf, tableName);

            check(admin, htable);

            return htable;
        } catch (Exception e) {
            throw DataXException.asDataXException(HbaseReaderErrorCode.INIT_TABLE_ERROR, e);
        }
    }


    private static void check(HBaseAdmin admin, HTable htable) throws DataXException, IOException {
        if (!admin.isMasterRunning()) {
            throw new IllegalStateException("HBase master 没有运行, 请检查您的配置 或者 联系 Hbase 管理员.");
        }
        if (!admin.tableExists(htable.getTableName())) {
            throw new IllegalStateException("HBase源头表" + Bytes.toString(htable.getTableName())
                    + "不存在, 请检查您的配置 或者 联系 Hbase 管理员.");
        }
        if (!admin.isTableAvailable(htable.getTableName()) || !admin.isTableEnabled(htable.getTableName())) {
            throw new IllegalStateException("HBase源头表" + Bytes.toString(htable.getTableName())
                    + " 不可用, 请检查您的配置 或者 联系 Hbase 管理员.");
        }
    }

    private static byte[] parseRowKeyByte(String rowkey, boolean isBinaryRowkey) {
        byte[] retRowKey;
        if (org.apache.commons.lang.StringUtils.isBlank(rowkey)) {
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

    public static boolean isRowkeyColumn(String columnName) {
        return Constant.ROWKEY_FLAG.equalsIgnoreCase(columnName);
    }
}
