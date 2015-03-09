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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class HbaseUtil {
    private static Logger LOG = LoggerFactory.getLogger(HbaseUtil.class);

    private static final String META_SCANNER_CACHING = "100";


    public static void doPretreatment(Configuration originalConfig) {
        originalConfig.getNecessaryValue(Key.HBASE_CONFIG,
                HbaseReaderErrorCode.REQUIRED_VALUE);

        originalConfig.getNecessaryValue(Key.TABLE, HbaseReaderErrorCode.REQUIRED_VALUE);

        String mode = HbaseUtil.dealMode(originalConfig);
        originalConfig.set(Key.MODE, mode);

        String encoding = originalConfig.getString(Key.ENCODING, "utf-8");
        originalConfig.set(Key.ENCODING, encoding);

        // 此处增强一个检查：isBinaryRowkey 配置不能出现在与 hbaseConfig 等配置平级地位
        Boolean isBinaryRowkey = originalConfig.getBool(Key.IS_BINARY_ROWKEY);
        if (isBinaryRowkey != null) {
            throw DataXException.asDataXException(HbaseReaderErrorCode.ILLEGAL_VALUE, String.format("%s 不能配置在此处，它应该配置在 range 里面.", Key.IS_BINARY_ROWKEY));
        }

        // 处理 range 的配置，将 range 内的配置值提取到与 hbaseConfig 等配置项平级地位，方便后续获取值
        boolean hasConfiguredRange = false;
        String startRowkey = originalConfig.getString(Constant.RANGE + "." + Key.START_ROWKEY);
        if (startRowkey != null) {
            hasConfiguredRange = true;
            originalConfig.set(Key.START_ROWKEY, startRowkey);
        }

        String endRowkey = originalConfig.getString(Constant.RANGE + "." + Key.END_ROWKEY);
        if (endRowkey != null) {
            hasConfiguredRange = true;
            originalConfig.set(Key.END_ROWKEY, endRowkey);
        }

        // 如果配置了 range, 就必须要配置 isBinaryRowkey
        if (hasConfiguredRange) {
            isBinaryRowkey = originalConfig.getBool(Constant.RANGE + "." + Key.IS_BINARY_ROWKEY);
            if (isBinaryRowkey == null) {
                throw DataXException.asDataXException(HbaseReaderErrorCode.REQUIRED_VALUE, "您需要在 range 内配置 isBinaryRowkey 项，用于告诉 DataX 把您填写的 rowkey 转换为内部的二进制时，采用那个 API(值为 true 时，采用Bytes.toBytesBinary(String rowKey)，值为 false 时，采用Bytes.toBytes(String rowKey))");
            }

            originalConfig.set(Key.IS_BINARY_ROWKEY, isBinaryRowkey);
        }
    }

    /**
     * 对模式以及与模式进行配对的配置进行检查
     */
    private static String dealMode(Configuration originalConfig) {
        String mode = originalConfig.getString(Key.MODE);

        ModeType modeType = ModeType.getByTypeName(mode);
        switch (modeType) {
            case Normal: {
                // normal 模式不需要配置 maxVersion，需要配置 column，并且 column 格式为 Map 风格
                String maxVersion = originalConfig.getString(Key.MAX_VERSION);
                Validate.isTrue(maxVersion == null, "您配置的是 normal 模式读取 hbase 中的数据，所以不能配置无关项：maxVersion");

                List<Map> column = originalConfig.getList(Key.COLUMN, Map.class);

                if (column == null || column.isEmpty()) {
                    throw DataXException.asDataXException(HbaseReaderErrorCode.REQUIRED_VALUE, "您配置的是 normal 模式读取 hbase 中的数据，所以必须配置 column，其形式为：column:[{\"name\": \"cf0:column0\",\"type\": \"string\"},{\"name\": \"cf1:column1\",\"type\": \"long\"}]");
                }

                // 通过 parse 进行 column 格式的进一步检查
                HbaseUtil.parseColumnOfNormalMode(column);
                break;
            }
            case MultiVersionFixedColumn: {
                // multiVersionFixedColumn 模式需要配置 maxVersion 和 column，并且 column 格式为 List 风格
                checkMaxVersion(originalConfig, mode);

                List<String> columns = originalConfig.getList(Key.COLUMN, String.class);
                if (columns == null || columns.isEmpty()) {
                    throw DataXException.asDataXException(HbaseReaderErrorCode.REQUIRED_VALUE, "您配置的是 multiVersionFixedColumn 模式读取 hbase 中的数据，所以必须配置 column，其形式为: column:[\"cf0:column0\",\"cf1:column1\"]");
                }

                // 检查配置的 column 格式是否包含cf:qualifier
                for (String column : columns) {
                    if (StringUtils.isBlank(column) || column.split(":").length != 2) {
                        throw DataXException.asDataXException(HbaseReaderErrorCode.ILLEGAL_VALUE, String.format("您配置的是 multiVersionFixedColumn 模式读取 hbase 中的数据，但是您配置的列格式[%s]不正确，每一个列元素应该配置为 列族:列名  的形式, 如 column:[\"cf0:column0\",\"cf1:column1\"]", column));
                    }
                }

                // 检查多版本固定列读取时，不能配置 columnFamily
                List<String> columnFamilies = originalConfig.getList(Key.COLUMN_FAMILY, String.class);
                if (columnFamilies != null) {
                    throw DataXException.asDataXException(HbaseReaderErrorCode.ILLEGAL_VALUE, "您配置的是 multiVersionFixedColumn 模式读取 hbase 中的数据，所以不能配置 columnFamily");
                }

                break;
            }
            case MultiVersionDynamicColumn: {
                // multiVersionDynamicColumn 模式需要配置 maxVersion 和 columnFamily，并且 columnFamily 格式为 List 风格
                checkMaxVersion(originalConfig, mode);
                List<String> columnFamilies = originalConfig.getList(Key.COLUMN_FAMILY, String.class);
                if (columnFamilies == null || columnFamilies.isEmpty()) {
                    throw DataXException.asDataXException(HbaseReaderErrorCode.REQUIRED_VALUE, "您配置的是 multiVersionDynamicColumn 模式读取 hbase 中的数据，所以必须配置 columnFamily，其形式为：columnFamily:[\"cf0\",\"cf1\"]");
                }

                // 检查多版本动态列读取时，不能配置 column
                List<String> columns = originalConfig.getList(Key.COLUMN, String.class);
                if (columns != null) {
                    throw DataXException.asDataXException(HbaseReaderErrorCode.ILLEGAL_VALUE, "您配置的是 multiVersionDynamicColumn 模式读取 hbase 中的数据，所以不能配置 column");
                }

                break;
            }
            default:
                throw DataXException.asDataXException(HbaseReaderErrorCode.ILLEGAL_VALUE, "Hbasereader 不支持此类模式:" + modeType);
        }

        return mode;
    }

    // 检查 maxVersion 是否存在，并且值是否合法
    private static void checkMaxVersion(Configuration configuration, String mode) {
        Integer maxVersion = configuration.getInt(Key.MAX_VERSION);
        Validate.notNull(maxVersion, String.format("您配置的是 %s 模式读取 hbase 中的数据，所以必须配置：maxVersion", mode));

        boolean isMaxVersionValid = maxVersion == -1 || maxVersion > 1;
        Validate.isTrue(isMaxVersionValid, String.format("您配置的是 %s 模式读取 hbase 中的数据，但是配置的 maxVersion 值错误. maxVersion规定：-1为读取全部版本，不能配置为0或者1（因为0或者1，我们认为用户是想用 normal 模式读取数据，而非 %s 模式读取，二者差别大），大于1则表示读取最新的对应个数的版本", mode, mode));
    }

    public static org.apache.hadoop.conf.Configuration getHbaseConf(String hbaseConf) {
        if (StringUtils.isBlank(hbaseConf)) {
            throw DataXException.asDataXException(HbaseReaderErrorCode.REQUIRED_VALUE, "读 Hbase 时需要配置 hbaseConfig，其内容为 Hbase 连接信息，请联系 Hbase PE 获取该信息.");
        }
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();

        Map<String, String> map;
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

    public static byte[] convertUserStartRowkey(Configuration configuration) {
        String startRowkey = configuration.getString(Key.START_ROWKEY);
        if (StringUtils.isBlank(startRowkey)) {
            return HConstants.EMPTY_BYTE_ARRAY;
        } else {
            boolean isBinaryRowkey = configuration.getBool(Key.IS_BINARY_ROWKEY);
            return HbaseUtil.stringToBytes(startRowkey, isBinaryRowkey);
        }
    }

    public static byte[] convertUserEndRowkey(Configuration configuration) {
        String endRowkey = configuration.getString(Key.END_ROWKEY);
        if (StringUtils.isBlank(endRowkey)) {
            return HConstants.EMPTY_BYTE_ARRAY;
        } else {
            boolean isBinaryRowkey = configuration.getBool(Key.IS_BINARY_ROWKEY);
            return HbaseUtil.stringToBytes(endRowkey, isBinaryRowkey);
        }
    }

    /**
     * 注意：convertUserStartRowkey 和 convertInnerStartRowkey，前者会受到 isBinaryRowkey 的影响，只用于第一次对用户配置的 String 类型的 rowkey 转为二进制时使用。而后者约定：切分时得到的二进制的 rowkey 回填到配置中时采用
     */
    public static byte[] convertInnerStartRowkey(Configuration configuration) {
        String startRowkey = configuration.getString(Key.START_ROWKEY);
        if (StringUtils.isBlank(startRowkey)) {
            return HConstants.EMPTY_BYTE_ARRAY;
        }

        return Bytes.toBytesBinary(startRowkey);
    }

    public static byte[] convertInnerEndRowkey(Configuration configuration) {
        String endRowkey = configuration.getString(Key.END_ROWKEY);
        if (StringUtils.isBlank(endRowkey)) {
            return HConstants.EMPTY_BYTE_ARRAY;
        }

        return Bytes.toBytesBinary(endRowkey);
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

    private static byte[] stringToBytes(String rowkey, boolean isBinaryRowkey) {
        if (isBinaryRowkey) {
            return Bytes.toBytesBinary(rowkey);
        } else {
            return Bytes.toBytes(rowkey);
        }
    }

    public static boolean isRowkeyColumn(String columnName) {
        return Constant.ROWKEY_FLAG.equalsIgnoreCase(columnName);
    }

    /**
     * 用于解析 Normal 模式下的列配置
     */
    public static List<HbaseColumnCell> parseColumnOfNormalMode(List<Map> column) {
        List<HbaseColumnCell> hbaseColumnCells = new ArrayList<HbaseColumnCell>();

        HbaseColumnCell oneColumnCell;

        for (Map<String, String> aColumn : column) {
            ColumnType type = ColumnType.getByTypeName(aColumn.get("type"));
            String columnName = aColumn.get("name");
            String columnValue = aColumn.get("value");
            String dateformat = aColumn.get("format");

            if (type == ColumnType.DATE) {
                Validate.notNull(dateformat, "Hbasereader 的列配置中，如果类型为时间，则必须指定时间格式. 形如：yyyy-MM-dd HH:mm:ss");

                Validate.isTrue(StringUtils.isNotBlank(columnName) || StringUtils.isNotBlank(columnValue), "Hbasereader 的列配置中，如果类型为时间，则要么是 type + name + format 的组合，要么是type + value + format 的组合. 而您的配置非这两种组合，请检查并修改.");

                oneColumnCell = new HbaseColumnCell
                        .Builder(type)
                        .columnName(columnName)
                        .columnValue(columnValue)
                        .dateformat(dateformat)
                        .build();
            } else {
                Validate.isTrue(dateformat == null, "Hbasereader 的列配置中，如果类型不为时间，则不需要指定时间格式.");

                Validate.isTrue(StringUtils.isNotBlank(columnName) || StringUtils.isNotBlank(columnValue), "Hbasereader 的列配置中，如果类型不是时间，则要么是 type + name 的组合，要么是type + value 的组合. 而您的配置非这两种组合，请检查并修改.");

                oneColumnCell = new HbaseColumnCell
                        .Builder(type)
                        .columnName(columnName)
                        .columnValue(columnValue)
                        .build();
            }

            hbaseColumnCells.add(oneColumnCell);
        }

        return hbaseColumnCells;
    }
}
