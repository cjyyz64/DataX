package com.alibaba.datax.plugin.writer.mysqlwriter.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.ListUtil;
import com.alibaba.datax.plugin.rdbms.util.*;
import com.alibaba.datax.plugin.writer.mysqlwriter.Constant;
import com.alibaba.datax.plugin.writer.mysqlwriter.Key;
import com.alibaba.datax.plugin.writer.mysqlwriter.MysqlWriterErrorCode;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public final class OriginalConfPretreatmentUtil {

    private static final Logger LOG = LoggerFactory
            .getLogger(OriginalConfPretreatmentUtil.class);

    public static void doPretreatment(Configuration originalConfig) {
        // 检查 username/password 配置（必填）
        originalConfig.getNecessaryValue(Key.USERNAME, MysqlWriterErrorCode.CONF_ERROR);
        originalConfig.getNecessaryValue(Key.PASSWORD, MysqlWriterErrorCode.CONF_ERROR);

        // 检查batchSize 配置（选填，如果未填写，则设置为默认值）
        int batchSize = originalConfig.getInt(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_SIZE);
        if (batchSize < 1) {
            throw DataXException.asDataXException(MysqlWriterErrorCode.CONF_ERROR, String.format(
                    "您所配置的写入数据库表的 batchSize:%s 不能小于1. 推荐配置范围为：[100-1000], 该值越大, 内存溢出可能性越大.",
                    batchSize));
        }

        originalConfig.set(Key.BATCH_SIZE, batchSize);
        simplifyConf(originalConfig);

        dealColumnConf(originalConfig);
        dealWriteMode(originalConfig);
    }

    private static void simplifyConf(Configuration originalConfig) {
        List<Object> connections = originalConfig.getList(Constant.CONN_MARK,
                Object.class);

        int tableNum = 0;

        for (int i = 0, len = connections.size(); i < len; i++) {
            Configuration connConf = Configuration.from(connections.get(i).toString());

            String jdbcUrl = connConf.getString(Key.JDBC_URL);
            if (StringUtils.isBlank(jdbcUrl)) {
                throw DataXException.asDataXException(MysqlWriterErrorCode.CONF_ERROR, "您未配置的写入数据库表的 jdbcUrl.");
            }

            originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK, i, Key.JDBC_URL), jdbcUrl);

            List<String> tables = connConf.getList(Key.TABLE, String.class);

            if (null == tables || tables.isEmpty()) {
                throw DataXException.asDataXException(MysqlWriterErrorCode.CONF_ERROR,
                        "您未配置写入数据库表的表名称.");
            }

            // 对每一个connection 上配置的table 项进行解析
            List<String> expandedTables = TableExpandUtil
                    .expandTableConf(DataBaseType.MySql, tables);

            if (null == expandedTables || expandedTables.isEmpty()) {
                throw DataXException.asDataXException(MysqlWriterErrorCode.CONF_ERROR,
                        "您配置的写入数据库表名称错误.");
            }

            tableNum += expandedTables.size();

            originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK,
                    i, Key.TABLE), expandedTables);
        }

        originalConfig.set(Constant.TABLE_NUMBER_MARK, tableNum);
    }

    private static void dealColumnConf(Configuration originalConfig) {
        List<String> columns = originalConfig.getList(Key.COLUMN, String.class);
        if (null == columns || columns.isEmpty()) {
            throw DataXException.asDataXException(MysqlWriterErrorCode.CONF_ERROR,
                    "您未配置写入数据库表的列名称.");
        } else {
            String jdbcUrl = originalConfig.getString(String.format("%s[0].%s",
                    Constant.CONN_MARK, Key.JDBC_URL));

            String username = originalConfig.getString(Key.USERNAME);
            String password = originalConfig.getString(Key.PASSWORD);
            String oneTable = originalConfig.getString(String.format(
                    "%s[0].%s[0]", Constant.CONN_MARK, Key.TABLE));

            List<String> allColumns = DBUtil.getTableColumns(DataBaseType.MySql, jdbcUrl, username, password, oneTable);


            LOG.info("table:[{}] all columns:[\n{}\n].", oneTable,
                    StringUtils.join(allColumns, ","));

            if (1 == columns.size() && "*".equals(columns.get(0))) {
                LOG.warn("您配置的写入数据库表的列为*，这是不推荐的行为，因为当您的表字段个数、类型有变动时，可能影响任务正确性甚至会运行出错。");

                // 回填其值，需要以 String 的方式转交后续处理
                originalConfig.set(Key.COLUMN, allColumns);
            } else if (columns.size() > allColumns.size()) {
                throw DataXException.asDataXException(MysqlWriterErrorCode.CONF_ERROR,
                        String.format("您所配置的写入数据库表的字段个数:%s 大于目的表的字段总个数:%s .",
                                columns.size(), allColumns.size()));
            } else {
                // 确保用户配置的 column 不重复
                ListUtil.makeSureNoValueDuplicate(columns, false);
            }
        }
    }

    private static void dealWriteMode(Configuration originalConfig) {
        List<String> columns = originalConfig.getList(Key.COLUMN, String.class);

        String jdbcUrl = originalConfig.getString(String.format("%s[0].%s",
                Constant.CONN_MARK, Key.JDBC_URL, String.class));

        // 默认为：insert 方式
        String writeMode = originalConfig.getString(Key.WRITE_MODE, "INSERT");

        boolean isWriteModeLegal = writeMode.trim().toLowerCase().startsWith("insert")
                || writeMode.trim().toLowerCase().startsWith("replace");

        if (!isWriteModeLegal) {
            throw DataXException.asDataXException(MysqlWriterErrorCode.CONF_ERROR, String.format("您所配置的 writeMode:%s 错误. DataX 目前仅支持replace 或 insert 方式.", writeMode));
        }


        String writeDataSqlTemplate = new StringBuilder().append(writeMode).append(" INTO %s (")
                .append(StringUtils.join(columns, ",")).append(") VALUES(")
                .append(getValueHolder(columns)).append(")").toString();

        String formattedSql = null;

        try {
            formattedSql = SqlFormatUtil.format(writeDataSqlTemplate);
        } catch (Exception unused) {
            // ignore it
        }
        LOG.info("Write data [\n{}\n], which jdbcUrl:[{}]",
                null != formattedSql ? formattedSql : writeDataSqlTemplate, jdbcUrl);

        originalConfig.set(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK,
                writeDataSqlTemplate);
    }

    private static String getValueHolder(List<String> columns) {
        StringBuilder placeHolder = new StringBuilder();
        for (int i = 0, len = columns.size(); i < len; i++) {
            placeHolder.append("?").append(",");
        }
        placeHolder.setLength(placeHolder.length() - 1);

        return placeHolder.toString();
    }

}
