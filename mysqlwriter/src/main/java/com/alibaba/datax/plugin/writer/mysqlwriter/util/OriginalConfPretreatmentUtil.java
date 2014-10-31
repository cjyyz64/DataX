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

    private static boolean IS_DEBUG = LOG.isDebugEnabled();

    public static void doPretreatment(Configuration originalConfig) {
        // 检查 username/password 配置（必填）
        originalConfig.getNecessaryValue(Key.USERNAME, MysqlWriterErrorCode.CONF_ERROR);
        originalConfig.getNecessaryValue(Key.PASSWORD, MysqlWriterErrorCode.CONF_ERROR);

        // 检查batchSize 配置（选填，如果未填写，则设置为默认值）
        int batchSize = originalConfig.getInt(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_SIZE);
        if (batchSize < 1) {
            throw new DataXException(MysqlWriterErrorCode.CONF_ERROR,
                    "batchSize can not less than 1.");
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
                throw new DataXException(MysqlWriterErrorCode.CONF_ERROR, "lost jdbcUrl config.");
            }

            originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK, i, Key.JDBC_URL), jdbcUrl);

            List<String> tables = connConf.getList(Key.TABLE, String.class);

            if (null == tables || tables.isEmpty()) {
                throw new DataXException(MysqlWriterErrorCode.CONF_ERROR,
                        "lost table config.");
            }

            // 对每一个connection 上配置的table 项进行解析
            List<String> expandedTables = TableExpandUtil
                    .expandTableConf(DataBaseType.MySql, tables);

            if (null == expandedTables || expandedTables.isEmpty()) {
                throw new DataXException(MysqlWriterErrorCode.CONF_ERROR,
                        "write table configured error.");
            }

            tableNum += expandedTables.size();

            originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK,
                    i, Key.TABLE), expandedTables);
        }

        originalConfig.set(Constant.TABLE_NUMBER_MARK, tableNum);
    }

    private static void dealColumnConf(Configuration originalConfig) {
        List<String> columns = originalConfig.getList(Key.COLUMN, String.class);
        if (null == columns || 0 == columns.size()) {
            throw new DataXException(MysqlWriterErrorCode.CONF_ERROR, "column can not be blank.");
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
                LOG.warn(DBUtilErrorCode.NOT_RECOMMENDED.toString()
                        + "because column configured as * may not work when you changed your table structure.");

                // 回填其值，需要以 String 的方式转交后续处理
                originalConfig.set(Key.COLUMN, allColumns);
            } else if (columns.size() < allColumns.size()) {
                throw new DataXException(MysqlWriterErrorCode.CONF_ERROR, "column exceed table all columns.");
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

        boolean isWriteModeLegal = writeMode.toLowerCase().startsWith("insert")
                || writeMode.toLowerCase().startsWith("replace");

        if (!isWriteModeLegal) {
            throw new DataXException(MysqlWriterErrorCode.CONF_ERROR, "Unsupported write mode=" + writeMode);
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
        LOG.info("do write data [\n{}\n], which jdbcUrl:[{}]",
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
