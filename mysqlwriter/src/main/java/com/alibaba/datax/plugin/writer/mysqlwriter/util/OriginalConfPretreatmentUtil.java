package com.alibaba.datax.plugin.writer.mysqlwriter.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.SqlFormatUtil;
import com.alibaba.datax.plugin.rdbms.util.TableExpandUtil;
import com.alibaba.datax.plugin.writer.mysqlwriter.Constant;
import com.alibaba.datax.plugin.writer.mysqlwriter.Key;
import com.alibaba.datax.plugin.writer.mysqlwriter.MysqlWriterErrorCode;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public final class OriginalConfPretreatmentUtil {

    private static final Logger LOG = LoggerFactory
            .getLogger(OriginalConfPretreatmentUtil.class);

    private static boolean IS_DEBUG = LOG.isDebugEnabled();

    public static void doPretreatment(Configuration originalConfig) {
        // 检查 username/password 配置（必填）
        originalConfig.getNecessaryValue(Key.USERNAME,
                MysqlWriterErrorCode.CONF_ERROR);
        originalConfig.getNecessaryValue(Key.PASSWORD,
                MysqlWriterErrorCode.CONF_ERROR);

        // 检查batchSize 配置（选填，如果未填写，则设置为默认值）
        int batchSize = originalConfig.getInt(Key.BATCH_SIZE,
                Constant.DEFAULT_BATCH_SIZE);
        if (batchSize < 1) {
            throw new DataXException(MysqlWriterErrorCode.CONF_ERROR,
                    "batchSize can not less than 1.");
        }

        originalConfig.set(Key.BATCH_SIZE, batchSize);

        simplifyConf(originalConfig);

        dealColumnConf(originalConfig);
        dealInsertOrReplaceConf(originalConfig);

    }

    private static void simplifyConf(Configuration originalConfig) {
        List<Object> conns = originalConfig.getList(Constant.CONN_MARK,
                Object.class);

        int tableNum = 0;

        for (int i = 0, len = conns.size(); i < len; i++) {
            Configuration connConf = Configuration
                    .from(conns.get(i).toString());

            String jdbcUrl = connConf.getString(Key.JDBC_URL);
            if (StringUtils.isBlank(jdbcUrl)) {
                throw new DataXException(MysqlWriterErrorCode.CONF_ERROR,
                        "lost jdbcUrl config.");
            }

            originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK,
                    i, Key.JDBC_URL), jdbcUrl);

            List<String> tables = connConf.getList(Key.TABLE, String.class);

            if (null == tables || 0 == tables.size()) {
                throw new DataXException(MysqlWriterErrorCode.CONF_ERROR,
                        "lost table config.");
            }

            // 对每一个connection 上配置的table 项进行解析(已对表名称进行了 ` 处理的)
            List<String> expandedTables = TableExpandUtil
                    .expandTableConf(DataBaseType.MySql, tables);

            tableNum += expandedTables.size();

            originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK,
                    i, Key.TABLE), expandedTables);

        }

        originalConfig.set(Constant.TABLE_NUMBER_MARK, tableNum);
    }

    private static void dealColumnConf(Configuration originalConfig) {
        List<String> columns = originalConfig.getList(Key.COLUMN, String.class);
        if (null == columns || 0 == columns.size()) {
            throw new DataXException(MysqlWriterErrorCode.CONF_ERROR,
                    "column can not be blank.");
        } else if (1 == columns.size() && "*".equals(columns.get(0))) {
            throw new DataXException(MysqlWriterErrorCode.CONF_ERROR,
                    "column can not be *.");
        } else {
            // TODO 检查用户配置的 column 是否重复(取决于字段是否大小写敏感)

        }

        String jdbcUrl = originalConfig.getString(String.format("%s[0].%s",
                Constant.CONN_MARK, Key.JDBC_URL));

        String user = originalConfig.getString(Key.USERNAME);
        String pass = originalConfig.getString(Key.PASSWORD);

        String tableName = originalConfig.getString(String.format(
                "%s[0].%s[0]", Constant.CONN_MARK, Key.TABLE));

        List<String> allColumns = DBUtil.getMysqlTableColumns(jdbcUrl, user,
                pass, tableName);
        if (IS_DEBUG) {
            LOG.debug("table:[{}] has columns:[{}].", tableName,
                    StringUtils.join(allColumns, ","));
        }

        List<String> retColumns = new ArrayList<String>();
        for (String column : columns) {
            if (allColumns.contains(column)) {
                retColumns.add(TableExpandUtil.quoteTableOrColumnName(DataBaseType.MySql, column));
            } else {
                throw new DataXException(MysqlWriterErrorCode.CONF_ERROR,
                        String.format("all columns:[%s],no column:[%s]",
                                StringUtils.join(allColumns, ","), column));
            }
        }

        originalConfig.set(Constant.COLUMN_NUMBER_MARK, retColumns.size());
        originalConfig.set(Key.COLUMN, retColumns);
    }

    private static void dealInsertOrReplaceConf(Configuration originalConfig) {
        List<String> columns = originalConfig.getList(Key.COLUMN, String.class);

        String jdbcUrl = originalConfig.getString(String.format("%s[0].%s",
                Constant.CONN_MARK, Key.JDBC_URL, String.class));

        // 默认为：insert 方式
        String insertOrReplace = originalConfig.getString(
                Key.INSERT_OR_REPLACE, "INSERT");

        String sqlTemplate = insertOrReplace + " INTO %s ("
                + StringUtils.join(columns, ",") + ") VALUES("
                + getValueHolder(columns) + ")";

        String formattedSql = null;

        try {
            formattedSql = SqlFormatUtil.format(sqlTemplate);
        } catch (Exception unused) {
            // ignore it
        }
        LOG.info("do write data [\n{}\n], from jdbc-url:[\n{}\n]",
                null != formattedSql ? formattedSql : sqlTemplate, jdbcUrl);

        originalConfig.set(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK,
                sqlTemplate);
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
