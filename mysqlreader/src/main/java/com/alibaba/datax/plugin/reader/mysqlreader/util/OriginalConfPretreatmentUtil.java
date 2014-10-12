package com.alibaba.datax.plugin.reader.mysqlreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.StrUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.TableExpandUtil;
import com.alibaba.datax.plugin.reader.mysqlreader.Constant;
import com.alibaba.datax.plugin.reader.mysqlreader.Key;
import com.alibaba.datax.plugin.reader.mysqlreader.MysqlReaderErrorCode;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public final class OriginalConfPretreatmentUtil {

    private static final Logger LOG = LoggerFactory
            .getLogger(OriginalConfPretreatmentUtil.class);

    private static boolean IS_DEBUG = LOG.isDebugEnabled();

    public static void doPretreatment(Configuration originalConfig) {
        // 检查 username/password 配置（必填）
        originalConfig.getNecessaryValue(Key.USERNAME,
                MysqlReaderErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(Key.PASSWORD,
                MysqlReaderErrorCode.REQUIRED_VALUE);

        dealFetchSize(originalConfig);

        simplifyConf(originalConfig);
    }

    private static void dealFetchSize(Configuration originalConfig) {
        // 检查batchSize 配置（选填，如果未填写，则设置为默认值）
        int fetchSize = originalConfig.getInt(Key.FETCH_SIZE,
                Constant.DEFAULT_FETCH_SIZE);
        if (fetchSize < 1) {
            throw new DataXException(MysqlReaderErrorCode.ILLEGAL_VALUE,
                    "fetchSize can not less than 1.");
        }

        originalConfig.set(Key.FETCH_SIZE, fetchSize);
    }

    /**
     * 对配置进行初步处理：
     * <ol>
     * <li>处理同一个数据库配置了多个jdbcUrl的情况</li>
     * <li>识别并标记是采用querySql 模式还是 table 模式</li>
     * <li>对 table 模式，确定分表个数，并处理 column 转 *事项</li>
     * </ol>
     */
    private static void simplifyConf(Configuration originalConfig) {
        boolean isTableMode = recognizeTableOrQuerySqlMode(originalConfig);
        originalConfig.set(Constant.TABLE_MODE, isTableMode);

        dealJdbcAndTable(originalConfig);

        dealColumnConf(originalConfig);
    }

    private static void dealJdbcAndTable(Configuration originalConfig) {
        String username = originalConfig.getString(Key.USERNAME);
        String password = originalConfig.getString(Key.PASSWORD);
        boolean isTableMode = originalConfig.getBool(Constant.TABLE_MODE);

        List<Object> conns = originalConfig.getList(Constant.CONN_MARK,
                Object.class);
        int tableNum = 0;

        for (int i = 0, len = conns.size(); i < len; i++) {
            Configuration connConf = Configuration
                    .from(conns.get(i).toString());

            connConf.getNecessaryValue(Key.JDBC_URL, MysqlReaderErrorCode.CONF_ERROR);
            List<String> jdbcUrls = connConf
                    .getList(Key.JDBC_URL, String.class);

            String jdbcUrl = chooseJdbcUrl(jdbcUrls, username, password);

            // 回写到connection[i].jdbcUrl
            originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK,
                    i, Key.JDBC_URL), jdbcUrl);

            if (isTableMode) {
                // table 方式
                // 对每一个connection 上配置的table 项进行解析(已对表名称进行了 ` 处理的)
                List<String> tables = connConf.getList(Key.TABLE, String.class);

                List<String> expandedTables = TableExpandUtil
                        .expandTableConf(tables);

                if (null == expandedTables || expandedTables.isEmpty()) {
                    throw new DataXException(MysqlReaderErrorCode.CONF_ERROR,
                            "read table configred error.");
                }

                tableNum += expandedTables.size();

                originalConfig.set(String.format("%s[%d].%s",
                        Constant.CONN_MARK, i, Key.TABLE), expandedTables);
            } else {
                // TODO delete
                // 说明是配置的 querySql 方式
            }
        }

        originalConfig.set(Constant.TABLE_NUMBER_MARK, tableNum);
    }

    private static void dealColumnConf(Configuration originalConfig) {
        boolean isTableMode = originalConfig.getBool(Constant.TABLE_MODE);

        List<String> columns = originalConfig.getList(Key.COLUMN, String.class);

        if (isTableMode) {
            if (null == columns || columns.isEmpty()) {
                String businessMessage = "Lost column config.";
                String message = StrUtil.buildOriginalCauseMessage(businessMessage, null);

                LOG.error(message);
                throw new DataXException(MysqlReaderErrorCode.REQUIRED_KEY, businessMessage);
            } else if (1 == columns.size() && "*".equals(columns.get(0))) {
                LOG.warn(MysqlReaderErrorCode.NOT_RECOMMENDED.toString()
                        + "because column configured as * may not work when you changed your table structure.");

                //回填其值，需要以 String 的方式转角后续处理
                originalConfig.set(Key.COLUMN, "*");
            } else {
                // TODO 检查用户配置的 column 是否重复(取决于字段是否大小写敏感)
                String jdbcUrl = originalConfig.getString(String.format(
                        "%s[0].%s", Constant.CONN_MARK, Key.JDBC_URL));

                String user = originalConfig.getString(Key.USERNAME);
                String pass = originalConfig.getString(Key.PASSWORD);

                String tableName = originalConfig.getString(String.format(
                        "%s[0].%s[0]", Constant.CONN_MARK, Key.TABLE));

                List<String> allColumns = DBUtil.getMysqlTableColumns(jdbcUrl,
                        user, pass, tableName);
                if (IS_DEBUG) {
                    LOG.debug("table:[{}] has columns:[{}].", tableName,
                            StringUtils.join(allColumns, ","));
                }

                List<String> quotedColumns = new ArrayList<String>();

                // 注意大小写还需要处理
                for (String column : columns) {
                    if ("*".equals(column)) {
                        throw new DataXException(
                                MysqlReaderErrorCode.CONF_ERROR,
                                "no column named[*].");
                    }

                    if (allColumns.contains(column)) {
                        quotedColumns.add(TableExpandUtil
                                .quoteTableOrColumnName(column));
                    } else {
                        // 可能是由于用户填写为函数，或者自己对字段进行了`处理
                        quotedColumns.add(column);
                    }
                }

                originalConfig.set(Key.COLUMN,
                        StringUtils.join(quotedColumns, ","));
            }
        } else {
            // querySql模式，不希望配制 column，那样是混淆不清晰的
            if (null != columns && columns.size() > 0) {
                LOG.warn(MysqlReaderErrorCode.NOT_RECOMMENDED.toString()
                        + "because you have configured querySql, no need to config column.");
                originalConfig.remove(Key.COLUMN);
            }

            // querySql模式，不希望配制 where，那样是混淆不清晰的
            String where = originalConfig.getString(Key.WHERE, null);
            if (StringUtils.isNotBlank(where)) {
                LOG.warn(MysqlReaderErrorCode.NOT_RECOMMENDED.toString()
                        + "because you have configured querySql, no need to config where.");
                originalConfig.remove(Key.WHERE);
            }
        }

    }

    private static boolean recognizeTableOrQuerySqlMode(
            Configuration originalConfig) {
        List<Object> conns = originalConfig.getList(Constant.CONN_MARK, Object.class);

        List<Boolean> tableModeFlag = new ArrayList<Boolean>();
        List<Boolean> querySqlModeFlag = new ArrayList<Boolean>();

        String table = null;
        String querySql = null;
        for (int i = 0, len = conns.size(); i < len; i++) {
            Configuration connConf = Configuration.from(conns.get(i).toString());
            table = connConf.getString(Key.TABLE, null);
            querySql = connConf.getString(Key.QUERY_SQL, null);

            tableModeFlag.add(StringUtils.isNotBlank(table));
            querySqlModeFlag.add(StringUtils.isNotBlank(querySql));

            // table 和 querySql 二者均未配制或者均配置
            boolean illlegalMode = tableModeFlag.get(i).booleanValue() ==
                    querySqlModeFlag.get(i).booleanValue();

            if (illlegalMode && tableModeFlag.get(i).booleanValue() == false) {
                throw new DataXException(MysqlReaderErrorCode.TABLE_QUERYSQL_MIXED,
                        "table and querySql can only have one.");
            } else if (illlegalMode && tableModeFlag.get(i).booleanValue() == true) {
                throw new DataXException(MysqlReaderErrorCode.TABLE_QUERYSQL_MIXED,
                        "table and querySql can not mixed.");
            }
        }

        // 混合配制 table 和 querySql
        if (!isListValueSame(tableModeFlag)
                || !isListValueSame(querySqlModeFlag)) {
            throw new DataXException(MysqlReaderErrorCode.TABLE_QUERYSQL_MIXED,
                    "table and querySql can not mixed.");
        }

        return tableModeFlag.get(0);
    }

    private static boolean isListValueSame(List<Boolean> flags) {
        if (flags.size() == 1) {
            return true;
        }

        boolean isSame = true;
        boolean preValue = flags.get(0);

        for (int i = 1, len = flags.size(); i < len; i++) {
            if (preValue != flags.get(i)) {
                isSame = false;
                break;
            }
        }
        return isSame;
    }

    private static String chooseJdbcUrl(List<String> jdbcUrls, String username,
                                        String password) {
        Connection conn = null;
        for (String jdbcUrl : jdbcUrls) {
            // TODO 需要修改其逻辑，不要直接报错
            try {
                conn = DBUtil.getConnection("mysql", jdbcUrl, username,
                        password);
            } catch (Exception e) {
                LOG.warn("jdbcUrl:[{}] not available.", jdbcUrl);
            }
            if (null != conn) {
                return jdbcUrl;
            }
        }

        throw new DataXException(
                DBUtilErrorCode.CONN_DB_ERROR, "no available jdbc.");
    }
}
