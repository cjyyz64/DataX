package com.alibaba.datax.plugin.reader.mysqlreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.StrUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
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
        originalConfig.getNecessaryValue(Key.USERNAME, MysqlReaderErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(Key.PASSWORD, MysqlReaderErrorCode.REQUIRED_VALUE);

        simplifyConf(originalConfig);
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
        originalConfig.set(Constant.IS_TABLE_MODE, isTableMode);

        dealJdbcAndTable(originalConfig);

        dealColumnConf(originalConfig);
    }

    private static void dealJdbcAndTable(Configuration originalConfig) {
        String username = originalConfig.getString(Key.USERNAME);
        String password = originalConfig.getString(Key.PASSWORD);
        boolean isTableMode = originalConfig.getBool(Constant.IS_TABLE_MODE);

        List<Object> conns = originalConfig.getList(Constant.CONN_MARK,
                Object.class);
        int tableNum = 0;

        for (int i = 0, len = conns.size(); i < len; i++) {
            Configuration connConf = Configuration.from(conns.get(i).toString());

            connConf.getNecessaryValue(Key.JDBC_URL, MysqlReaderErrorCode.REQUIRED_VALUE);

            List<String> jdbcUrls = connConf.getList(Key.JDBC_URL, String.class);
            List<String> preSql = connConf.getList(Key.PRE_SQL, String.class);


            String jdbcUrl = chooseJdbcUrl(DataBaseType.MySql, jdbcUrls, username, password, preSql);

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
                    throw new DataXException(MysqlReaderErrorCode.ILLEGAL_VALUE,
                            "read table configured error.");
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
        boolean isTableMode = originalConfig.getBool(Constant.IS_TABLE_MODE);

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

                // 回填其值，需要以 String 的方式转角后续处理
                originalConfig.set(Key.COLUMN, "*");
            } else {
                // TODO 检查用户配置的 column 是否重复(取决于字段是否大小写敏感)
                String jdbcUrl = originalConfig.getString(String.format(
                        "%s[0].%s", Constant.CONN_MARK, Key.JDBC_URL));

                String username = originalConfig.getString(Key.USERNAME);
                String password = originalConfig.getString(Key.PASSWORD);

                String tableName = originalConfig.getString(String.format(
                        "%s[0].%s[0]", Constant.CONN_MARK, Key.TABLE));

                List<String> allColumns = DBUtil.getMysqlTableColumns(jdbcUrl,
                        username, password, tableName);
                if (IS_DEBUG) {
                    LOG.debug("table:[{}] has columns:[{}].", tableName,
                            StringUtils.join(allColumns, ","));
                }

                List<String> quotedColumns = new ArrayList<String>();

                // TODO 注意大小写还需要处理
                for (String column : columns) {
                    if ("*".equals(column)) {
                        throw new DataXException(
                                MysqlReaderErrorCode.ILLEGAL_VALUE,
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

                originalConfig.set(Key.COLUMN, StringUtils.join(quotedColumns, ","));
                String splitPk = originalConfig.getString(Key.SPLIT_PK, null);
                if (StringUtils.isNotBlank(splitPk)) {
                    String pureSplitPk = splitPk;
                    if (splitPk.startsWith("`") && splitPk.endsWith("`")) {
                        pureSplitPk = splitPk.substring(1, splitPk.length() - 1);
                    }

                    if (!allColumns.contains(pureSplitPk)) {
                        throw new DataXException(MysqlReaderErrorCode.ILLEGAL_SPLIT_PK,
                                String.format("no pk column named:[%s].", splitPk));
                    }

                    originalConfig.set(Key.SPLIT_PK, TableExpandUtil.quoteTableOrColumnName(pureSplitPk));
                }

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

            // querySql模式，不希望配制 splitPk，那样是混淆不清晰的
            String splitPk = originalConfig.getString(Key.SPLIT_PK, null);
            if (StringUtils.isNotBlank(splitPk)) {
                LOG.warn(MysqlReaderErrorCode.NOT_RECOMMENDED.toString()
                        + "because you have configured querySql, no need to config splitPk.");
                originalConfig.remove(Key.SPLIT_PK);
            }
        }

    }


    private static boolean recognizeTableOrQuerySqlMode(
            Configuration originalConfig) {
        List<Object> conns = originalConfig.getList(Constant.CONN_MARK, Object.class);

        List<Boolean> tableModeFlags = new ArrayList<Boolean>();
        List<Boolean> querySqlModeFlags = new ArrayList<Boolean>();

        String table = null;
        String querySql = null;

        boolean isTableMode = false;
        boolean isQuerySqlMode = false;
        for (int i = 0, len = conns.size(); i < len; i++) {
            Configuration connConf = Configuration.from(conns.get(i).toString());
            table = connConf.getString(Key.TABLE, null);
            querySql = connConf.getString(Key.QUERY_SQL, null);

            isTableMode = StringUtils.isNotBlank(table);
            tableModeFlags.add(isTableMode);

            isQuerySqlMode = StringUtils.isNotBlank(querySql);
            querySqlModeFlags.add(isQuerySqlMode);

            if (false == isTableMode && false == isQuerySqlMode) {
                // table 和 querySql 二者均未配制
                throw new DataXException(MysqlReaderErrorCode.TABLE_QUERYSQL_MISSING,
                        "table and querySql should configured one item.");
            } else if (true == isTableMode && true == isQuerySqlMode) {
                // table 和 querySql 二者均配置
                throw new DataXException(MysqlReaderErrorCode.TABLE_QUERYSQL_MIXED,
                        "table and querySql can not mixed.");
            }
        }

        // 混合配制 table 和 querySql
        if (!isListValueSame(tableModeFlags) || !isListValueSame(querySqlModeFlags)) {
            throw new DataXException(MysqlReaderErrorCode.TABLE_QUERYSQL_MIXED,
                    "table and querySql can not mixed.");
        }

        return tableModeFlags.get(0);
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

    //TODO： 添加多 presql 的校验
    private static String chooseJdbcUrl(DataBaseType dataBaseType, List<String> jdbcUrls, String username,
                                        String password, List<String> preSql) {
        if (null == jdbcUrls || jdbcUrls.isEmpty()) {
            throw new DataXException(null, "jdbcURL can not be blank.");
        }

        Connection conn = null;
        boolean connOK = false;
        int maxTryTime = 3;
        for (int tryTime = 0; tryTime < maxTryTime; tryTime++) {
            for (String url : jdbcUrls) {
                if (StringUtils.isNotBlank(url)) {
                    url = url.trim();
                    if (null != preSql && !preSql.isEmpty()) {
                        connOK = DBUtil.testConnWithoutRetry(dataBaseType, url,
                                username, password, preSql);
                    } else {
                        connOK = DBUtil.testConnWithoutRetry(dataBaseType, url,
                                username, password);
                    }
                    if (connOK) {
                        return url;
                    }
                }
            }

            // 指数时间等待，重试
            if (tryTime < maxTryTime - 1) {
                // 最后一次，不用sleep
                try {
                    Thread.sleep(1000L * (long) Math.pow(2, tryTime));
                } catch (InterruptedException unused) {
                }
            }
        }

        throw new DataXException(DBUtilErrorCode.CONN_DB_ERROR, "no available jdbcURL from : "
                + jdbcUrls.toString());
    }
}
