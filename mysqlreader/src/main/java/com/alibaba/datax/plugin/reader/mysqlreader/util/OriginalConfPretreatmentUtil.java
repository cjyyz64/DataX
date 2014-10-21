package com.alibaba.datax.plugin.reader.mysqlreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.ListUtil;
import com.alibaba.datax.common.util.StrUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.TableExpandUtil;
import com.alibaba.datax.plugin.reader.mysqlreader.Constant;
import com.alibaba.datax.plugin.reader.mysqlreader.Key;
import com.alibaba.datax.plugin.reader.mysqlreader.MysqlReaderErrorCode;
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


            String jdbcUrl = DBUtil.chooseJdbcUrl(DataBaseType.MySql, jdbcUrls, username, password, preSql);

            // 回写到connection[i].jdbcUrl
            originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK,
                    i, Key.JDBC_URL), jdbcUrl);

            if (isTableMode) {
                // table 方式
                // 对每一个connection 上配置的table 项进行解析(已对表名称进行了 ` 处理的)
                List<String> tables = connConf.getList(Key.TABLE, String.class);

                List<String> expandedTables = TableExpandUtil
                        .expandTableConf(DataBaseType.MySql, tables);

                if (null == expandedTables || expandedTables.isEmpty()) {
                    throw new DataXException(MysqlReaderErrorCode.ILLEGAL_VALUE,
                            "read table configured error.");
                }

                tableNum += expandedTables.size();

                originalConfig.set(String.format("%s[%d].%s",
                        Constant.CONN_MARK, i, Key.TABLE), expandedTables);
            } else {
                // 说明是配置的 querySql 方式 doNothgin
            }
        }

        originalConfig.set(Constant.TABLE_NUMBER_MARK, tableNum);
    }

    private static void dealColumnConf(Configuration originalConfig) {
        boolean isTableMode = originalConfig.getBool(Constant.IS_TABLE_MODE);

        List<String> userConfiguredColumns = originalConfig.getList(Key.COLUMN, String.class);

        if (isTableMode) {
            if (null == userConfiguredColumns || userConfiguredColumns.isEmpty()) {
                String businessMessage = "Lost column config.";
                String message = StrUtil.buildOriginalCauseMessage(businessMessage, null);

                LOG.error(message);
                throw new DataXException(MysqlReaderErrorCode.REQUIRED_KEY, businessMessage);
            } else {
                // deal split pk quote
                String splitPk = originalConfig.getString(Key.SPLIT_PK, null);
                if (StringUtils.isNoneBlank(splitPk)) {
                    if (splitPk.startsWith("`") && splitPk.endsWith("`")) {
                        splitPk = splitPk.substring(1, splitPk.length() - 1).toLowerCase();
                    }
                    originalConfig.set(Key.SPLIT_PK, TableExpandUtil.quoteTableOrColumnName(
                            DataBaseType.MySql, splitPk));
                }

                if (1 == userConfiguredColumns.size() && "*".equals(userConfiguredColumns.get(0))) {
                    LOG.warn(MysqlReaderErrorCode.NOT_RECOMMENDED.toString()
                            + "because column configured as * may not work when you changed your table structure.");

                    // 回填其值，需要以 String 的方式转交后续处理
                    originalConfig.set(Key.COLUMN, "*");
                } else {
                    String jdbcUrl = originalConfig.getString(String.format(
                            "%s[0].%s", Constant.CONN_MARK, Key.JDBC_URL));

                    String username = originalConfig.getString(Key.USERNAME);
                    String password = originalConfig.getString(Key.PASSWORD);

                    String tableName = originalConfig.getString(String.format(
                            "%s[0].%s[0]", Constant.CONN_MARK, Key.TABLE));

                    List<String> allColumns = DBUtil.getMysqlTableColumns(jdbcUrl,
                            username, password, tableName);
                    allColumns = ListUtil.valueToLowerCase(allColumns);

                    if (IS_DEBUG) {
                        LOG.debug("table:[{}] has userConfiguredColumns:[{}].", tableName,
                                StringUtils.join(allColumns, ","));
                    }

                    List<String> quotedColumns = new ArrayList<String>();

                    for (String column : userConfiguredColumns) {
                        if ("*".equals(column)) {
                            throw new DataXException(
                                    MysqlReaderErrorCode.ILLEGAL_VALUE,
                                    "no column named[*].");
                        }

                        if (allColumns.contains(column.toLowerCase())) {
                            quotedColumns.add(TableExpandUtil
                                    .quoteTableOrColumnName(DataBaseType.MySql, column));
                        } else {
                            // 可能是由于用户填写为函数，或者自己对字段进行了`处理
                            quotedColumns.add(column);
                        }
                    }

                    originalConfig.set(Key.COLUMN, StringUtils.join(quotedColumns, ","));
                    if (StringUtils.isNotBlank(splitPk)) {

                        if (!allColumns.contains(splitPk)) {
                            String bussinessMessage = String.format("No pk column named:[%s].", splitPk);
                            String message = StrUtil.buildOriginalCauseMessage(bussinessMessage, null);
                            LOG.error(message);

                            throw new DataXException(MysqlReaderErrorCode.ILLEGAL_SPLIT_PK, bussinessMessage);
                        }
                    }

                }
            }
        } else {
            // querySql模式，不希望配制 column，那样是混淆不清晰的
            if (null != userConfiguredColumns && userConfiguredColumns.size() > 0) {
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
                String bussinessMessage = "table and querySql should configured one item.";
                String message = StrUtil.buildOriginalCauseMessage(
                        bussinessMessage, null);
                LOG.error(message);

                throw new DataXException(MysqlReaderErrorCode.TABLE_QUERYSQL_MISSING,
                        bussinessMessage);
            } else if (true == isTableMode && true == isQuerySqlMode) {
                // table 和 querySql 二者均配置
                String bussinessMessage = "table and querySql can not mixed.";
                String message = StrUtil.buildOriginalCauseMessage(
                        bussinessMessage, null);
                LOG.error(message);

                throw new DataXException(MysqlReaderErrorCode.TABLE_QUERYSQL_MIXED, bussinessMessage);
            }
        }

        // 混合配制 table 和 querySql
        if (!ListUtil.checkIfValueSame(tableModeFlags) || !ListUtil.checkIfValueSame(tableModeFlags)) {
            String bussinessMessage = "table and querySql can not mixed.";
            String message = StrUtil.buildOriginalCauseMessage(
                    bussinessMessage, null);
            LOG.error(message);

            throw new DataXException(MysqlReaderErrorCode.TABLE_QUERYSQL_MIXED, bussinessMessage);
        }

        return tableModeFlags.get(0);
    }


}
