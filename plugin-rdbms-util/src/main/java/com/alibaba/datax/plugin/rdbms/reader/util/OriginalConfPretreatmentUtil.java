package com.alibaba.datax.plugin.rdbms.reader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.ListUtil;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.TableExpandUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public final class OriginalConfPretreatmentUtil {
    private static final Logger LOG = LoggerFactory
            .getLogger(OriginalConfPretreatmentUtil.class);

    private static boolean IS_DEBUG = LOG.isDebugEnabled();

    public static DataBaseType DATABASE_TYPE;

    public static void doPretreatment(Configuration originalConfig) {
        // 检查 username/password 配置（必填）
        originalConfig.getNecessaryValue(Key.USERNAME,
                DBUtilErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(Key.PASSWORD,
                DBUtilErrorCode.REQUIRED_VALUE);

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
            Configuration connConf = Configuration
                    .from(conns.get(i).toString());

            connConf.getNecessaryValue(Key.JDBC_URL,
                    DBUtilErrorCode.REQUIRED_VALUE);

            List<String> jdbcUrls = connConf
                    .getList(Key.JDBC_URL, String.class);
            List<String> preSql = connConf.getList(Key.PRE_SQL, String.class);

            String jdbcUrl = DBUtil.chooseJdbcUrl(DATABASE_TYPE, jdbcUrls,
                    username, password, preSql);

            jdbcUrl = DATABASE_TYPE.appendJDBCSuffix(jdbcUrl);

            // 回写到connection[i].jdbcUrl
            originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK,
                    i, Key.JDBC_URL), jdbcUrl);

            if (isTableMode) {
                // table 方式
                // 对每一个connection 上配置的table 项进行解析(已对表名称进行了 ` 处理的)
                List<String> tables = connConf.getList(Key.TABLE, String.class);

                List<String> expandedTables = TableExpandUtil.expandTableConf(
                        DATABASE_TYPE, tables);

                if (null == expandedTables || expandedTables.isEmpty()) {
                    throw DataXException.asDataXException(
                            DBUtilErrorCode.ILLEGAL_VALUE, String.format("您所配置的读取数据库表:%s 不正确. " +
                                    "请先了解 DataX 配置.", StringUtils.join(tables, ",")));
                }

                tableNum += expandedTables.size();

                originalConfig.set(String.format("%s[%d].%s",
                        Constant.CONN_MARK, i, Key.TABLE), expandedTables);
            } else {
                // 说明是配置的 querySql 方式，不做处理.
            }
        }

        originalConfig.set(Constant.TABLE_NUMBER_MARK, tableNum);
    }

    private static void dealColumnConf(Configuration originalConfig) {
        boolean isTableMode = originalConfig.getBool(Constant.IS_TABLE_MODE);

        List<String> userConfiguredColumns = originalConfig.getList(Key.COLUMN,
                String.class);

        if (isTableMode) {
            if (null == userConfiguredColumns
                    || userConfiguredColumns.isEmpty()) {
                throw DataXException.asDataXException(DBUtilErrorCode.REQUIRED_VALUE, "您未配置读取数据库表的列信息. " +
                        "正确的配置方式是给 column 配置上您需要读取的列名称,用英文逗号分隔.");
            } else {
                String splitPk = originalConfig.getString(Key.SPLIT_PK, null);

                if (1 == userConfiguredColumns.size()
                        && "*".equals(userConfiguredColumns.get(0))) {
                    LOG.warn("您未配置读取数据库表的列，这是不推荐的行为，因为当您的表字段个数、类型有变动时，可能影响任务正确性甚至会运行出错。");
                    // 回填其值，需要以 String 的方式转交后续处理
                    originalConfig.set(Key.COLUMN, "*");
                } else {
                    String jdbcUrl = originalConfig.getString(String.format(
                            "%s[0].%s", Constant.CONN_MARK, Key.JDBC_URL));

                    String username = originalConfig.getString(Key.USERNAME);
                    String password = originalConfig.getString(Key.PASSWORD);

                    String tableName = originalConfig.getString(String.format(
                            "%s[0].%s[0]", Constant.CONN_MARK, Key.TABLE));

                    List<String> allColumns = DBUtil.getTableColumns(
                            DATABASE_TYPE, jdbcUrl, username, password,
                            tableName);
                    // warn:注意mysql表名区分大小写
                    allColumns = ListUtil.valueToLowerCase(allColumns);

                    if (IS_DEBUG) {
                        LOG.debug("table:[{}] has userConfiguredColumns:[{}].",
                                tableName, StringUtils.join(allColumns, ","));
                    }

                    List<String> quotedColumns = new ArrayList<String>();

                    for (String column : userConfiguredColumns) {
                        if ("*".equals(column)) {
                            throw DataXException.asDataXException(
                                    DBUtilErrorCode.ILLEGAL_VALUE,
                                    "读取数据库表的列中不允许存在多个*.");
                        }

                        if (null == column) {
                            quotedColumns.add(null);
                        } else {
                            if (allColumns.contains(column.toLowerCase())) {
                                quotedColumns.add(column);
                            } else {
                                // 可能是由于用户填写为函数，或者自己对字段进行了`处理或者常量
                                quotedColumns.add(column);
                            }
                        }
                    }

                    originalConfig.set(Key.COLUMN,
                            StringUtils.join(quotedColumns, ","));
                    if (StringUtils.isNotBlank(splitPk)) {

                        if (!allColumns.contains(splitPk)) {
                            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_SPLIT_PK,
                                    String.format("您读取的数据库表:%s 中没有主键名为:%s .", tableName, splitPk));
                        }
                    }

                }
            }
        } else {
            // querySql模式，不希望配制 column，那样是混淆不清晰的
            if (null != userConfiguredColumns
                    && userConfiguredColumns.size() > 0) {
                LOG.warn("由于您读取数据库表采用了querySql的方式, 所以您不需要再配置 column. 如果您不想看到这条提醒，请移除您源头表中配置中的 column.");
                originalConfig.remove(Key.COLUMN);
            }

            // querySql模式，不希望配制 where，那样是混淆不清晰的
            String where = originalConfig.getString(Key.WHERE, null);
            if (StringUtils.isNotBlank(where)) {
                LOG.warn("由于您读取数据库表采用了querySql的方式, 所以您不需要再配置 where. 如果您不想看到这条提醒，请移除您源头表中配置中的 where.");
                originalConfig.remove(Key.WHERE);
            }

            // querySql模式，不希望配制 splitPk，那样是混淆不清晰的
            String splitPk = originalConfig.getString(Key.SPLIT_PK, null);
            if (StringUtils.isNotBlank(splitPk)) {
                LOG.warn("由于您读取数据库表采用了querySql的方式, 所以您不需要再配置 splitPk. 如果您不想看到这条提醒，请移除您源头表中配置中的 splitPk.");
                originalConfig.remove(Key.SPLIT_PK);
            }
        }

    }

    private static boolean recognizeTableOrQuerySqlMode(
            Configuration originalConfig) {
        List<Object> conns = originalConfig.getList(Constant.CONN_MARK,
                Object.class);

        List<Boolean> tableModeFlags = new ArrayList<Boolean>();
        List<Boolean> querySqlModeFlags = new ArrayList<Boolean>();

        String table = null;
        String querySql = null;

        boolean isTableMode = false;
        boolean isQuerySqlMode = false;
        for (int i = 0, len = conns.size(); i < len; i++) {
            Configuration connConf = Configuration
                    .from(conns.get(i).toString());
            table = connConf.getString(Key.TABLE, null);
            querySql = connConf.getString(Key.QUERY_SQL, null);

            isTableMode = StringUtils.isNotBlank(table);
            tableModeFlags.add(isTableMode);

            isQuerySqlMode = StringUtils.isNotBlank(querySql);
            querySqlModeFlags.add(isQuerySqlMode);

            if (false == isTableMode && false == isQuerySqlMode) {
                // table 和 querySql 二者均未配制
                throw DataXException.asDataXException(
                        DBUtilErrorCode.TABLE_QUERYSQL_MISSING, "您配置错误. table和querySql 应该并且只能配置一个.");
            } else if (true == isTableMode && true == isQuerySqlMode) {
                // table 和 querySql 二者均配置
                throw DataXException.asDataXException(DBUtilErrorCode.TABLE_QUERYSQL_MIXED,
                        "您配置凌乱了. 不能同时既配置table又配置querySql");
            }
        }

        // 混合配制 table 和 querySql
        if (!ListUtil.checkIfValueSame(tableModeFlags)
                || !ListUtil.checkIfValueSame(tableModeFlags)) {
            throw DataXException.asDataXException(DBUtilErrorCode.TABLE_QUERYSQL_MIXED,
                    "您配置凌乱了. 不能同时既配置table又配置querySql");
        }

        return tableModeFlags.get(0);
    }

}
