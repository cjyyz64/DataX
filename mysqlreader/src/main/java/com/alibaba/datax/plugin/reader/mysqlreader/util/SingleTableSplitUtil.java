package com.alibaba.datax.plugin.reader.mysqlreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.RangeSplitUtil;
import com.alibaba.datax.plugin.reader.mysqlreader.Constant;
import com.alibaba.datax.plugin.reader.mysqlreader.Key;
import com.alibaba.datax.plugin.reader.mysqlreader.MysqlReaderErrorCode;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public class SingleTableSplitUtil {
    private static final Logger LOG = LoggerFactory
            .getLogger(SingleTableSplitUtil.class);

    private SingleTableSplitUtil() {
    }

    public static List<Configuration> splitSingleTable(Configuration plugin,
                                                       int adviceNum) {
        List<Configuration> pluginParams = new ArrayList<Configuration>();

        Pair<Object, Object> minMaxPK = getPkRange(plugin);

        if (null == minMaxPK) {
            throw new DataXException(MysqlReaderErrorCode.ILLEGAL_SPLIT_PK, "split by pk failed.");
        }
        if (null == minMaxPK.getLeft() || null == minMaxPK.getRight()) {
            //切分后获取到的start/end 有 Null 的情况
            pluginParams.add(plugin);
            return pluginParams;
        }

        String splitPkName = plugin.getString(Key.SPLIT_PK);
        String column = plugin.getString(Key.COLUMN);
        String table = plugin.getString(Key.TABLE);
        String where = plugin.getString(Key.WHERE, null);

        boolean hasWhere = StringUtils.isNotBlank(where);

        boolean isStringType = Constant.PK_TYPE_STRING.equals(plugin.getString(Constant.PK_TYPE));
        boolean isLongType = Constant.PK_TYPE_LONG.equals(plugin.getString(Constant.PK_TYPE));


        List<String> rangeList = null;
        if (isStringType) {
            rangeList = RangeSplitUtil.splitAndWrap(String.valueOf(minMaxPK.getLeft()), String.valueOf(minMaxPK.getRight()),
                    adviceNum, splitPkName, "'", DataBaseType.MySql);
        } else if (isLongType) {

            rangeList = RangeSplitUtil.splitAndWrap(Long.parseLong(minMaxPK.getLeft().toString()),
                    Long.parseLong(minMaxPK.getRight().toString()), adviceNum, splitPkName);
        } else {
            //error
        }


        String tempQuerySql = null;
        if (null != rangeList) {
            for (String range : rangeList) {

                Configuration tempConfig = plugin.clone();

                //注意 range前后的()
                tempQuerySql = buildQuerySql(column, table, where)
                        + (hasWhere ? " and " : " where ") + range;

                LOG.info("splitted tempQuerySql:" + tempQuerySql);

                tempConfig.set(Key.QUERY_SQL, tempQuerySql);
                pluginParams.add(tempConfig);

            }
        } else {
            pluginParams.add(plugin);
        }

        return pluginParams;
    }

    protected static String buildQuerySql(String column, String table,
                                          String where) {
        String querySql = null;

        if (StringUtils.isBlank(where)) {
            querySql = String.format(Constant.QUERY_SQL_TEMPLATE_WHITOUT_WHERE,
                    column, table);
        } else {
            querySql = String.format(Constant.QUERY_SQL_TEMPLATE, column,
                    table, where);
        }

        return querySql;
    }

    private static Pair<Object, Object> getPkRange(Configuration configuration) {
        List<String> sqls = genPKRangeSQL(configuration);

        String checkPKSQL = sqls.get(0);
        String pkRangeSQL = sqls.get(1);

        String jdbcURL = configuration.getString(Key.JDBC_URL);
        String username = configuration.getString(Key.USERNAME);
        String password = configuration.getString(Key.PASSWORD);

        Connection conn = null;
        ResultSet rs = null;
        Pair<Object, Object> minMaxPK = null;
        try {
            conn = DBUtil.getConnection(DataBaseType.MySql, jdbcURL, username, password);
            rs = DBUtil.query(conn, checkPKSQL, Integer.MIN_VALUE);
            while (rs.next()) {
                //要求主键对应值非空
                if (rs.getLong(1) > 0L) {
                    throw new DataXException(MysqlReaderErrorCode.ILLEGAL_SPLIT_PK,
                            "Configured PK has null value!");
                }
            }
            rs = DBUtil.query(conn, pkRangeSQL, Integer.MIN_VALUE);
            ResultSetMetaData rsMetaData = rs.getMetaData();
            if (isPKTypeValid(rsMetaData)) {
                if (isStringType(rsMetaData.getColumnType(1))) {
                    configuration.set(Constant.PK_TYPE, Constant.PK_TYPE_STRING);
                    while (rs.next()) {
                        minMaxPK = new ImmutablePair<Object, Object>(rs.getString(1), rs.getString(2));
                    }
                } else if (isLongType(rsMetaData.getColumnType(1))) {
                    configuration.set(Constant.PK_TYPE, Constant.PK_TYPE_LONG);

                    while (rs.next()) {
                        minMaxPK = new ImmutablePair<Object, Object>(rs.getString(1), rs.getString(2));
                    }
                } else {
                    LOG.warn("pk type not long nor string. split single table failed, use no-split strategy.");
                }
            } else {
                LOG.warn("pk type not long nor string. split single table failed, use no-split strategy.");
            }
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.error(e.getMessage(), e);
            }
            LOG.warn("split single table failed, use no-split strategy.");
        } finally {
            DBUtil.closeDBResources(rs, null, conn);
        }

        return minMaxPK;
    }

    private static boolean isPKTypeValid(ResultSetMetaData rsMetaData) {
        boolean ret = false;
        try {
            int minType = rsMetaData.getColumnType(1);
            int maxType = rsMetaData.getColumnType(2);

            boolean isNumberType = isLongType(minType);

            boolean isStringType = isStringType(minType);

            if (minType == maxType && (isNumberType || isStringType)) {
                ret = true;
            }
        } catch (Exception e) {
            LOG.error("error when get splitPk type");
            throw new DataXException(
                    MysqlReaderErrorCode.ILLEGAL_SPLIT_PK,
                    "error when get splitPk type");
        }
        return ret;
    }

    private static boolean isLongType(int type) {
        return type == Types.BIGINT || type == Types.INTEGER
                || type == Types.SMALLINT || type == Types.TINYINT;
    }

    private static boolean isStringType(int type) {
        return type == Types.CHAR || type == Types.NCHAR
                || type == Types.VARCHAR || type == Types.LONGVARCHAR ||
                type == Types.NVARCHAR;
    }

    private static List<String> genPKRangeSQL(Configuration plugin) {
        List<String> sqls = new ArrayList<String>();

        String splitPK = plugin.getString(Key.SPLIT_PK).trim();
        String table = plugin.getString(Key.TABLE).trim();
        String where = plugin.getString(Key.WHERE, null);

        String checkPKTemplate = "SELECT COUNT(1) FROM %s WHERE %s IS NULL";
        String checkPKSQL = String.format(checkPKTemplate, table, splitPK);

        String minMaxTemplate = "SELECT MIN(%s),MAX(%s) FROM %s";
        String pkRangeSQL = String.format(minMaxTemplate, splitPK, splitPK,
                table);
        if (StringUtils.isNotBlank(where)) {
            pkRangeSQL += " WHERE " + where;
        }

        sqls.add(checkPKSQL);
        sqls.add(pkRangeSQL);
        return sqls;
    }

}