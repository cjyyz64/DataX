package com.alibaba.datax.plugin.rdbms.reader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.StrUtil;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.RangeSplitUtil;
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

    public static DataBaseType DATABASE_TYPE;

    private SingleTableSplitUtil() {
    }

    public static List<Configuration> splitSingleTable(
            Configuration configuration, int adviceNum) {
        List<Configuration> pluginParams = new ArrayList<Configuration>();

        Pair<Object, Object> minMaxPK = getPkRange(configuration);

        if (null == minMaxPK) {
            throw new DataXException(DBUtilErrorCode.ILLEGAL_SPLIT_PK,
                    "split table with splitPk failed.");
        }

        String splitPkName = configuration.getString(Key.SPLIT_PK);
        String column = configuration.getString(Key.COLUMN);
        String table = configuration.getString(Key.TABLE);
        String where = configuration.getString(Key.WHERE, null);

        boolean hasWhere = StringUtils.isNotBlank(where);

        configuration.set(Key.QUERY_SQL, buildQuerySql(column, table, where));
        if (null == minMaxPK.getLeft() || null == minMaxPK.getRight()) {
            // 切分后获取到的start/end 有 Null 的情况
            pluginParams.add(configuration);
            return pluginParams;
        }

        boolean isStringType = Constant.PK_TYPE_STRING.equals(configuration
                .getString(Constant.PK_TYPE));
        boolean isLongType = Constant.PK_TYPE_LONG.equals(configuration
                .getString(Constant.PK_TYPE));

        List<String> rangeList = null;
        if (isStringType) {
            rangeList = RangeSplitUtil.splitAndWrap(
                    String.valueOf(minMaxPK.getLeft()),
                    String.valueOf(minMaxPK.getRight()), adviceNum,
                    splitPkName, "'", DATABASE_TYPE);
        } else if (isLongType) {
            rangeList = RangeSplitUtil.splitAndWrap(
                    Long.parseLong(minMaxPK.getLeft().toString()),
                    Long.parseLong(minMaxPK.getRight().toString()), adviceNum,
                    splitPkName);
        } else {
            String businessMessage = "Unsupported splitPk type.";
            String message = StrUtil.buildOriginalCauseMessage(
                    businessMessage, null);

            LOG.error(message);
            throw new DataXException(DBUtilErrorCode.ILLEGAL_SPLIT_PK, businessMessage);
        }

        String tempQuerySql = null;
        if (null != rangeList) {
            for (String range : rangeList) {
                Configuration tempConfig = configuration.clone();

                tempQuerySql = buildQuerySql(column, table, where)
                        + (hasWhere ? " and " : " where ") + range;

                LOG.info("After split, tempQuerySql=[\n{}\n].", tempQuerySql);

                tempConfig.set(Key.QUERY_SQL, tempQuerySql);
                pluginParams.add(tempConfig);
            }
        } else {
            pluginParams.add(configuration);
        }

        //deal pk is null
        Configuration tempConfig = configuration.clone();
        tempQuerySql = buildQuerySql(column, table, where)
                + (hasWhere ? " and " : " where ") + String.format(" %s IS NULL", splitPkName);

        LOG.info("After split, tempQuerySql=[\n{}\n].", tempQuerySql);

        tempConfig.set(Key.QUERY_SQL, tempQuerySql);
        pluginParams.add(tempConfig);

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
        String pkRangeSQL = genPKRangeSQL(configuration);

        int fetchSize = configuration.getInt(Constant.FETCH_SIZE);

        String jdbcURL = configuration.getString(Key.JDBC_URL);
        String username = configuration.getString(Key.USERNAME);
        String password = configuration.getString(Key.PASSWORD);

        Connection conn = null;
        ResultSet rs = null;
        Pair<Object, Object> minMaxPK = null;
        try {
            conn = DBUtil.getConnection(DATABASE_TYPE, jdbcURL, username,
                    password);

            rs = DBUtil.query(conn, pkRangeSQL, fetchSize);
            ResultSetMetaData rsMetaData = rs.getMetaData();
            if (isPKTypeValid(rsMetaData)) {
                if (isStringType(rsMetaData.getColumnType(1))) {
                    configuration.set(Constant.PK_TYPE, Constant.PK_TYPE_STRING);
                    while (rs.next()) {
                        minMaxPK = new ImmutablePair<Object, Object>(
                                rs.getString(1), rs.getString(2));
                    }
                } else if (isLongType(rsMetaData.getColumnType(1))) {
                    configuration.set(Constant.PK_TYPE, Constant.PK_TYPE_LONG);

                    while (rs.next()) {
                        minMaxPK = new ImmutablePair<Object, Object>(
                                rs.getString(1), rs.getString(2));
                    }
                } else {
                    throw new DataXException(DBUtilErrorCode.ILLEGAL_SPLIT_PK,
                            "unsupported splitPk type，pk type not long nor string");
                }
            } else {
                throw new DataXException(DBUtilErrorCode.ILLEGAL_SPLIT_PK,
                        "unsupported splitPk type，pk type not long nor string");
            }
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.error(e.getMessage(), e);
            }

            throw new DataXException(DBUtilErrorCode.ILLEGAL_SPLIT_PK, e);
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
            throw new DataXException(DBUtilErrorCode.ILLEGAL_SPLIT_PK,
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
                || type == Types.VARCHAR || type == Types.LONGVARCHAR
                || type == Types.NVARCHAR;
    }


    private static String genPKRangeSQL(Configuration configuration) {

        String splitPK = configuration.getString(Key.SPLIT_PK).trim();
        String table = configuration.getString(Key.TABLE).trim();
        String where = configuration.getString(Key.WHERE, null);

        String minMaxTemplate = "SELECT MIN(%s),MAX(%s) FROM %s";
        String pkRangeSQL = String.format(minMaxTemplate, splitPK, splitPK, table);
        if (StringUtils.isNotBlank(where)) {
            pkRangeSQL = String.format("%s WHERE (%s AND %s IS NOT NULL)", pkRangeSQL,
                    where, splitPK);
        }

        return pkRangeSQL;
    }

}