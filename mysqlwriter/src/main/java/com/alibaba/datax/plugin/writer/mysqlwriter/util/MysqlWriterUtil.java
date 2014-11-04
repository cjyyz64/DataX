package com.alibaba.datax.plugin.writer.mysqlwriter.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.writer.mysqlwriter.Constant;
import com.alibaba.datax.plugin.writer.mysqlwriter.Key;
import com.alibaba.datax.plugin.writer.mysqlwriter.MysqlWriterErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class MysqlWriterUtil {
    private static final Logger LOG = LoggerFactory
            .getLogger(MysqlWriterUtil.class);

    public static List<Configuration> doSplit(Configuration simplifiedConf,
                                              int adviceNumber) {

        List<Configuration> splitResultConfigs = new ArrayList<Configuration>();

        int tableNumber = simplifiedConf.getInt(Constant.TABLE_NUMBER_MARK)
                .intValue();

        //处理单表的情况
        if (tableNumber == 1) {
            //由于在之前的  master prepare 中已经把 table,jdbcUrl 提取出来，所以这里处理十分简单
            for (int j = 0; j < adviceNumber; j++) {
                splitResultConfigs.add(simplifiedConf.clone());
            }

            return splitResultConfigs;
        }

        if (tableNumber != adviceNumber) {
            throw new DataXException(MysqlWriterErrorCode.CONF_ERROR,
                    String.format("tableNumber:[%s], but adviceNumb:[%s]",
                            tableNumber, adviceNumber));
        }

        String jdbcUrl = null;
        List<String> preSqls = simplifiedConf.getList(Key.PRE_SQL, String.class);
        List<String> postSqls = simplifiedConf.getList(Key.POST_SQL, String.class);

        List<Object> conns = simplifiedConf.getList(Constant.CONN_MARK,
                Object.class);

        for (int i = 0, len = conns.size(); i < len; i++) {
            Configuration sliceConfig = simplifiedConf.clone();

            Configuration connConf = Configuration.from(conns.get(i).toString());
            jdbcUrl = connConf.getString(Key.JDBC_URL);
            sliceConfig.set(Key.JDBC_URL, appendJDBCSuffix(jdbcUrl));

            sliceConfig.remove(Constant.CONN_MARK);

            List<String> tables = connConf.getList(Key.TABLE, String.class);

            for (String table : tables) {
                Configuration tempSlice = sliceConfig.clone();
                tempSlice.set(Key.TABLE, table);
                tempSlice.set(Key.PRE_SQL, renderPreOrPostSqls(preSqls, table));
                tempSlice.set(Key.POST_SQL, renderPreOrPostSqls(postSqls, table));

                splitResultConfigs.add(tempSlice);
            }

        }

        return splitResultConfigs;
    }


    public static List<String> renderPreOrPostSqls(List<String> preOrPostSqls, String tableName) {
        if (null == preOrPostSqls) {
            return Collections.emptyList();
        }

        List<String> renderedSqls = new ArrayList<String>();
        for (String sql : preOrPostSqls) {
            renderedSqls.add(sql.replace(Constant.TABLE_NAME_PLACEHOLDER, tableName));
        }

        return renderedSqls;
    }

    public static String appendJDBCSuffix(String jdbc) {
        String suffix = "yearIsDateType=false&zeroDateTimeBehavior=convertToNull";

        if (jdbc.contains("?")) {
            return jdbc + "&" + suffix;
        } else {
            return jdbc + "?" + suffix;
        }
    }


    public static void executeSqls(Connection conn, List<String> sqls, String basicMessage) {
        Statement stmt = null;
        String currentSql = null;
        try {
            stmt = conn.createStatement();
            for (String sql : sqls) {
                currentSql = sql;
                DBUtil.executeSqlWithoutResultSet(stmt, sql);
            }
        } catch (Exception e) {
            LOG.error("execute sql:[{}] failed, {}.", currentSql,
                    basicMessage);
            throw DataXException.asDataXException(MysqlWriterErrorCode.EXECUTE_SQL_ERROR, e);
        } finally {
            DBUtil.closeDBResources(null, stmt, null);
        }
    }

}
