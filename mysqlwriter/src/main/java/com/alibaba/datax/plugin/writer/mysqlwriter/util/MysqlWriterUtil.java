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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class MysqlWriterUtil {
    private static final Logger LOG = LoggerFactory
            .getLogger(MysqlWriterUtil.class);

    private static final boolean IS_DEBUG = LOG.isDebugEnabled();

    public static List<Configuration> doSplit(Configuration simplifiedConf,
                                              int adviceNumber) {

        List<Configuration> splitResultConfigs = new ArrayList<Configuration>();

        int tableNumber = simplifiedConf.getInt(Constant.TABLE_NUMBER_MARK)
                .intValue();

        if (tableNumber != adviceNumber && tableNumber != 1) {
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

            if (tableNumber == 1) {
                String table = tables.get(0);

                Configuration tempSlice = sliceConfig.clone();
                tempSlice.set(Key.TABLE, table);

                //delete it
                tempSlice.set(Key.PRE_SQL, renderPreOrPostSqls(preSqls, table));
                tempSlice.set(Key.POST_SQL, renderPreOrPostSqls(postSqls, table));

                for (int j = 0; j < adviceNumber; j++) {
                    splitResultConfigs.add(tempSlice.clone());
                }
            } else {
                for (String table : tables) {
                    Configuration tempSlice = sliceConfig.clone();
                    tempSlice.set(Key.TABLE, table);
                    tempSlice.set(Key.PRE_SQL, renderPreOrPostSqls(preSqls, table));
                    tempSlice.set(Key.POST_SQL, renderPreOrPostSqls(postSqls, table));

                    splitResultConfigs.add(tempSlice);
                }
            }

        }

        if (IS_DEBUG) {
            LOG.debug("after split(), splitResultConfigs:[\n{}\n].", splitResultConfigs);
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
        try {
            stmt = conn.createStatement();
        } catch (SQLException e) {
            throw new DataXException(MysqlWriterErrorCode.EXECUTE_SQL_ERROR, e);
        } finally {
            DBUtil.closeDBResources(null, stmt, null);
        }

        for (String sql : sqls) {
            try {
                DBUtil.executeSqlWithoutResultSet(stmt, sql);
            } catch (SQLException e) {
                LOG.error("execute sql:[{}] failed, {}.", sql,
                        basicMessage);
                throw new DataXException(
                        MysqlWriterErrorCode.EXECUTE_SQL_ERROR, e);
            } finally {
                DBUtil.closeDBResources(null, stmt, null);
            }
        }

    }

}
