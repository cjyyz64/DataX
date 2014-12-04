package com.alibaba.datax.plugin.rdbms.writer.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.SqlFormatUtil;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class WriterUtil {
    private static final Logger LOG = LoggerFactory.getLogger(WriterUtil.class);

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
            throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
                    String.format("您要写入的目的端的表个数是:%s , 但是根据系统建议需要切分的份数是：%s .",
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
            sliceConfig.set(Key.JDBC_URL, jdbcUrl);

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
            throw DataXException.asDataXException(DBUtilErrorCode.SQL_EXECUTE_FAIL,
                    String.format("执行 Sql:%s 语句失败. 上下文信息是:%s .", currentSql, basicMessage), e);
        } finally {
            DBUtil.closeDBResources(null, stmt, null);
        }
    }
    
    public static String getWriteTemplate(List<String> columnHolders, List<String> valueHolders, String writeMode){
		boolean isWriteModeLegal = writeMode.trim().toLowerCase().startsWith("insert")
				|| writeMode.trim().toLowerCase().startsWith("replace");

		if (!isWriteModeLegal) {
			throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
				String.format("您所配置的 writeMode:%s 错误. DataX 目前仅支持replace 或 insert 方式.", writeMode));
		}

		String writeDataSqlTemplate = new StringBuilder().append(writeMode)
				.append(" INTO %s (").append(StringUtils.join(columnHolders, ","))
				.append(") VALUES(").append(StringUtils.join(valueHolders, ","))
				.append(")").toString();

		String formattedSql = writeDataSqlTemplate;
		try {
			formattedSql = SqlFormatUtil.format(writeDataSqlTemplate);
		} catch (Exception unused) {
			// ignore it
		}
		return formattedSql;
	}

}
