package com.alibaba.datax.plugin.rdbms.writer.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.RdbmsException;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.druid.sql.parser.ParserException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.util.*;

public final class WriterUtil {
    private static final Logger LOG = LoggerFactory.getLogger(WriterUtil.class);

    //TODO 切分报错
    public static List<Configuration> doSplit(Configuration simplifiedConf,
                                              int adviceNumber) {

        List<Configuration> splitResultConfigs = new ArrayList<Configuration>();

        int tableNumber = simplifiedConf.getInt(Constant.TABLE_NUMBER_MARK);

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
                    String.format("您的配置文件中的列配置信息有误. 您要写入的目的端的表个数是:%s , 但是根据系统建议需要切分的份数是：%s. 请检查您的配置并作出修改.",
                            tableNumber, adviceNumber));
        }

        String jdbcUrl;
        List<String> preSqls = simplifiedConf.getList(Key.PRE_SQL, String.class);
        List<String> postSqls = simplifiedConf.getList(Key.POST_SQL, String.class);

        List<Object> conns = simplifiedConf.getList(Constant.CONN_MARK,
                Object.class);

        for (Object conn : conns) {
            Configuration sliceConfig = simplifiedConf.clone();

            Configuration connConf = Configuration.from(conn.toString());
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
            //preSql为空时，不加入执行队列
            if (StringUtils.isNotBlank(sql)) {
                renderedSqls.add(sql.replace(Constant.TABLE_NAME_PLACEHOLDER, tableName));
            }
        }

        return renderedSqls;
    }

    public static void executeSqls(Connection conn, List<String> sqls, String basicMessage,DataBaseType dataBaseType) {
        Statement stmt = null;
        String currentSql = null;
        try {
            stmt = conn.createStatement();
            for (String sql : sqls) {
                currentSql = sql;
                DBUtil.sqlValid(sql,dataBaseType);
                DBUtil.executeSqlWithoutResultSet(stmt, sql);
            }
        } catch (ParserException e){
            throw RdbmsException.asSqlParserException(dataBaseType,e,currentSql);
        } catch (Exception e) {
            if (dataBaseType == DataBaseType.MySql || dataBaseType == DataBaseType.Oracle){
                throw RdbmsException.asQueryException(dataBaseType,e,currentSql,null,null);
            }else{
                throw DataXException.asDataXException(DBUtilErrorCode.SQL_EXECUTE_FAIL,
                        String.format("您的sql配置有误. 因为根据您的配置执行 Sql:%s 语句失败，相关上下文信息是:%s. 请检查您的配置并作出修改.", currentSql, basicMessage), e);
            }
        } finally {
            DBUtil.closeDBResources(null, stmt, null);
        }
    }

    public static String getWriteTemplate(List<String> columnHolders, List<String> valueHolders, String writeMode){
		boolean isWriteModeLegal = writeMode.trim().toLowerCase().startsWith("insert")
				|| writeMode.trim().toLowerCase().startsWith("replace");

		if (!isWriteModeLegal) {
			throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
				String.format("您所配置的 writeMode:%s 错误. 因为DataX 目前仅支持replace 或 insert 方式. 请检查您的配置并作出修改.", writeMode));
		}

		String writeDataSqlTemplate = new StringBuilder().append(writeMode)
				.append(" INTO %s (").append(StringUtils.join(columnHolders, ","))
				.append(") VALUES(").append(StringUtils.join(valueHolders, ","))
				.append(")").toString();

		return writeDataSqlTemplate;
	}

    public static void preCheckPrePareSQL(Configuration originalConfig, DataBaseType type) {
        List<Object> conns = originalConfig.getList(Constant.CONN_MARK, Object.class);
        Configuration connConf = Configuration.from(conns.get(0).toString());
        String table = connConf.getList(Key.TABLE, String.class).get(0);

        List<String> preSqls = originalConfig.getList(Key.PRE_SQL,
                String.class);
        List<String> renderedPreSqls = WriterUtil.renderPreOrPostSqls(
                preSqls, table);

        if (null != renderedPreSqls && !renderedPreSqls.isEmpty()) {
            LOG.info("Begin to preCheck preSqls:[{}].",
                    StringUtils.join(renderedPreSqls, ";"));
            for(String sql : renderedPreSqls) {
                try{
                    DBUtil.sqlValid(sql, type);
                }catch(ParserException e) {
                    throw RdbmsException.asPreSQLParserException(type,e,sql);
                }
            }
        }
    }

    public static void preCheckPostSQL(Configuration originalConfig, DataBaseType type) {
        List<Object> conns = originalConfig.getList(Constant.CONN_MARK, Object.class);
        Configuration connConf = Configuration.from(conns.get(0).toString());
        String table = connConf.getList(Key.TABLE, String.class).get(0);

        List<String> postSqls = originalConfig.getList(Key.POST_SQL,
                String.class);
        List<String> renderedPostSqls = WriterUtil.renderPreOrPostSqls(
                postSqls, table);
        if (null != renderedPostSqls && !renderedPostSqls.isEmpty()) {

            LOG.info("Begin to preCheck postSqls:[{}].",
                    StringUtils.join(renderedPostSqls, ";"));
            for(String sql : renderedPostSqls) {
                try{
                    DBUtil.sqlValid(sql, type);
                }catch(ParserException e){
                    throw RdbmsException.asPostSQLParserException(type,e,sql);
                }

            }
        }
    }

    /*目前只支持MySQL Writer跟Oracle Writer*/
    public static void writerPreCheck(Configuration originalConfig,DataBaseType dataBaseType){
        /*检查PreSql跟PostSql语句*/
        WriterUtil.preCheckPrePareSQL(originalConfig, dataBaseType);
        WriterUtil.preCheckPostSQL(originalConfig, dataBaseType);

        /*检查insert 跟delete权限*/
        String username = originalConfig.getString(Key.USERNAME);
        String password = originalConfig.getString(Key.PASSWORD);
        List<Object> connections = originalConfig.getList(Constant.CONN_MARK,
                Object.class);

        for (int i = 0, len = connections.size(); i < len; i++) {
            Configuration connConf = Configuration.from(connections.get(i).toString());
            String jdbcUrl = connConf.getString(Key.JDBC_URL);
            List<String> expandedTables = connConf.getList(Key.TABLE, String.class);
            boolean hasInsertPri = DBUtil.checkInsertPrivilege(dataBaseType,jdbcUrl,username,password,expandedTables);

            if(!hasInsertPri){
                throw RdbmsException.asDeletePriException(dataBaseType,originalConfig.getString(Key.USERNAME), jdbcUrl);
            }

            if(DBUtil.needCheckDeletePrivilege(originalConfig)) {
                boolean hasDeletePri = DBUtil.checkDeletePrivilege(dataBaseType,jdbcUrl, username, password, expandedTables);
                if(!hasDeletePri) {
                    throw RdbmsException.asDeletePriException(dataBaseType, originalConfig.getString(Key.USERNAME), jdbcUrl);
                }
            }
        }
    }
}
