package com.alibaba.datax.plugin.rdbms.reader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.util.OriginalConfPretreatmentUtil;
import com.alibaba.datax.plugin.rdbms.reader.util.ReaderSplitUtil;
import com.alibaba.datax.plugin.rdbms.reader.util.SingleTableSplitUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.druid.sql.parser.ParserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.List;

public class CommonRdbmsReader {

    public static class Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);

        public Job(DataBaseType dataBaseType) {
            OriginalConfPretreatmentUtil.DATABASE_TYPE = dataBaseType;
            SingleTableSplitUtil.DATABASE_TYPE = dataBaseType;
        }

        public void init(Configuration originalConfig) {

            OriginalConfPretreatmentUtil.doPretreatment(originalConfig);

            LOG.debug("After job init(), job config now is:[\n{}\n]",
                    originalConfig.toJSON());
        }

        public List<Configuration> split(Configuration originalConfig,
                                         int adviceNumber) {
            return ReaderSplitUtil.doSplit(originalConfig, adviceNumber);
        }

        public void post(Configuration originalConfig) {
            // do nothing
        }

        public void destroy(Configuration originalConfig) {
            // do nothing
        }

    }

    public static class Task {
        private static final Logger LOG = LoggerFactory
                .getLogger(Task.class);

        private DataBaseType dataBaseType;

        private String username;
        private String password;
        private String jdbcUrl;
        private String mandatoryEncoding;

        // 作为日志显示信息时，需要附带的通用信息。比如信息所对应的数据库连接等信息，针对哪个表做的操作
        private String basicMsg;

        public Task(DataBaseType dataBaseType) {
            this.dataBaseType = dataBaseType;
        }

        public void init(Configuration readerSliceConfig) {

			/* for database connection */

            this.username = readerSliceConfig.getString(Key.USERNAME);
            this.password = readerSliceConfig.getString(Key.PASSWORD);
            this.jdbcUrl = readerSliceConfig.getString(Key.JDBC_URL);
            this.mandatoryEncoding = readerSliceConfig.getString(Key.MANDATORY_ENCODING, "");

            basicMsg = String.format("jdbcUrl:[%s]", this.jdbcUrl);
        }

        public void startRead(Configuration readerSliceConfig,
                              RecordSender recordSender,
                              TaskPluginCollector taskPluginCollector, int fetchSize) {
            String querySql = readerSliceConfig.getString(Key.QUERY_SQL);

            LOG.info("Begin to read record by Sql: [{}\n] {}.",
                    querySql, basicMsg);

            Connection conn = DBUtil.getConnection(this.dataBaseType, jdbcUrl,
                    username, password);

            // session config .etc related
            DBUtil.dealWithSessionConfig(conn, readerSliceConfig,
                    this.dataBaseType, basicMsg);

            int columnNumber = 0;
            ResultSet rs = null;
            try {
                boolean isPassSqlValid = DBUtil.sqlValid(querySql,dataBaseType);
                if (isPassSqlValid == true){
                    rs = DBUtil.query(conn, querySql, fetchSize);
                    ResultSetMetaData metaData = rs.getMetaData();
                    columnNumber = metaData.getColumnCount();

                    while (rs.next()) {
                        ResultSetReadProxy.transportOneRecord(recordSender, rs,
                                metaData, columnNumber, mandatoryEncoding, taskPluginCollector);
                    }

                    LOG.info("Finished read record by Sql: [{}\n] {}.",
                            querySql, basicMsg);
                }
            } catch (ParserException e){
                if (dataBaseType.equals(DataBaseType.MySql)){
                    throw DataXException.asDataXException(DBUtilErrorCode.MYSQL_QUERY_SQL_ERROR,querySql+e);
                }else if (dataBaseType.equals(DataBaseType.Oracle)){
                    throw DataXException.asDataXException(DBUtilErrorCode.ORACLE_QUERY_SQL_ERROR,querySql+e);
                }else{
                    throw DataXException.asDataXException(DBUtilErrorCode.READ_RECORD_FAIL,querySql+e);
                }

            }
            catch (Exception e) {
                throw analysisException(this.dataBaseType,e,querySql);
            } finally {
                DBUtil.closeDBResources(null, conn);
            }
        }

        private DataXException analysisException(DataBaseType dataBaseType, Exception e,String querySql){
            if (dataBaseType.equals(DataBaseType.MySql)){
                DBUtilErrorCode dbUtilErrorCode = mySqlConnectionErrorAna(e.getMessage());
                return DataXException.asDataXException(dbUtilErrorCode,querySql+e);
            }else if (dataBaseType.equals(DataBaseType.Oracle)){
                DBUtilErrorCode dbUtilErrorCode = oracleConnectionErrorAna(e.getMessage());
                return DataXException.asDataXException(dbUtilErrorCode,querySql+e);
            }else{
                return DataXException.asDataXException(DBUtilErrorCode.READ_RECORD_FAIL,querySql+e);
            }
        }

        private DBUtilErrorCode mySqlConnectionErrorAna(String e){
            if (e.contains(Constant.MYSQL_TABLE_NAME_ERR1) && e.contains(Constant.MYSQL_TABLE_NAME_ERR2)){
                return DBUtilErrorCode.MYSQL_QUERY_TABLE_NAME_ERROR;
            }else if (e.contains(Constant.MYSQL_INSERT_PRI)){
                return DBUtilErrorCode.MYSQL_QUERY_INSERT_PRI_ERROR;
            }else if (e.contains(Constant.MYSQL_COLUMN1) && e.contains(Constant.MYSQL_COLUMN2)){
                return DBUtilErrorCode.MYSQL_QUERY_COLUMN_ERROR;
            }else if (e.contains(Constant.MYSQL_WHERE)){
                return DBUtilErrorCode.MYSQL_QUERY_SQL_ERROR;
            }
            return DBUtilErrorCode.READ_RECORD_FAIL;
        }

        private DBUtilErrorCode oracleConnectionErrorAna(String e){
            if (e.contains(Constant.ORACLE_TABLE_NAME)){
                return DBUtilErrorCode.ORACLE_QUERY_TABLE_NAME_ERROR;
            }else if (e.contains(Constant.ORACLE_SQL)){
                return DBUtilErrorCode.ORACLE_QUERY_SQL_ERROR;
            }else if (e.contains(Constant.ORACLE_INSERT_PRI)){
                return DBUtilErrorCode.ORACLE_QUERY_INSERT_PRI_ERROR;
            }

            return DBUtilErrorCode.READ_RECORD_FAIL;
        }

        public void post(Configuration originalConfig) {
            // do nothing
        }

        public void destroy(Configuration originalConfig) {
            // do nothing
        }
    }
}
