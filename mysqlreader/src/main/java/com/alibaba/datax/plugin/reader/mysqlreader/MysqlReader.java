package com.alibaba.datax.plugin.reader.mysqlreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.reader.util.ReaderSplitUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;

public class MysqlReader extends Reader {

    private static final DataBaseType DATABASE_TYPE = DataBaseType.MySql;

    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);

        private Configuration originalConfig = null;
        private CommonRdbmsReader.Job commonRdbmsReaderJob;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();

            Integer userConfigedFetchSize = this.originalConfig.getInt(Constant.FETCH_SIZE);
            if (userConfigedFetchSize != null) {
                LOG.warn("对 mysqlreader 不需要配置 fetchSize, mysqlreader 将会忽略这项配置. 如果您不想再看到此警告,请去除fetchSize 配置.");
            }

            this.originalConfig.set(Constant.FETCH_SIZE, Integer.MIN_VALUE);

            this.commonRdbmsReaderJob = new CommonRdbmsReader.Job(DATABASE_TYPE);
            this.commonRdbmsReaderJob.init(this.originalConfig);
        }

        @Override
        public void preCheck(int adviceNumber){
            init();
            List<Configuration> confs = ReaderSplitUtil.doSplit(this.originalConfig,adviceNumber);
            for (Configuration conf:confs){
                String querySql = conf.getString(Key.QUERY_SQL);
                String jdbcUrl = conf.getString(Key.JDBC_URL);
                String username = conf.getString(Key.USERNAME);
                String password = conf.getString(Key.PASSWORD);
                Connection conn = DBUtil.getConnection(DATABASE_TYPE.MySql, jdbcUrl,
                        username, password);
                int fetchSize = 1;
                ResultSet rs = null;
                try {
                    rs = DBUtil.query(conn, querySql, fetchSize);

                } catch (Exception e) {
                    throw analysisException(DATABASE_TYPE.MySql,e,querySql);
//                throw DataXException.asDataXException(
//                        DBUtilErrorCode.READ_RECORD_FAIL, String.format(
//                                "读数据库数据失败. 上下文信息是:%s , 执行的语句是:[%s]",
//                                basicMsg, querySql), e);
                } finally {
                    DBUtil.closeDBResources(null, conn);
                }
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
            return DataXException.asDataXException(DBUtilErrorCode.CONN_DB_ERROR,querySql+e);
        }
    }

    private DBUtilErrorCode mySqlConnectionErrorAna(String e){
//        if (e.contains(Constant.MYSQL_TABLE_NAME_ERR1) && e.contains(Constant.MYSQL_TABLE_NAME_ERR2)){
//            return DBUtilErrorCode.MYSQL_QUERY_TABLE_NAME_ERROR;
//        }else if (e.contains(Constant.MYSQL_INSERT_PRI)){
//            return DBUtilErrorCode.MYSQL_QUERY_INSERT_PRI_ERROR;
//        }else if (e.contains(Constant.MYSQL_COLUMN1) && e.contains(Constant.MYSQL_COLUMN2)){
//            return DBUtilErrorCode.MYSQL_QUERY_COLUMN_ERROR;
//        }else if (e.contains(Constant.MYSQL_WHERE)){
//            return DBUtilErrorCode.MYSQL_QUERY_SQL_ERROR;
//        }
        return DBUtilErrorCode.READ_RECORD_FAIL;
    }

    private DBUtilErrorCode oracleConnectionErrorAna(String e){
//        if (e.contains(Constant.ORACLE_TABLE_NAME)){
//            return DBUtilErrorCode.ORACLE_QUERY_TABLE_NAME_ERROR;
//        }else if (e.contains(Constant.ORACLE_SQL)){
//            return DBUtilErrorCode.ORACLE_QUERY_SQL_ERROR;
//        }else if (e.contains(Constant.ORACLE_INSERT_PRI)){
//            return DBUtilErrorCode.ORACLE_QUERY_INSERT_PRI_ERROR;
//        }

        return DBUtilErrorCode.READ_RECORD_FAIL;
    }

        @Override
        public List<Configuration> split(int adviceNumber) {
            return this.commonRdbmsReaderJob.split(this.originalConfig, adviceNumber);
        }

        @Override
        public void post() {
            this.commonRdbmsReaderJob.post(this.originalConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsReaderJob.destroy(this.originalConfig);
        }

    }

    public static class Task extends Reader.Task {

        private Configuration readerSliceConfig;
        private CommonRdbmsReader.Task commonRdbmsReaderTask;

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            this.commonRdbmsReaderTask = new CommonRdbmsReader.Task(DATABASE_TYPE);
            this.commonRdbmsReaderTask.init(this.readerSliceConfig);

        }

        @Override
        public void startRead(RecordSender recordSender) {
            int fetchSize = this.readerSliceConfig.getInt(Constant.FETCH_SIZE);

            this.commonRdbmsReaderTask.startRead(this.readerSliceConfig, recordSender,
                    super.getTaskPluginCollector(), fetchSize);
        }

        @Override
        public void post() {
            this.commonRdbmsReaderTask.post(this.readerSliceConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsReaderTask.destroy(this.readerSliceConfig);
        }

    }

}
