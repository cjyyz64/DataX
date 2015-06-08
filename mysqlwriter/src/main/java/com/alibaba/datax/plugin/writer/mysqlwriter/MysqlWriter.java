package com.alibaba.datax.plugin.writer.mysqlwriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.druid.sql.parser.ParserException;

import java.util.List;


//TODO writeProxy
public class MysqlWriter extends Writer {
    private static final DataBaseType DATABASE_TYPE = DataBaseType.MySql;

    public static class Job extends Writer.Job {
        private Configuration originalConfig = null;
        private CommonRdbmsWriter.Job commonRdbmsWriterJob;

        @Override
        public void init() {
            boolean isPreCheck = false;
            this.originalConfig = super.getPluginJobConf();
            this.commonRdbmsWriterJob = new CommonRdbmsWriter.Job(DATABASE_TYPE,isPreCheck);
            this.commonRdbmsWriterJob.init(this.originalConfig);
        }

        @Override
        public void preCheck(){
            boolean isPreCheck = true;
            this.originalConfig = super.getPluginJobConf();
            this.commonRdbmsWriterJob = new CommonRdbmsWriter.Job(DATABASE_TYPE,isPreCheck);
            this.commonRdbmsWriterJob.init(this.originalConfig);

            /*检查PreSql跟PostSql语句*/
            List<String> preSqls = this.originalConfig.getList(Key.PRE_SQL,String.class);
            List<String> postSqls = this.originalConfig.getList(Key.POST_SQL,String.class);
            try{
                if (preSqls != null && !preSqls.isEmpty()){
                    for (String preSql:preSqls){
                        DBUtil.sqlValid(preSql,DATABASE_TYPE);
                    }
                }
            }catch (ParserException e){
                throw DataXException.asDataXException(DBUtilErrorCode.MYSQL_PRE_SQL_ERROR,e.getMessage());
            }
            try{
                if (postSqls != null && !postSqls.isEmpty()){
                    for (String postSql:postSqls){
                        DBUtil.sqlValid(postSql,DATABASE_TYPE);
                    }
                }
            }catch (ParserException e){
                throw DataXException.asDataXException(DBUtilErrorCode.MYSQL_PRE_SQL_ERROR,e.getMessage());
            }
            /*检查insert 权限*/
            String username = this.originalConfig.getString(Key.USERNAME);
            String password = this.originalConfig.getString(Key.PASSWORD);
            List<Object> connections = originalConfig.getList(Constant.CONN_MARK,
                    Object.class);

            for (int i = 0, len = connections.size(); i < len; i++) {
                Configuration connConf = Configuration.from(connections.get(i).toString());
                String dbName = connConf.getString(Key.DBNAME);
                String jdbcUrl = connConf.getString(Key.JDBC_URL);
                List<String> expandedTables = connConf.getList(Key.TABLE, String.class);
                boolean hasInsertPri = DBUtil.hasInsertPrivilege(DATABASE_TYPE, jdbcUrl,dbName, username, password, expandedTables);

                if(!hasInsertPri){
                    throw DataXException.asDataXException(DBUtilErrorCode.NO_INSERT_PRIVILEGE, originalConfig.getString(Key.USERNAME) + jdbcUrl);
                }
            }
        }

        // 一般来说，是需要推迟到 task 中进行pre 的执行（单表情况例外）
        @Override
        public void prepare() {
            this.commonRdbmsWriterJob.prepare(this.originalConfig);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            return this.commonRdbmsWriterJob.split(this.originalConfig, mandatoryNumber);
        }

        // 一般来说，是需要推迟到 task 中进行post 的执行（单表情况例外）
        @Override
        public void post() {
            this.commonRdbmsWriterJob.post(this.originalConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsWriterJob.destroy(this.originalConfig);
        }

    }

    public static class Task extends Writer.Task {
        private Configuration writerSliceConfig;
        private CommonRdbmsWriter.Task commonRdbmsWriterTask;

        @Override
        public void init() {
            this.writerSliceConfig = super.getPluginJobConf();
            this.commonRdbmsWriterTask = new CommonRdbmsWriter.Task(DATABASE_TYPE);
            this.commonRdbmsWriterTask.init(this.writerSliceConfig);
        }

        @Override
        public void prepare() {
            this.commonRdbmsWriterTask.prepare(this.writerSliceConfig);
        }

        //TODO 改用连接池，确保每次获取的连接都是可用的（注意：连接可能需要每次都初始化其 session）
        public void startWrite(RecordReceiver recordReceiver) {
            this.commonRdbmsWriterTask.startWrite(recordReceiver, this.writerSliceConfig,
                    super.getTaskPluginCollector());
        }

        @Override
        public void post() {
            this.commonRdbmsWriterTask.post(this.writerSliceConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsWriterTask.destroy(this.writerSliceConfig);
        }

    }


}
