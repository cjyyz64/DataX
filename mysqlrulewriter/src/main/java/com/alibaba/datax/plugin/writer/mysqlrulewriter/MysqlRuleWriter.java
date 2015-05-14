package com.alibaba.datax.plugin.writer.mysqlrulewriter;

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
import com.alibaba.datax.plugin.rdbms.writer.util.WriterUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public class MysqlRuleWriter extends Writer {
    private static final DataBaseType DATABASE_TYPE = DataBaseType.MySql;

    public static class Job extends Writer.Job {
        private Configuration originalConfig = null;
        private CommonRdbmsWriter.Job commonRdbmsWriterJob;
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();

            this.commonRdbmsWriterJob = new CommonRdbmsWriter.Job(DATABASE_TYPE);
            this.commonRdbmsWriterJob.init(this.originalConfig);
            // 检查 db/table规则 配置（必填）
            originalConfig.getNecessaryValue(Key.DB_NAME_PATTERN, DBUtilErrorCode.REQUIRED_VALUE);
            originalConfig.getNecessaryValue(Key.DB_RULE, DBUtilErrorCode.REQUIRED_VALUE);
            originalConfig.getNecessaryValue(Key.TABLE_NAME_PATTERN, DBUtilErrorCode.REQUIRED_VALUE);
            originalConfig.getNecessaryValue(Key.TABLE_RULE, DBUtilErrorCode.REQUIRED_VALUE);
        }

        // 一般来说，是需要推迟到 task 中进行pre 的执行（单表情况例外）
        @Override
        public void prepare() {
            this.commonRdbmsWriterJob.prepare(this.originalConfig);

            int tableNumber = originalConfig.getInt(Constant.TABLE_NUMBER_MARK);
            if (tableNumber == 1) {
                throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR, "tableNumber=1, mysqlrulewriter只能支持分库分表任务");
            }
            String username = originalConfig.getString(Key.USERNAME);
            String password = originalConfig.getString(Key.PASSWORD);

            //获取presql配置，并执行
            List<String> preSqls = originalConfig.getList(Key.PRE_SQL, String.class);
            originalConfig.remove(Key.PRE_SQL);

            List<Object> conns = originalConfig.getList(Constant.CONN_MARK, Object.class);
            for(Object connConfObject : conns) {
                Configuration connConf = Configuration.from(connConfObject.toString());
                // 这里的 jdbcUrl 已经 append 了合适后缀参数
                String jdbcUrl = connConf.getString(Key.JDBC_URL);

                List<String> tableList = connConf.getList(Key.TABLE, String.class);
                for (String table : tableList) {
                    List<String> renderedPreSqls = WriterUtil.renderPreOrPostSqls(preSqls, table);
                    if (null != renderedPreSqls && !renderedPreSqls.isEmpty()) {
                        Connection conn = DBUtil.getConnection(DATABASE_TYPE, jdbcUrl, username, password);
                        LOG.info("Begin to execute preSqls:[{}]. context info:{}.",
                                StringUtils.join(renderedPreSqls, ";"), jdbcUrl);

                        WriterUtil.executeSqls(conn, renderedPreSqls, jdbcUrl);
                        DBUtil.closeDBResources(null, null, conn);
                    }
                }
            }
            LOG.debug("After job prepare(), originalConfig now is:[\n{}\n]",
                    originalConfig.toJSON());
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            Configuration simplifiedConf = this.originalConfig;

            List<Configuration> splitResultConfigs = new ArrayList<Configuration>();
            for (int j = 0; j < mandatoryNumber; j++) {
                splitResultConfigs.add(simplifiedConf.clone());
            }
            return splitResultConfigs;
        }

        @Override
        public void post() {
            String username = originalConfig.getString(Key.USERNAME);
            String password = originalConfig.getString(Key.PASSWORD);
            List<Object> conns = originalConfig.getList(Constant.CONN_MARK,
                    Object.class);
            List<String> postSqls = originalConfig.getList(Key.POST_SQL,
                    String.class);
            for(Object connConfObject : conns) {
                Configuration connConf = Configuration.from(connConfObject.toString());
                String jdbcUrl = connConf.getString(Key.JDBC_URL);
                List<String> tableList = connConf.getList(Key.TABLE, String.class);

                for (String table : tableList) {
                    List<String> renderedPostSqls = WriterUtil.renderPreOrPostSqls(postSqls, table);
                    if (null != renderedPostSqls && !renderedPostSqls.isEmpty()) {
                        // 说明有 postSql 配置，则此处删除掉
                        Connection conn = DBUtil.getConnection(DATABASE_TYPE, jdbcUrl, username, password);
                        LOG.info("Begin to execute postSqls:[{}]. context info:{}.", StringUtils.join(renderedPostSqls, ";"), jdbcUrl);
                        WriterUtil.executeSqls(conn, renderedPostSqls, jdbcUrl);
                        DBUtil.closeDBResources(null, null, conn);
                    }
                }
            }
            originalConfig.remove(Key.POST_SQL);
        }

        @Override
        public void destroy() {
            this.commonRdbmsWriterJob.destroy(this.originalConfig);
        }

    }

    public static class Task extends Writer.Task {
        private Configuration writerSliceConfig;
        private MysqlRuleCommonRdbmsWriter.Task commonRdbmsWriterTask;

        @Override
        public void init() {
            this.writerSliceConfig = super.getPluginJobConf();
            this.commonRdbmsWriterTask = new MysqlRuleCommonRdbmsWriter.Task(DATABASE_TYPE);
            this.commonRdbmsWriterTask.init(this.writerSliceConfig);
        }

        @Override
        public void prepare() {
            //do nothing
        }

        public void startWrite(RecordReceiver recordReceiver) {
            this.commonRdbmsWriterTask.startWrite(recordReceiver, this.writerSliceConfig, super.getTaskPluginCollector());
        }


        @Override
        public void post() {
            //do nothing
        }

        @Override
        public void destroy() {
            this.commonRdbmsWriterTask.destroy(this.writerSliceConfig);
        }

    }
}
