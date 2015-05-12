package com.alibaba.datax.core.writer.tddlwriter;

import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.datax.plugin.rdbms.writer.util.OriginalConfPretreatmentUtil;
import com.alibaba.datax.plugin.rdbms.writer.util.WriterUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public class TddlWriter extends Writer {
    private static final DataBaseType DATABASE_TYPE = DataBaseType.Tddl;
    private static TddlConnectionFactory tddlConnectionFactory = new TddlConnectionFactory();

    public static class Job extends Writer.Job {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originalConfig = null;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            //check appName,table
            originalConfig.getNecessaryValue(Key.TDDL_APP_NAME, DBUtilErrorCode.REQUIRED_VALUE);
            originalConfig.getNecessaryValue(Key.TABLE, DBUtilErrorCode.REQUIRED_VALUE);

            // init tddlConnctionFactory
            String appName = originalConfig.getString(Key.TDDL_APP_NAME);
            tddlConnectionFactory.initAppName(appName);
            //check batch size
            OriginalConfPretreatmentUtil.doCheckBatchSize(originalConfig);
            //deal config
            String table = originalConfig.getString(Key.TABLE);
            OriginalConfPretreatmentUtil.dealColumnConf(originalConfig, tddlConnectionFactory, table);
            OriginalConfPretreatmentUtil.dealWriteMode(originalConfig);

            LOG.debug("After job init(), originalConfig now is:[\n{}\n]", originalConfig.toJSON());
        }

        /**
         * execute pre sqls
         */
        @Override
        public void prepare() {
            String table = originalConfig.getString(Key.TABLE);
            List<String> preSqls = originalConfig.getList(Key.PRE_SQL, String.class);
            List<String> renderedPreSqls = WriterUtil.renderPreOrPostSqls(preSqls, table);
            if (null != renderedPreSqls && !renderedPreSqls.isEmpty()) {
                // 说明有 preSql 配置，则此处删除掉
                originalConfig.remove(Key.PRE_SQL);
                Connection conn = tddlConnectionFactory.getConnecttion();
                LOG.info("Begin to execute preSqls:[{}]. context info:{}.", StringUtils.join(renderedPreSqls, ";"), tddlConnectionFactory.getConnectionInfo());
                WriterUtil.executeSqls(conn, renderedPreSqls, tddlConnectionFactory.getConnectionInfo());
                DBUtil.closeDBResources(null, null, conn);
            }
            LOG.debug("After job prepare(), originalConfig now is:[\n{}\n]", originalConfig.toJSON());
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

        /**
         * execute post sqls
         */
        @Override
        public void post() {
            String table = originalConfig.getString(Key.TABLE);
            List<String> postSqls = originalConfig.getList(Key.POST_SQL, String.class);
            List<String> renderedPostSqls = WriterUtil.renderPreOrPostSqls(postSqls, table);
            if (null != renderedPostSqls && !renderedPostSqls.isEmpty()) {
                // 说明有 postSql 配置，则此处删除掉
                originalConfig.remove(Key.POST_SQL);
                Connection conn = tddlConnectionFactory.getConnecttion();
                LOG.info("Begin to execute postSqls:[{}]. context info:{}.",
                        StringUtils.join(renderedPostSqls, ";"), tddlConnectionFactory.getConnectionInfo());
                WriterUtil.executeSqls(conn, renderedPostSqls, tddlConnectionFactory.getConnectionInfo());
                DBUtil.closeDBResources(null, null, conn);
            }
        }

        @Override
        public void destroy() {
        }

    }

    public static class Task extends Writer.Task {

        private Configuration writerSliceConfig;
        private TddlCommonRdbmsWriter.Task commonRdbmsWriterTask;

        @Override
        public void init() {
            this.writerSliceConfig = super.getPluginJobConf();
            //检查appName为必选项
            writerSliceConfig.getNecessaryValue(Key.TDDL_APP_NAME, DBUtilErrorCode.REQUIRED_VALUE);
            String appName = writerSliceConfig.getString(Key.TDDL_APP_NAME);
            tddlConnectionFactory.initAppName(appName);

            this.commonRdbmsWriterTask = new TddlCommonRdbmsWriter.Task(DATABASE_TYPE);
            this.commonRdbmsWriterTask.init(this.writerSliceConfig);
        }

        @Override
        public void prepare() {
        }

        public void startWrite(RecordReceiver recordReceiver) {
            Connection conn = tddlConnectionFactory.getConnecttion();
            this.commonRdbmsWriterTask.startWriteWithConnection(recordReceiver, super.getTaskPluginCollector(), conn);
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }

    }

}
