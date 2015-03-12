package com.alibaba.datax.core.writer.tddlwriter;

import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.datax.plugin.rdbms.writer.util.WriterUtil;
import com.taobao.tddl.client.jdbc.TDataSource;
import com.taobao.tddl.common.exception.TddlException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class TddlWriter extends Writer {
    private static final DataBaseType DATABASE_TYPE = DataBaseType.Tddl;

    public static class Job extends Writer.Job {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originalConfig = null;
        private CommonRdbmsWriter.Job commonRdbmsWriterJob;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();

            this.commonRdbmsWriterJob = new CommonRdbmsWriter.Job(DATABASE_TYPE);
            this.commonRdbmsWriterJob.init(this.originalConfig);
        }

        @Override
        public void prepare() {
            String appName = originalConfig.getString(Key.TDDL_APP_NAME);
            String table = originalConfig.getString(Key.TABLE);

            List<String> preSqls = originalConfig.getList(Key.PRE_SQL, String.class);
            List<String> renderedPreSqls = WriterUtil.renderPreOrPostSqls(preSqls, table);
            if (null != renderedPreSqls && !renderedPreSqls.isEmpty()) {
                // 说明有 preSql 配置，则此处删除掉
                originalConfig.remove(Key.PRE_SQL);

                Connection conn = getTddlConnection(appName);
                LOG.info("Begin to execute preSqls:[{}]. tddlAppName:{}.", StringUtils.join(renderedPreSqls, ";"), appName);

                WriterUtil.executeSqls(conn, renderedPreSqls, appName);
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

        @Override
        public void post() {
            String appName = originalConfig.getString(Key.TDDL_APP_NAME);
            String table = originalConfig.getString(Key.TABLE);
            List<String> postSqls = originalConfig.getList(Key.POST_SQL, String.class);
            List<String> renderedPostSqls = WriterUtil.renderPreOrPostSqls(postSqls, table);

            if (null != renderedPostSqls && !renderedPostSqls.isEmpty()) {
                // 说明有 postSql 配置，则此处删除掉
                originalConfig.remove(Key.POST_SQL);

                Connection conn = getTddlConnection(appName);
                LOG.info("Begin to execute postSqls:[{}]. tddlAppName:{}.", StringUtils.join(renderedPostSqls, ";"), appName);
                WriterUtil.executeSqls(conn, renderedPostSqls, appName);
                DBUtil.closeDBResources(null, null, conn);
            }
        }

        @Override
        public void destroy() {
        }

    }

    public static class Task extends Writer.Task {

        private Configuration writerSliceConfig;
        private CommonRdbmsWriter.Task commonRdbmsWriterTask;
        private String tddlAppName;

        @Override
        public void init() {
            this.writerSliceConfig = super.getPluginJobConf();
            this.commonRdbmsWriterTask = new CommonRdbmsWriter.Task(DATABASE_TYPE);
            this.commonRdbmsWriterTask.init(this.writerSliceConfig);
            this.tddlAppName = writerSliceConfig.getString("appName");
        }

        @Override
        public void prepare() {
        }

        public void startWrite(RecordReceiver recordReceiver) {
            Connection conn = getTddlConnection(tddlAppName);
            this.commonRdbmsWriterTask.startWriteWithConnection(recordReceiver, super.getTaskPluginCollector(), conn);
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }

    }

    public static Connection getTddlConnection(String appName) {
        try {
            TDataSource ds = new TDataSource();
            ds.setAppName(appName);
            ds.setDynamicRule(true);
            ds.init();
            return ds.getConnection();
        } catch (TddlException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

}
