package com.alibaba.datax.core.writer.tddlwriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.ListUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
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
    private static final Logger LOG = LoggerFactory.getLogger(TddlWriter.class);
    private static final DataBaseType DATABASE_TYPE = DataBaseType.Tddl;
    private static String APP_NAME;

    public static class Job extends Writer.Job {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originalConfig = null;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            originalConfig.getNecessaryValue(Key.TDDL_APP_NAME, DBUtilErrorCode.REQUIRED_VALUE);

            // 检查batchSize 配置（选填，如果未填写，则设置为默认值）
            int batchSize = originalConfig.getInt(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_SIZE);
            if (batchSize < 1) {
                throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE, String.format(
                        "您的batchSize配置有误. 您所配置的写入数据库表的 batchSize:%s 不能小于1. 推荐配置范围为：[100-1000], 该值越大, 内存溢出可能性越大. 请检查您的配置并作出修改.",
                        batchSize));
            }
            originalConfig.set(Key.BATCH_SIZE, batchSize);

            APP_NAME = originalConfig.getString(Key.TDDL_APP_NAME);
            Connection conn = getTddlConnection(APP_NAME);
            String table = originalConfig.getString(Key.TABLE);
            dealColumnConf(originalConfig, conn, table);
            dealWriteMode(originalConfig);
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
            this.tddlAppName = writerSliceConfig.getString(Key.TDDL_APP_NAME);
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

    public static void dealWriteMode(Configuration originalConfig) {
        List<String> columns = originalConfig.getList(Key.COLUMN, String.class);

        // 默认为：insert 方式
        String writeMode = originalConfig.getString(Key.WRITE_MODE, "INSERT");

        List<String> valueHolders = new ArrayList<String>(columns.size());
        for(int i=0; i<columns.size(); i++){
            valueHolders.add("?");
        }

        String writeDataSqlTemplate = WriterUtil.getWriteTemplate(columns, valueHolders, writeMode);

        LOG.info("Write data [\n{}\n], which tddl appName like:[{}]", writeDataSqlTemplate, APP_NAME);

        originalConfig.set(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK, writeDataSqlTemplate);
    }


    public static void dealColumnConf(Configuration originalConfig, Connection conn, String oneTable) {
        List<String> userConfiguredColumns = originalConfig.getList(Key.COLUMN, String.class);
        if (null == userConfiguredColumns || userConfiguredColumns.isEmpty()) {
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
                    "您的配置文件中的列配置信息有误. 因为您未配置写入数据库表的列名称，DataX获取不到列信息. 请检查您的配置并作出修改.");
        } else {

            List<String> allColumns = DBUtil.getTableColumnsByConn(conn, oneTable);

            LOG.info("table:[{}] all columns:[\n{}\n].", oneTable,
                    StringUtils.join(allColumns, ","));

            if (1 == userConfiguredColumns.size() && "*".equals(userConfiguredColumns.get(0))) {
                LOG.warn("您的配置文件中的列配置信息存在风险. 因为您配置的写入数据库表的列为*，当您的表字段个数、类型有变动时，可能影响任务正确性甚至会运行出错。请检查您的配置并作出修改.");

                // 回填其值，需要以 String 的方式转交后续处理
                originalConfig.set(Key.COLUMN, allColumns);
            } else if (userConfiguredColumns.size() > allColumns.size()) {
                throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
                        String.format("您的配置文件中的列配置信息有误. 因为您所配置的写入数据库表的字段个数:%s 大于目的表的总字段总个数:%s. 请检查您的配置并作出修改.",
                                userConfiguredColumns.size(), allColumns.size()));
            } else {
                // 确保用户配置的 column 不重复
                ListUtil.makeSureNoValueDuplicate(userConfiguredColumns, false);

                conn = getTddlConnection(APP_NAME);
                // 检查列是否都为数据库表中正确的列（通过执行一次 select column from table 进行判断）
                DBUtil.getColumnMetaData(conn, oneTable,StringUtils.join(userConfiguredColumns, ","));
            }
        }
    }

}
