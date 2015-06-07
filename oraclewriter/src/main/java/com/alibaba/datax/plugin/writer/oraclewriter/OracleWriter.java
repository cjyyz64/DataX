package com.alibaba.datax.plugin.writer.oraclewriter;

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

import java.util.List;

public class OracleWriter extends Writer {
	private static final DataBaseType DATABASE_TYPE = DataBaseType.Oracle;

	public static class Job extends Writer.Job {
		private Configuration originalConfig = null;
		private CommonRdbmsWriter.Job commonRdbmsWriterJob;

        public void preCheck() {
            this.init();
            WriterUtil.preCheckPrePareOrPostSQL(originalConfig, DATABASE_TYPE);

            String username = this.originalConfig.getString(Key.USERNAME);
            String password = this.originalConfig.getString(Key.PASSWORD);
            List<Object> connections = originalConfig.getList(Constant.CONN_MARK,
                    Object.class);

            for (int i = 0, len = connections.size(); i < len; i++) {
                Configuration connConf = Configuration.from(connections.get(i).toString());
                String jdbcUrl = connConf.getString(Key.JDBC_URL);
                List<String> expandedTables = connConf.getList(Key.TABLE, String.class);
                boolean hasInsertPri = DBUtil.hasOracleInsertPrivilege(DATABASE_TYPE, jdbcUrl, username, password, expandedTables);

                if(!hasInsertPri){
                    throw DataXException.asDataXException(DBUtilErrorCode.NO_INSERT_PRIVILEGE, originalConfig.getString(Key.USERNAME) + jdbcUrl);
                }
            }
        }

		@Override
		public void init() {
			this.originalConfig = super.getPluginJobConf();

			// warn：not like mysql, oracle only support insert mode, don't use
			String writeMode = this.originalConfig.getString(Key.WRITE_MODE);
			if (null != writeMode) {
				throw DataXException
						.asDataXException(
								DBUtilErrorCode.CONF_ERROR,
								String.format(
										"写入模式(writeMode)配置错误. 因为Oracle不支持配置项 writeMode: %s, Oracle只能使用insert sql 插入数据. 请检查您的配置并作出修改",
										writeMode));
			}

			this.commonRdbmsWriterJob = new CommonRdbmsWriter.Job(
					DATABASE_TYPE);
			this.commonRdbmsWriterJob.init(this.originalConfig);
		}

		@Override
		public void prepare() {
			this.commonRdbmsWriterJob.prepare(this.originalConfig);
		}

		@Override
		public List<Configuration> split(int mandatoryNumber) {
			return this.commonRdbmsWriterJob.split(this.originalConfig,
					mandatoryNumber);
		}

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

		public void startWrite(RecordReceiver recordReceiver) {
			this.commonRdbmsWriterTask.startWrite(recordReceiver,
					this.writerSliceConfig, super.getTaskPluginCollector());
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
