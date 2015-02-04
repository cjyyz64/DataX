package com.alibaba.datax.plugin.reader.oraclereader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class OracleReader extends Reader {

	private static final DataBaseType DATABASE_TYPE = DataBaseType.Oracle;

	public static class Job extends Reader.Job {
		private static final Logger LOG = LoggerFactory
				.getLogger(OracleReader.Job.class);

		private Configuration originalConfig = null;
		private CommonRdbmsReader.Job commonRdbmsReaderJob;

		@Override
		public void init() {
			this.originalConfig = super.getPluginJobConf();
			
			dealFetchSize(this.originalConfig);

			this.commonRdbmsReaderJob = new CommonRdbmsReader.Job(
					DATABASE_TYPE);
			this.commonRdbmsReaderJob.init(this.originalConfig);

			// 注意：要在 this.commonRdbmsReaderJob.init(this.originalConfig); 之后执行，这样可以直接快速判断是否是querySql 模式
			dealHint(this.originalConfig);
		}

		@Override
		public List<Configuration> split(int adviceNumber) {
			return this.commonRdbmsReaderJob.split(this.originalConfig,
					adviceNumber);
		}

		@Override
		public void post() {
			this.commonRdbmsReaderJob.post(this.originalConfig);
		}

		@Override
		public void destroy() {
			this.commonRdbmsReaderJob.destroy(this.originalConfig);
		}

		private void dealFetchSize(Configuration originalConfig) {
			int fetchSize = originalConfig.getInt(
					com.alibaba.datax.plugin.rdbms.reader.Constant.FETCH_SIZE,
					Constant.DEFAULT_FETCH_SIZE);
			if (fetchSize < 1) {
				throw DataXException
						.asDataXException(DBUtilErrorCode.REQUIRED_VALUE,
								String.format("您配置的 fetchSize 有误，fetchSize:[%d] 值不能小于 1.",
										fetchSize));
			}
			originalConfig.set(
					com.alibaba.datax.plugin.rdbms.reader.Constant.FETCH_SIZE,
					fetchSize);
		}

		private void dealHint(Configuration originalConfig) {
			String hint = originalConfig.getString(Key.HINT);
			if (StringUtils.isNotBlank(hint)) {
				boolean isTableMode = originalConfig.getBool(com.alibaba.datax.plugin.rdbms.reader.Constant.IS_TABLE_MODE).booleanValue();
				List<Object> conns = originalConfig.getList(com.alibaba.datax.plugin.rdbms.reader.Constant.CONN_MARK, Object.class);

				Configuration connConf = Configuration.from(conns.get(0).toString());

				List<String> tables = connConf.getList(com.alibaba.datax.plugin.rdbms.reader.Key.TABLE, String.class);
				if (isTableMode && conns.size() == 1 && tables.size() == 1) {
					// 小心，需要对 originalConfig 进行操作，以改写其 column，而不是对 originalConfig 进行操作
					String column = originalConfig.getString(com.alibaba.datax.plugin.rdbms.reader.Key.COLUMN);
					originalConfig.set(com.alibaba.datax.plugin.rdbms.reader.Key.COLUMN, hint + column);
					LOG.info("Use hint:{}.", hint);
				} else {
					throw DataXException.asDataXException(OracleReaderErrorCode.HINT_ERROR, "当且仅当非 querySql 模式读取 oracle 单表时才能配置 HINT.");
				}
			}
		}

	}

	public static class Task extends Reader.Task {

		private Configuration readerSliceConfig;
		private CommonRdbmsReader.Task commonRdbmsReaderTask;

		@Override
		public void init() {
			this.readerSliceConfig = super.getPluginJobConf();
			this.commonRdbmsReaderTask = new CommonRdbmsReader.Task(
					DATABASE_TYPE);
			this.commonRdbmsReaderTask.init(this.readerSliceConfig);
		}

		@Override
		public void startRead(RecordSender recordSender) {
			int fetchSize = this.readerSliceConfig
					.getInt(com.alibaba.datax.plugin.rdbms.reader.Constant.FETCH_SIZE);

			this.commonRdbmsReaderTask.startRead(this.readerSliceConfig,
					recordSender, super.getTaskPluginCollector(), fetchSize);
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
