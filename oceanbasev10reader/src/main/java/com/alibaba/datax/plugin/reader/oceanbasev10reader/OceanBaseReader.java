package com.alibaba.datax.plugin.reader.oceanbasev10reader;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.ext.ReaderJob;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.ext.ReaderTask;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.util.OBUtils;

public class OceanBaseReader extends Reader {

	public static class Job extends Reader.Job {
		private static final Logger LOG = LoggerFactory.getLogger(Job.class);

		private Configuration originalConfig = null;
		private ReaderJob readerJob;

		@Override
		public void init() {
			this.originalConfig = super.getPluginJobConf();

			Integer userConfigedFetchSize = this.originalConfig.getInt(Constant.FETCH_SIZE);
			if (userConfigedFetchSize != null) {
				LOG.warn("对 OceanBaseReader 不需要配置 fetchSize, mysqlreader 将会忽略这项配置. 如果您不想再看到此警告,请去除fetchSize 配置.");
			}

			this.originalConfig.set(Constant.FETCH_SIZE, Integer.MIN_VALUE);

			this.readerJob = new ReaderJob();
			this.readerJob.init(this.originalConfig);
		}

		@Override
		public void preCheck() {
			init();
			this.readerJob.preCheck(this.originalConfig, OBUtils.DATABASE_TYPE);

		}

		@Override
		public List<Configuration> split(int adviceNumber) {
			return this.readerJob.split(this.originalConfig, adviceNumber);
		}

		@Override
		public void post() {
			this.readerJob.post(this.originalConfig);
		}

		@Override
		public void destroy() {
			this.readerJob.destroy(this.originalConfig);
		}

	}

	public static class Task extends Reader.Task {
		private Configuration readerSliceConfig;
		private ReaderTask commonRdbmsReaderTask;

		@Override
		public void init() {
			this.readerSliceConfig = super.getPluginJobConf();
			this.commonRdbmsReaderTask = new ReaderTask(super.getTaskGroupId(), super.getTaskId());
			this.commonRdbmsReaderTask.init(this.readerSliceConfig);

		}

		@Override
		public void startRead(RecordSender recordSender) {
			int fetchSize = this.readerSliceConfig.getInt(Constant.FETCH_SIZE);
			this.commonRdbmsReaderTask.startRead(this.readerSliceConfig, recordSender, super.getTaskPluginCollector(),
					fetchSize);
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
