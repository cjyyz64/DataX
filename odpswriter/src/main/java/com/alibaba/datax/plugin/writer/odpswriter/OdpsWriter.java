package com.alibaba.datax.plugin.writer.odpswriter;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.FilterUtil;
import com.alibaba.datax.plugin.writer.odpswriter.util.OdpsSplitUtil;
import com.alibaba.datax.plugin.writer.odpswriter.util.OdpsUtil;
import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.Table;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;

public class OdpsWriter extends Writer {
	public static class Master extends Writer.Master {
		private static final Logger LOG = LoggerFactory
				.getLogger(OdpsWriter.Master.class);

		private static boolean IS_DEBUG = LOG.isDebugEnabled();

		private Configuration originalConfig;

		private Table table;

		@Override
		public void init() {
			this.originalConfig = getPluginJobConf();
			dealMaxRetryTime(this.originalConfig);

			Odps odps = OdpsUtil.initOdps(this.originalConfig);

			String tableName = this.originalConfig.getNecessaryValue(Key.TABLE,
					OdpsWriterErrorCode.NOT_SUPPORT_TYPE);

			try {
				this.table = OdpsUtil.getTable(odps, tableName);
				this.originalConfig.set(Constant.IS_PARTITIONED_TABLE,
						OdpsUtil.isPartitionedTable(table));
			} catch (Exception e) {
				throw new DataXException(OdpsWriterErrorCode.RUNTIME_EXCEPTION,
						e);
			}

			boolean isVirtualView = table.isVirtualView();
			if (isVirtualView) {
				throw new DataXException(
						OdpsWriterErrorCode.NOT_SUPPORT_TYPE,
						String.format(
								"Table:[%s] is Virtual View, DataX not support to read data from it.",
								tableName));
			}

			dealPartition(this.originalConfig, table);

			// column 可能不需要用户配置以及顺序调整
			// dealColumn(originalConfig, table);
		}

		@Override
		public void prepare() {
			LOG.info("begin prepare()");
			boolean truncate = this.originalConfig.getBool(Key.TRUNCATE, true);

			boolean isPartitionedTable = this.originalConfig
					.getBool(Constant.IS_PARTITIONED_TABLE);
			if (truncate) {
				if (isPartitionedTable) {
					String partition = this.originalConfig
							.getString(Key.PARTITION);
					LOG.info(
							"try to clean partitioned table:[{}], partition:[{}].",
							this.table.getName(), partition);
					OdpsUtil.truncatePartition(this.table, partition);
				} else {
					LOG.info("try to clean non partitioned table:[{}].",
							this.table.getName());

					// table.truncate();
				}
			}
			LOG.info("end prepare()");
		}

		@Override
		public List<Configuration> split(int mandatoryNumber) {
			return OdpsSplitUtil.doSplit(this.originalConfig, mandatoryNumber);
		};

		@Override
		public void post() {
			LOG.info("post()");
		}

		@Override
		public void destroy() {
			LOG.info("destroy()");
		}

		private void dealMaxRetryTime(Configuration originalConfig) {
			int maxRetryTime = originalConfig.getInt(Key.MAX_RETRY_TIME, 3);
			if (maxRetryTime < 1) {
				throw new DataXException(OdpsWriterErrorCode.NOT_SUPPORT_TYPE,
						"maxRetryTime should >=1.");
			}
			this.originalConfig.set(Key.MAX_RETRY_TIME, maxRetryTime);
		}

		/**
		 * 对分区的配置处理。如果是分区表，则必须配置一个叶子分区。如果是非分区表，则不允许配置分区。
		 */
		private void dealPartition(Configuration originalConfig, Table table) {
			String userConfigedPartition = originalConfig
					.getString(Key.PARTITION);

			boolean isPartitionedTable = originalConfig
					.getBool(Constant.IS_PARTITIONED_TABLE);

			if (isPartitionedTable) {
				// 分区表，需要配置分区
				if (StringUtils.isBlank(userConfigedPartition)) {
					throw new DataXException(
							OdpsWriterErrorCode.NOT_SUPPORT_TYPE,
							String.format(
									"Lost partition config, table:[%s] is partitioned.",
									table.getName()));
				} else {
					List<String> allPartitions = OdpsUtil
							.getTableAllPartitions(table,
									originalConfig.getInt(Key.MAX_RETRY_TIME));

					List<String> retPartitions = checkUserConfigedPartition(
							allPartitions, userConfigedPartition);

					if (null == retPartitions || retPartitions.isEmpty()) {
						throw new DataXException(
								OdpsWriterErrorCode.NOT_SUPPORT_TYPE,
								String.format(
										"illegal partition:[%s], can not found it in all partition:[\n%s\n].",
										userConfigedPartition,
										StringUtils.join(allPartitions, "\n")));
					} else if (retPartitions.size() > 1) {
						throw new DataXException(
								OdpsWriterErrorCode.NOT_SUPPORT_TYPE,
								String.format(
										"illegal partition:[%s], more than one partitions in all partition:[\n%s\n].",
										userConfigedPartition,
										StringUtils.join(allPartitions, "\n")));
					}
					originalConfig.set(Key.PARTITION, retPartitions);
				}
			} else {
				// 非分区表，则不能配置分区
				if (null != userConfigedPartition
						&& !userConfigedPartition.isEmpty()) {
					throw new DataXException(
							OdpsWriterErrorCode.NOT_SUPPORT_TYPE,
							String.format(
									"Can not config partition, Table:[%s] is not partitioned, ",
									table.getName()));
				}
			}

		}

		private List<String> checkUserConfigedPartition(
				List<String> allPartitions, String userConfigedPartition) {
			// 对odps 本身的所有分区进行特殊字符的处理
			List<String> allStandardPartitions = OdpsUtil
					.formatPartitions(allPartitions);

			// 对用户自身配置的所有分区进行特殊字符的处理
			String standardUserConfigedPartitions = OdpsUtil
					.formatPartition(userConfigedPartition);

			if (standardUserConfigedPartitions.indexOf("*") >= 0) {
				// 不允许
				throw new DataXException(OdpsWriterErrorCode.NOT_SUPPORT_TYPE,
						" partition can not configed as *。.");
			}

			List<String> retPartitions = FilterUtil.filterByRegular(
					allStandardPartitions, standardUserConfigedPartitions);

			return retPartitions;
		}

		private void dealColumn(Configuration originalConfig, Table t) {
			// 用户配置的 column
			List<String> userConfigedColumns = this.originalConfig.getList(
					Key.COLUMN, String.class);

			List<Column> allColumns = OdpsUtil.getTableAllColumns(t);
			List<String> tableOriginalColumnNameList = OdpsUtil
					.getTableOriginalColumnNameList(allColumns);

			if (IS_DEBUG) {
				LOG.debug("Table:[{}] all columns:[{}]", t.getName(),
						StringUtils.join(tableOriginalColumnNameList, ","));
			}

			if (null == userConfigedColumns || userConfigedColumns.isEmpty()) {
				LOG.warn("column blank is not recommended.");
				originalConfig.set(Key.COLUMN, tableOriginalColumnNameList);
			} else if (1 == userConfigedColumns.size()
					&& "*".equals(userConfigedColumns.get(0))) {
				LOG.warn("column * is not recommended.");
				originalConfig.set(Key.COLUMN, tableOriginalColumnNameList);
			}

		}

	}

	public static class Slave extends Writer.Slave {
		private static final Logger LOG = LoggerFactory
				.getLogger(OdpsWriter.Slave.class);
		private Configuration writerSliceConf;

		private String tunnelServer;
		private String table = null;

		private Odps odps = null;
		private boolean isPartitionedTable;

		@Override
		public void init() {
			this.writerSliceConf = getPluginJobConf();
			this.tunnelServer = this.writerSliceConf.getString(
					Key.TUNNEL_SERVER, null);

			this.table = this.writerSliceConf.getString(Key.TABLE);

			this.odps = OdpsUtil.initOdps(this.writerSliceConf);
			this.isPartitionedTable = this.writerSliceConf
					.getBool(Constant.IS_PARTITIONED_TABLE);
		}

		@Override
		public void prepare() {
			LOG.info("prepare()");
		}

		// ref:http://odps.alibaba-inc.com/doc/prddoc/odps_tunnel/odps_tunnel_examples.html#id4
		@Override
		public void startWrite(RecordReceiver recordReceiver) {
			UploadSession uploadSession = null;
			String partition = this.writerSliceConf.getString(Key.PARTITION);

			if (this.isPartitionedTable) {
				try {
					uploadSession = OdpsSplitUtil
							.getSessionForPartitionedTable(this.odps,
									this.tunnelServer, this.table, partition);

					LOG.info("Session status:[{}]", uploadSession.getStatus()
							.toString());
				} catch (Exception e) {
					throw new DataXException(
							OdpsWriterErrorCode.NOT_SUPPORT_TYPE, e);
				}
			} else {
				uploadSession = OdpsSplitUtil.getSessionForNonPartitionedTable(
						this.odps, this.tunnelServer, this.table);
			}

			long blockId = 1L;
			try {

				WriterProxy writerProxy = new WriterProxy(recordReceiver,
						uploadSession.getSchema(), null, uploadSession);

				writerProxy.doWrite();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		@Override
		public void post() {
			LOG.info("post()");
		}

		@Override
		public void destroy() {
			LOG.info("destroy()");
		}

	}
}
