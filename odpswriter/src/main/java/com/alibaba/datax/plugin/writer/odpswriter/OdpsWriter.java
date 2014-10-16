package com.alibaba.datax.plugin.writer.odpswriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.StrUtil;
import com.alibaba.odps.tunnel.Column;
import com.alibaba.odps.tunnel.DataTunnel;
import com.alibaba.odps.tunnel.RecordSchema;
import com.alibaba.odps.tunnel.Upload;
import com.alibaba.odps.tunnel.io.ProtobufRecordWriter;
import com.alibaba.odps.tunnel.io.Record;
import com.aliyun.openservices.odps.Project;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

//TODO: 换行符：System.getProperties("line.separator")方式获取
public class OdpsWriter extends Writer {

	public static class Master extends Writer.Master {
		private static final Logger LOG = LoggerFactory
				.getLogger(OdpsWriter.Master.class);

		private static final boolean IS_DEBUG = LOG.isDebugEnabled();

		private Configuration originalConfig;

		private String project;

		private String table;

		private String partition;

		private String accountType;

		private boolean truncate;

		private DataTunnel dataTunnel;

		private Project odpsProject;

		private String uploadId;

		private Upload masterUpload;

		@Override
		public void init() {
			this.originalConfig = super.getPluginJobConf();

			checkNecessaryConfig(this.originalConfig);

			this.project = this.originalConfig.getString(Key.PROJECT);
			this.table = this.originalConfig.getString(Key.TABLE);

			this.partition = OdpsUtil.formatPartition(this.originalConfig
					.getString(Key.PARTITION, ""));
			this.originalConfig.set(Key.PARTITION, this.partition);

			this.accountType = this.originalConfig.getString(Key.ACCOUNT_TYPE,
					Constant.DEFAULT_ACCOUNT_TYPE);
			this.originalConfig.set(Key.ACCOUNT_TYPE,
					Constant.DEFAULT_ACCOUNT_TYPE);

			this.truncate = this.originalConfig.getBool(Key.TRUNCATE);

			if (IS_DEBUG) {
				LOG.debug("after init, job config: [\n{}\n] .",
						this.originalConfig.toJSON());
			}
		}

		private void checkNecessaryConfig(Configuration originalConfig) {
			originalConfig.getNecessaryValue(Key.ODPS_SERVER,
					OdpsWriterErrorCode.REQUIRED_VALUE);
			originalConfig.getNecessaryValue(Key.TUNNEL_SERVER,
					OdpsWriterErrorCode.REQUIRED_VALUE);

			originalConfig.getNecessaryValue(Key.PROJECT,
					OdpsWriterErrorCode.REQUIRED_VALUE);
			originalConfig.getNecessaryValue(Key.TABLE,
					OdpsWriterErrorCode.REQUIRED_VALUE);

			// getBool 内部要求，值只能为 true,false 的字符串（大小写不敏感），其他一律报错，不再有默认配置
			originalConfig.getBool(Key.TRUNCATE);
		}

		@Override
		public void prepare() {
			// TODO 更正其行为。以前是优先环境变量获取 id,key 。现在需要校正为：优先使用用户配置的 id,key
			String accessId = null;
			String accessKey = null;

			if (Constant.DEFAULT_ACCOUNT_TYPE
					.equalsIgnoreCase(this.accountType)) {
				this.originalConfig = SecurityChecker
						.parseAccessIdAndKey(this.originalConfig);
				accessId = this.originalConfig.getString(Key.ACCESS_ID);
				accessKey = this.originalConfig.getString(Key.ACCESS_KEY);
				if (IS_DEBUG) {
					LOG.debug("accessId:[{}], accessKey:[{}] .", accessId,
							accessKey);
				}

				LOG.info("accessId:[{}] .", accessId);
			}

			// init dataTunnel config
			this.dataTunnel = OdpsUtil.initDataTunnel(this.originalConfig);

			// init odps config
			this.odpsProject = OdpsUtil.initOpdsProject(this.originalConfig);

			if (this.truncate) {
				LOG.info("Try to clean {}:{} .", this.table, this.partition);
				if (StringUtils.isBlank(this.partition)) {
					LOG.info("Try to clean {}.", this.table);
					OdpsUtil.truncateTable(this.originalConfig,
							this.odpsProject);
				} else {
					LOG.info("Try to clean partition:[{}] in table:[{}].",
							this.partition, this.table);
					OdpsUtil.truncatePart(this.odpsProject, this.table,
							this.partition);
				}
			} else {
				// add part if not exists
				if (StringUtils.isNotBlank(this.partition)
						&& !OdpsUtil.isPartExists(this.odpsProject, this.table,
								this.partition)) {
					LOG.info("Try to add partition:[{}] in table:[{}].",
							this.partition, this.table);
					OdpsUtil.addPart(this.odpsProject, this.table,
							this.partition);
				}
			}
		}

		/**
		 * 此处主要是对 uploadId进行设置，以及对 blockId 的开始值进行设置。
		 * <p/>
		 * 对 blockId 需要同时设置开始值以及下一个 blockId 的步长值(INTERVAL_STEP)。
		 */
		@Override
		public List<Configuration> split(int mandatoryNumber) {
			List<Configuration> splittedConfigs = new ArrayList<Configuration>();

			this.masterUpload = OdpsUtil.createMasterTunnelUpload(
					this.dataTunnel, this.project, this.table, this.partition);
			this.uploadId = this.masterUpload.getUploadId();
			LOG.info("uploadId:[{}].", this.uploadId);

			for (int i = 0; i < mandatoryNumber; i++) {
				Configuration tempConfig = this.originalConfig.clone();

				tempConfig.set(Constant.UPLOAD_ID, this.uploadId);
				tempConfig.set(Constant.BLOCK_ID, String.valueOf(i));
				tempConfig.set(Constant.INTERVAL_STEP, mandatoryNumber);

				splittedConfigs.add(tempConfig);
			}
			return splittedConfigs;
		}

		@Override
		public void post() {
			List<Long> blocks = new ArrayList<Long>();
			List<String> salveWroteBlockIds = super.getMasterPluginCollector()
					.getMessage(Constant.SLAVE_WROTE_BLOCK_MESSAGE);

			for (String context : salveWroteBlockIds) {

				String[] blockIdStrs = context.split(",");
				for (int i = 0, len = blockIdStrs.length; i < len; i++) {
					blocks.add(Long.parseLong(blockIdStrs[i]));
				}
			}

			int count = 0;
			while (true) {
				count++;
				try {
					this.masterUpload.complete(blocks.toArray(new Long[0]));
					break;
				} catch (Exception e) {
					if (count > Constant.MAX_RETRY_TIME)
						throw new DataXException(null,
								"Error when complete upload." + e);
					else {
						try {
							Thread.sleep(2 * count * 1000);
						} catch (InterruptedException e1) {
						}
						continue;
					}
				}
			}
		}

		@Override
		public void destroy() {
			LOG.info("destroy()");
		}

		private void dealMaxRetryTime(Configuration originalConfig) {
			int maxRetryTime = originalConfig.getInt(Key.MAX_RETRY_TIME,
					Constant.MAX_RETRY_TIME);
			if (maxRetryTime < 1) {
				String bussinessMessage = "maxRetryTime should >=1.";
				String message = StrUtil.buildOriginalCauseMessage(
						bussinessMessage, null);

				LOG.error(message);
				throw new DataXException(OdpsWriterErrorCode.ILLEGAL_VALUE,
						bussinessMessage);
			}

			originalConfig.set(Key.MAX_RETRY_TIME, maxRetryTime);
		}

	}

	// TODO 重构：odpswriterProxy 进行写入的操作。
	public static class Slave extends Writer.Slave {
		private static final Logger LOG = LoggerFactory
				.getLogger(OdpsWriter.Slave.class);

		private static final boolean IS_DEBUG = LOG.isDebugEnabled();

		private Configuration sliceConfig;

		private DataTunnel dataTunnel;

		private RecordSchema schema;

		private String project;

		private String table;

		private String partition;

		private Upload slaveUpload;

		private ByteArrayOutputStream out = new ByteArrayOutputStream(
				70 * 1024 * 1024);

		private ProtobufRecordWriter pwriter = null;

		@Override
		public void init() {
			this.sliceConfig = super.getPluginJobConf();

			this.project = this.sliceConfig.getString(Key.PROJECT);
			this.table = this.sliceConfig.getString(Key.TABLE);
			this.partition = OdpsUtil.formatPartition(this.sliceConfig
					.getString(Key.PARTITION, ""));

			this.dataTunnel = OdpsUtil.initDataTunnel(this.sliceConfig);

			String uploadId = this.sliceConfig.getString(Constant.UPLOAD_ID);
			this.slaveUpload = OdpsUtil.getSlaveTunnelUpload(this.dataTunnel,
					this.project, this.table, this.partition, uploadId);
			this.schema = this.slaveUpload.getSchema();
		}

		@Override
		public void prepare() {
			LOG.info("prepare()");
		}

		private int max_buffer_length = 64 * 1024 * 1024;

		@Override
		public void startWrite(RecordReceiver recordReceiver) {
			// blockId 的开始值，master在 split 的时候已经准备好
			long blockId = this.sliceConfig.getLong(Constant.BLOCK_ID);
			List<Long> blocks = new ArrayList<Long>();
			com.alibaba.datax.common.element.Record dataxRecord = null;

			try {
				pwriter = new ProtobufRecordWriter(schema, out);
				while ((dataxRecord = recordReceiver.getFromReader()) != null) {
					Record r = line2Record(dataxRecord, schema);
					if (null != r) {
						pwriter.write(r);
					}

					if (out.size() >= max_buffer_length) {
						pwriter.close();
						writeBlock(this.slaveUpload, blockId);
						blocks.add(blockId);
						out.reset();
						pwriter = new ProtobufRecordWriter(schema, out);

						blockId += this.sliceConfig
								.getInt(Constant.INTERVAL_STEP);
					}
				}
				// complete protobuf stream, then write to http
				pwriter.close();
				if (out.size() != 0) {
					writeBlock(this.slaveUpload, blockId);
					blocks.add(blockId);
					// reset the buffer for next block
					out.reset();
				}

				super.getSlavePluginCollector().collectMessage(
						Constant.SLAVE_WROTE_BLOCK_MESSAGE,
						StringUtils.join(blockId, ","));
			} catch (Exception e) {
				throw new DataXException(null,
						"Error when upload data to odps dataTunnel" + e);
			}
		}

		private void writeBlock(Upload slaveUpload, long blockId) {
			int count = 0;
			while (true) {
				count++;
				try {
					OutputStream hpout = slaveUpload.openOutputStream(blockId);
					LOG.info("Current buf size: {} .", out.size());
					LOG.info("Start to write block {}, UploadId is {}.",
							blockId);
					out.writeTo(hpout);
					hpout.close();
					LOG.info("Write block [{}] OK .", blockId);
					break;
				} catch (Exception e) {
					if (count > Constant.MAX_RETRY_TIME)
						throw new DataXException(null,
								"Error when upload data to odps dataTunnel" + e);
					else {
						try {
							Thread.sleep(2 * count * 1000);
						} catch (InterruptedException e1) {
						}
						continue;
					}
				}
			}
		}

		private volatile boolean printColumnLess = true;// 是否打印对于源头字段数小于odps目的表的行的日志

		private volatile boolean is_compatible = true;// TODO tunnelConfig or
														// delete it

		public Record line2Record(
				com.alibaba.datax.common.element.Record dataxRecord,
				RecordSchema schema) {
			int destColumnCount = schema.getColumnCount();
			int sourceColumnCount = dataxRecord.getColumnNumber();
			Record r = new Record(schema.getColumnCount());

			if (sourceColumnCount > destColumnCount) {
				super.getSlavePluginCollector().collectDirtyRecord(dataxRecord,
						"source column number bigger than dest column num. ");
				return null;
			} else if (sourceColumnCount < destColumnCount) {
				if (printColumnLess) {
					printColumnLess = false;
					LOG.warn(
							"source column={} is less than dest column={}, fill dest column with null !",
							dataxRecord.getColumnNumber(), destColumnCount);
				}
			}

			for (int i = 0; i < sourceColumnCount; i++) {
				com.alibaba.datax.common.element.Column v = dataxRecord
						.getColumn(i);
				if (null == v)
					continue;
				// for compatible dt lib, "" as null ?? TODO
				if (is_compatible && "".equals(v)) {
					continue;
				}
				Column.Type type = schema.getColumnType(i);
				try {
					switch (type) {
					case ODPS_BIGINT:
						r.setBigint(i, v.asLong());
						break;
					case ODPS_DOUBLE:
						r.setDouble(i, v.asDouble());
						break;
					case ODPS_DATETIME:
						r.setDatetime(i, v.asDate());
						break;
					case ODPS_BOOLEAN:
						r.setBoolean(i, v.asBoolean());
						break;
					case ODPS_STRING:
						r.setString(i, v.asString());
						break;
					default:
						throw new DataXException(
								OdpsWriterErrorCode.UNSUPPORTED_COLUMN_TYPE,
								String.format("Unsupported Type: [%s] .",
										type.toString()));
					}
				} catch (Exception e) {
					LOG.warn("BAD TYPE: " + e.toString());
					super.getSlavePluginCollector().collectDirtyRecord(
							dataxRecord, "Unsupported type: " + e.getMessage());
					return null;
				}
			}
			return r;
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