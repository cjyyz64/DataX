package com.alibaba.datax.plugin.writer.odpswriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.plugin.writer.odpswriter.util.OdpsUtil;
import com.alibaba.odps.tunnel.Column;
import com.alibaba.odps.tunnel.RecordSchema;
import com.alibaba.odps.tunnel.Upload;
import com.alibaba.odps.tunnel.io.ProtobufRecordWriter;
import com.alibaba.odps.tunnel.io.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class OdpsWriterProxy {
	private static final Logger LOG = LoggerFactory
			.getLogger(OdpsWriterProxy.class);

	private volatile boolean printColumnLess = true;// 是否打印对于源头字段数小于odps目的表的行的日志

	private TaskPluginCollector taskPluginCollector;

	private Upload slaveUpload;

	private int blockSizeInMB;

	private RecordSchema schema;

	private ByteArrayOutputStream byteArrayOutputStream;

	private int max_buffer_length;

	private ProtobufRecordWriter protobufRecordWriter;

	private AtomicLong blockId;

	private List<Integer> columnPositions;

	private List<Column.Type> tableOriginalColumnTypeList;

	private boolean emptyAsNull;

	public OdpsWriterProxy(Upload slaveUpload, int blockSizeInMB,
			AtomicLong blockId, List<Integer> columnPositions,
			TaskPluginCollector taskPluginCollector, boolean emptyAsNull)
			throws IOException {
		this.slaveUpload = slaveUpload;
		this.blockSizeInMB = blockSizeInMB;
		this.schema = this.slaveUpload.getSchema();
		this.tableOriginalColumnTypeList = OdpsUtil
				.getTableOriginalColumnTypeList(this.schema);

		this.byteArrayOutputStream = new ByteArrayOutputStream(
				(this.blockSizeInMB + 1) * 1024 * 1024);
		this.protobufRecordWriter = new ProtobufRecordWriter(schema,
				byteArrayOutputStream);
		this.blockId = blockId;
		this.columnPositions = columnPositions;
		this.taskPluginCollector = taskPluginCollector;
		this.emptyAsNull = emptyAsNull;

		// 初始化与 buffer 区相关的值

		this.max_buffer_length = this.blockSizeInMB * 1024 * 1024;
	}

	public void writeOneRecord(
			com.alibaba.datax.common.element.Record dataXRecord,
			List<Long> blocks) throws Exception {

		Record record = dataxRecordToOdpsRecord(dataXRecord, schema);

		if (null == record) {
			return;
		}

		protobufRecordWriter.write(record);

		if (byteArrayOutputStream.size() >= max_buffer_length) {
			protobufRecordWriter.close();
			OdpsUtil.slaveWriteOneBlock(this.slaveUpload,
					this.byteArrayOutputStream, blockId.get());
			LOG.info("write block {} ok.", blockId.get());

			blocks.add(blockId.get());
			byteArrayOutputStream.reset();
			protobufRecordWriter = new ProtobufRecordWriter(schema,
					byteArrayOutputStream);

			this.blockId.incrementAndGet();
		}
	}

	public void writeRemainingRecord(List<Long> blocks) throws Exception {
		// complete protobuf stream, then write to http
		protobufRecordWriter.close();
		if (byteArrayOutputStream.size() != 0) {
			OdpsUtil.slaveWriteOneBlock(this.slaveUpload,
					this.byteArrayOutputStream, blockId.get());
			LOG.info("write block {} ok.", blockId.get());

			blocks.add(blockId.get());
			// reset the buffer for next block
			byteArrayOutputStream.reset();
		}
	}

	public Record dataxRecordToOdpsRecord(
			com.alibaba.datax.common.element.Record dataXRecord,
			RecordSchema schema) throws Exception {
		int sourceColumnCount = dataXRecord.getColumnNumber();
		int destColumnCount = schema.getColumnCount();
		Record odpsRecord = new Record(destColumnCount);

		int userConfiguredColumnNumber = this.columnPositions.size();
//todo
		if (sourceColumnCount > userConfiguredColumnNumber) {
			throw DataXException
					.asDataXException(
							OdpsWriterErrorCode.ILLEGAL_VALUE,
							String.format(
									"亲，配置中的源表的列个数和目的端表不一致，源表中您配置的列数是:%s 大于目的端的列数是:%s , 这样会导致源头数据无法正确导入目的端, 请检查您的配置并修改.",
									sourceColumnCount,
									userConfiguredColumnNumber));
		} else if (sourceColumnCount < userConfiguredColumnNumber) {
			if (printColumnLess) {
				LOG.warn(
						"源表的列个数小于目的表的列个数，源表列数是:{} 目的表列数是:{} , 数目不匹配. DataX 会把目的端多出的咧的值设置为空值. 如果这个默认配置不符合您的期望，请保持源表和目的表配置的列数目保持一致",
						sourceColumnCount, userConfiguredColumnNumber);
			}
		}

		int currentIndex = -1;
		int sourceIndex = 0;
		com.alibaba.datax.common.element.Column columnValue = null;
		try {
			for (int len = sourceColumnCount; sourceIndex < len; sourceIndex++) {
				currentIndex = columnPositions.get(sourceIndex);
				Column.Type type = this.tableOriginalColumnTypeList
						.get(currentIndex);
				columnValue = dataXRecord.getColumn(sourceIndex);

				switch (type) {
				case ODPS_STRING:
					if (this.emptyAsNull && "".equals(columnValue.asString())) {
						break;
					} else {
						odpsRecord.setString(currentIndex,
								columnValue.asString());
					}
					break;
				case ODPS_BIGINT:
					odpsRecord.setBigint(currentIndex, columnValue.asLong());
					break;
				case ODPS_BOOLEAN:
					odpsRecord
							.setBoolean(currentIndex, columnValue.asBoolean());
					break;
				case ODPS_DATETIME:
					odpsRecord.setDatetime(currentIndex, columnValue.asDate());
					break;
				case ODPS_DOUBLE:
					odpsRecord.setDouble(currentIndex, columnValue.asDouble());
					break;
				default:
					break;
				}
			}

			return odpsRecord;
		} catch (Exception e) {
			String message = String.format(
					"写入 ODPS 目的表时遇到了脏数据, 因为源端第[%s]个字段, 具体值[%s] 的数据不符合ODPS对应字段的格式要求，请检查该数据并作出修改 或者您可以增大阀值，忽略这条记录.", sourceIndex,
					dataXRecord.getColumn(sourceIndex));
			this.taskPluginCollector.collectDirtyRecord(dataXRecord, e,
					message);

			return null;
		}

	}
}
