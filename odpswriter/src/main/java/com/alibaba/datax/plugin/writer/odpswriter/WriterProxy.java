package com.alibaba.datax.plugin.writer.odpswriter;

import java.util.List;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.NumberColumn;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;

public class WriterProxy {
	private RecordReceiver recordReceiver;

	private TableSchema tableSchema;
	private List<OdpsType> tableOriginalColumnTypeList;
	private UploadSession uploadSession;

	public WriterProxy(RecordReceiver recordReceiver, TableSchema tableSchema,
			List<OdpsType> tableOriginalColumnTypeList,
			UploadSession uploadSession) {
		this.recordReceiver = recordReceiver;
		this.tableSchema = tableSchema;
		this.tableOriginalColumnTypeList = tableOriginalColumnTypeList;
		this.uploadSession = uploadSession;
	}

	public void doWrite() {

		try {
			Record odpsRecord = null;
			com.alibaba.datax.common.element.Record dataXRecord;
			int originalColumnSize = this.tableSchema.getColumns().size();
			long blockId = 1;
			RecordWriter recordWriter = uploadSession.openRecordWriter(blockId);

			while ((dataXRecord = this.recordReceiver.getFromReader()) != null) {
				odpsRecord = uploadSession.newRecord();

				for (int i = 0, len = dataXRecord.getColumnNumber(); i < len; i++) {
					Column column = tableSchema.getColumn(i);
					switch (column.getType()) {
					case BIGINT:
						odpsRecord.setBigint(i, dataXRecord.getColumn(i)
								.asLong());
						break;
					case BOOLEAN:
						odpsRecord.setBoolean(i, dataXRecord.getColumn(i)
								.asBoolean());
						break;
					case DATETIME:
						odpsRecord.setDatetime(i, dataXRecord.getColumn(i)
								.asDate());
						break;
					case DOUBLE:
						odpsRecord.setDouble(i, dataXRecord.getColumn(i)
								.asDouble());
						break;
					case STRING:
						odpsRecord.setString(i, dataXRecord.getColumn(i)
								.toString());
						break;
					default:
						throw new RuntimeException("Unknown column type: "
								+ column.getType());

					}
				}
				recordWriter.write(odpsRecord);
			}
			recordWriter.close();

			uploadSession.commit(new Long[] { blockId });
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void odpsColumnToDataXField(Record odpsRecord,
			com.alibaba.datax.common.element.Record dataXRecord,
			List<OdpsType> tableOriginalColumnTypeList, int i) {
		OdpsType type = tableOriginalColumnTypeList.get(i);
		switch (type) {
		case BIGINT: {
			dataXRecord.addColumn(new NumberColumn(odpsRecord.getBigint(i)));
			break;
		}
		case BOOLEAN: {
			dataXRecord.addColumn(new BoolColumn(odpsRecord.getBoolean(i)));
			break;
		}
		case DATETIME: {
			dataXRecord.addColumn(new DateColumn(odpsRecord.getDatetime(i)));
			break;
		}
		case DOUBLE: {
			dataXRecord.addColumn(new NumberColumn(odpsRecord.getDouble(i)));
			break;
		}
		case STRING: {
			dataXRecord.addColumn(new StringColumn(odpsRecord.getString(i)));
			break;
		}
		default:
			throw new DataXException(OdpsWriterErrorCode.NOT_SUPPORT_TYPE,
					"Unknown column type: " + type);
		}
	}

}
