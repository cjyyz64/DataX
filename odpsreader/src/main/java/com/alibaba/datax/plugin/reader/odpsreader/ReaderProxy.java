package com.alibaba.datax.plugin.reader.odpsreader;

import java.io.IOException;
import java.util.List;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.NumberColumn;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.util.Configuration;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;

public class ReaderProxy {
	private RecordSender recordSender;
	private Configuration sliceConfig;

	private RecordReader recordReader;
	private TableSchema tableSchema;
	private List<OdpsType> tableOriginalColumnTypeList;

	public ReaderProxy(RecordSender recordSender, RecordReader recordReader,
			TableSchema tableSchema, Configuration sliceConfig,
			List<OdpsType> tableOriginalColumnTypeList) {
		this.recordSender = recordSender;
		this.recordReader = recordReader;
		this.recordReader = recordReader;
		this.tableSchema = tableSchema;
		this.sliceConfig = sliceConfig;
		this.tableOriginalColumnTypeList = tableOriginalColumnTypeList;
	}

	public void doRead() {
		List<String> allColumnParsedWithConstant = this.sliceConfig.getList(
				Constant.ALL_COLUMN_PARSED_WITH_CONSTANT, String.class);

		List<Integer> columnPositions = this.sliceConfig.getList(
				Constant.COLUMN_POSITIONS, Integer.class);

		try {
			Record odpsRecord;
			String tempStr = null;
			int originalColumnSize = this.tableSchema.getColumns().size();
			while ((odpsRecord = recordReader.read()) != null) {
				com.alibaba.datax.common.element.Record dataXRecord = recordSender
						.createRecord();

				for (int i : columnPositions) {
					if (i >= originalColumnSize) {
						tempStr = allColumnParsedWithConstant.get(i);
						dataXRecord.addColumn(new StringColumn(tempStr
								.substring(1, tempStr.length() - 1)));
					} else {

						odpsColumnToDataXField(odpsRecord, dataXRecord,
								this.tableOriginalColumnTypeList, i);
					}
				}

				recordSender.sendToWriter(dataXRecord);
			}
			recordReader.close();
		} catch (IOException e) {
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
			throw new DataXException(OdpsReaderErrorCode.NOT_SUPPORT_TYPE,
					"Unknown column type: " + type);
		}
	}

}
