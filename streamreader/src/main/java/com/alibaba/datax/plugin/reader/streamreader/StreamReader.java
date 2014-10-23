package com.alibaba.datax.plugin.reader.streamreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class StreamReader extends Reader {

	public static class Master extends Reader.Master {

		private Configuration originalConfig;

		@Override
		public void init() {
			this.originalConfig = super.getPluginJobConf();
			dealColumn(this.originalConfig);

			Long sliceRecordCount = this.originalConfig
					.getLong(Key.SLICE_RECORD_COUNT);
			if (null == sliceRecordCount) {
				throw new DataXException(StreamReaderErrorCode.REQUIRED_VALUE,
						"Lost config sliceRecordCount.");
			} else if (sliceRecordCount < 1) {
				throw new DataXException(StreamReaderErrorCode.ILLEGAL_VALUE,
						"sliceRecordCount can not <1.");
			}

		}

		private void dealColumn(Configuration originalConfig) {
			List<JSONObject> columns = originalConfig.getList(Key.COLUMN,
					JSONObject.class);
			if (null == columns || columns.isEmpty()) {
				throw new DataXException(StreamReaderErrorCode.REQUIRED_VALUE,
						"Lost config column.");
			}

			List<String> dealedColumns = new ArrayList<String>();
			for (JSONObject eachColumn : columns) {
				Configuration eachColumnConfig = Configuration.from(eachColumn);
				eachColumnConfig.getNecessaryValue(Constant.VALUE,
						StreamReaderErrorCode.REQUIRED_VALUE);
				String typeName = eachColumnConfig.getString(Constant.TYPE);
				if (StringUtils.isBlank(typeName)) {
					// empty typeName will be set to default type: string
					eachColumnConfig.set(Constant.TYPE, Type.STRING);
				} else {
					if (Type.DATE.name().equalsIgnoreCase(typeName)) {
						boolean notAssignDateFormat = StringUtils
								.isBlank(eachColumnConfig
										.getString(Constant.DATE_FORMAT_MARK));
						if (notAssignDateFormat) {
							eachColumnConfig.set(Constant.DATE_FORMAT_MARK,
									Constant.DEFAULT_DATE_FORMAT);
						}
					}
					if (!Type.isTypeIllegal(typeName)) {
						throw new DataXException(
								StreamReaderErrorCode.NOT_SUPPORT_TYPE,
								"Unsupported type:" + typeName);
					}
				}
				dealedColumns.add(eachColumnConfig.toJSON());
			}

			originalConfig.set(Key.COLUMN, dealedColumns);
		}

		@Override
		public void prepare() {
		}

		@Override
		public List<Configuration> split(int adviceNumber) {
			List<Configuration> configurations = new ArrayList<Configuration>();

			for (int i = 0; i < adviceNumber; i++) {
				configurations.add(this.originalConfig.clone());
			}
			return configurations;
		}

		@Override
		public void post() {
		}

		@Override
		public void destroy() {
		}

	}

	public static class Slave extends Reader.Slave {

		private Configuration readerSliceConfig;

		private List<String> columns;

		private long sliceRecordCount;

		@Override
		public void init() {
			this.readerSliceConfig = super.getPluginJobConf();
			this.columns = this.readerSliceConfig.getList(Key.COLUMN,
					String.class);

			this.sliceRecordCount = this.readerSliceConfig
					.getLong(Key.SLICE_RECORD_COUNT);
		}

		@Override
		public void prepare() {
		}

		@Override
		public void startRead(RecordSender recordSender) {
			Record oneRecord = buildOneRecord(recordSender, this.columns);

			while (this.sliceRecordCount > 0) {
				recordSender.sendToWriter(oneRecord);
				this.sliceRecordCount--;
			}
		}

		@Override
		public void post() {
		}

		@Override
		public void destroy() {
		}

		private Record buildOneRecord(RecordSender recordSender,
				List<String> columns) {
			if (null == recordSender) {
				throw new IllegalArgumentException(
						"Parameter recordSender can not be null.");
			}

			if (null == columns || columns.isEmpty()) {
				throw new IllegalArgumentException(
						"Parameter columns can not be null nor empty.");
			}

			Record record = recordSender.createRecord();
			try {
				for (String eachColumn : columns) {
					Configuration eachColumnConfig = Configuration
							.from(eachColumn);
					String columnValue = eachColumnConfig
							.getString(Constant.VALUE);
					Type columnType = Type.valueOf(eachColumnConfig.getString(
							Constant.TYPE).toUpperCase());
					switch (columnType) {
					case STRING:
						record.addColumn(new StringColumn(columnValue));
						break;
					case LONG:
						record.addColumn(new LongColumn(columnValue));
						break;
					case DOUBLE:
						record.addColumn(new DoubleColumn(columnValue));
						break;
					case DATE:
						SimpleDateFormat format = new SimpleDateFormat(
								eachColumnConfig
										.getString(Constant.DATE_FORMAT_MARK));
						record.addColumn(new DateColumn(format
								.parse(columnValue)));
						break;
					case BOOL:
						record.addColumn(new BoolColumn("true"
								.equalsIgnoreCase(columnValue) ? true : false));
						break;
					case BYTES:
						record.addColumn(new BytesColumn(columnValue.getBytes()));
						break;
					default:
						// in fact,never to be here
						throw new Exception("Unsupported type:"
								+ columnType.name());
					}
				}
			} catch (Exception e) {
				throw new DataXException(StreamReaderErrorCode.ILLEGAL_VALUE,
						"Construct one record failed.", e);
			}

			return record;
		}
	}

	private enum Type {
		STRING, LONG, BOOL, DOUBLE, DATE, BYTES, ;

		private static boolean isTypeIllegal(String typeString) {
			try {
				Type.valueOf(typeString.toUpperCase());
			} catch (Exception e) {
				return false;
			}

			return true;
		}
	}

}
