package com.alibaba.datax.plugin.reader.sqlserverreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.StrUtil;
import com.alibaba.datax.plugin.reader.sqlserverreader.Constant;
import com.alibaba.datax.plugin.reader.sqlserverreader.Key;
import com.alibaba.datax.plugin.reader.sqlserverreader.SqlServerReaderErrorCode;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public final class SqlServerReaderSplitUtil {
	private static final Logger LOG = LoggerFactory
			.getLogger(SqlServerReaderSplitUtil.class);

	private SqlServerReaderSplitUtil() {
	}

	// 任务切分
	public static List<Configuration> doSplit(Configuration readerSliceConfig,
			int adviceNumber) {
		List<Configuration> splittedConfigs = new ArrayList<Configuration>();
		boolean isTableMode = readerSliceConfig.getBool(Constant.IS_TABLE_MODE)
				.booleanValue();
		int eachTableShouldSplittedNumber = -1;
		if (isTableMode) {
			eachTableShouldSplittedNumber = calculateEachTableShouldSplittedNumber(
					adviceNumber,
					readerSliceConfig.getInt(Constant.TABLE_NUMBER_MARK));
		}

		String column = readerSliceConfig.getString(Key.COLUMN);
		String where = readerSliceConfig.getString(Key.WHERE, null);

		List<Object> conns = readerSliceConfig.getList(Constant.CONN_MARK,
				Object.class);

		String jdbcUrl = null;

		for (int i = 0, len = conns.size(); i < len; i++) {
			Configuration sliceConfig = readerSliceConfig.clone();

			Configuration connConf = Configuration
					.from(conns.get(i).toString());
			jdbcUrl = connConf.getString(Key.JDBC_URL);
			// warn:no use of appendJDBCSuffix
			sliceConfig.set(Key.JDBC_URL, jdbcUrl);

			sliceConfig.remove(Constant.CONN_MARK);

			Configuration tempSlice;

			// 说明是配置的 table 方式
			if (isTableMode) {
				// 已在之前进行了扩展和[]处理，可以直接使用
				List<String> tables = connConf.getList(Key.TABLE, String.class);

				if (null == tables || tables.isEmpty()) {
					String bussinessMessage = "source table configed error.";
					String message = StrUtil.buildOriginalCauseMessage(
							bussinessMessage, null);
					LOG.error(message);

					throw new DataXException(
							SqlServerReaderErrorCode.ILLEGAL_VALUE,
							bussinessMessage);
				}

				String splitPk = readerSliceConfig
						.getString(Key.SPLIT_PK, null);

				// 最终切分份数不一定等于 eachTableShouldSplittedNumber
				boolean needSplitTable = eachTableShouldSplittedNumber > 1
						&& StringUtils.isNotBlank(splitPk);

				if (needSplitTable) {
					// 尝试对每个表，切分为eachTableShouldSplittedNumber 份
					for (String table : tables) {
						tempSlice = sliceConfig.clone();
						tempSlice.set(Key.TABLE, table);
						// warn : this should be eachTableShouldSplittedNumber
						List<Configuration> splittedSlices = SingleTableSplitUtil
								.splitSingleTable(tempSlice,
										eachTableShouldSplittedNumber);

						splittedConfigs.addAll(splittedSlices);
					}
				} else {

					for (String table : tables) {
						tempSlice = sliceConfig.clone();
						tempSlice.set(Key.QUERY_SQL, SingleTableSplitUtil
								.buildQuerySql(column, table, where));
						splittedConfigs.add(tempSlice);
					}
				}
			} else {
				// 说明是配置的 querySql 方式
				List<String> sqls = connConf.getList(Key.QUERY_SQL,
						String.class);

				for (String querySql : sqls) {
					tempSlice = sliceConfig.clone();
					tempSlice.set(Key.QUERY_SQL, querySql);
					splittedConfigs.add(tempSlice);
				}
			}

		}

		return splittedConfigs;
	}

	// TODO 是否允许用户自定义该配置？
	@SuppressWarnings("unused")
	private static String appendJDBCSuffix(String jdbc) {
		String suffix = "yearIsDateType=false&zeroDateTimeBehavior=convertToNull";

		if (jdbc.contains("?")) {
			return jdbc + "&" + suffix;
		} else {
			return jdbc + "?" + suffix;
		}
	}

	private static int calculateEachTableShouldSplittedNumber(int adviceNumber,
			int tableNumber) {
		double tempNum = 1.0 * adviceNumber / tableNumber;

		return (int) Math.ceil(tempNum);
	}

}
