package com.alibaba.datax.plugin.writer.mysqlwriter.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.mysqlwriter.Constant;
import com.alibaba.datax.plugin.writer.mysqlwriter.Key;
import com.alibaba.datax.plugin.writer.mysqlwriter.MysqlWriterErrorCode;

public final class MysqlWriterSplitUtil {
	private static final Logger LOG = LoggerFactory
			.getLogger(MysqlWriterSplitUtil.class);

	// TODO 是否要求，所有表上，要么都有presql, 要么都有postSql ???
	// TODO 参数需要在 init 中进行校验
	public static List<Configuration> doSplit(Configuration simplifiedConf,
			int adviceNumber) {

		List<Configuration> splittedConfigs = new ArrayList<Configuration>();

		int tableNumber = simplifiedConf.getInt(Constant.TABLE_NUMBER_MARK)
				.intValue();
		if (tableNumber != adviceNumber) {
			throw new DataXException(MysqlWriterErrorCode.UNKNOWN_ERROR,
					String.format("tableNumber:[%s], but adviceNumb:[%s]",
							tableNumber, adviceNumber));
		}

		String jdbcUrl = null;
		List<String> preSqls = null;
		List<String> postSqls = null;

		List<Object> conns = simplifiedConf.getList(Constant.CONN_MARK,
				Object.class);
		for (int i = 0, len = conns.size(); i < len; i++) {
			Configuration sliceConfig = simplifiedConf.clone();

			Configuration connConf = Configuration
					.from(conns.get(i).toString());
			jdbcUrl = connConf.getString(Key.JDBC_URL);
			preSqls = connConf.getList(Key.PRE_SQL, String.class);
			postSqls = connConf.getList(Key.POST_SQL, String.class);

			sliceConfig.set(Key.JDBC_URL, appendJDBCSuffix(jdbcUrl));
			sliceConfig.set(Key.PRE_SQL, preSqls);
			sliceConfig.set(Key.POST_SQL, postSqls);

			sliceConfig.remove(Constant.CONN_MARK);

			// 已在之前进行了扩展和`处理，可以直接使用了！
			List<String> tables = connConf.getList(Key.TABLE, String.class);
			Validate.isTrue(null != tables && !tables.isEmpty(),
					"dest table configed error.");

			// 尝试对每个表，切分为eachTableShouldSplittedNumber 份
			for (String table : tables) {
				Configuration tempSlice = sliceConfig.clone();
				tempSlice.set(Key.TABLE, table);

				splittedConfigs.add(tempSlice);
			}

		}

		return splittedConfigs;
	}

	private static String appendJDBCSuffix(String jdbc) {
		String suffix = "yearIsDateType=false&zeroDateTimeBehavior=convertToNull&useCursorFetch=true";

		if (jdbc.contains("?")) {
			return jdbc + "&" + suffix;
		} else {
			return jdbc + "?" + suffix;
		}
	}

}
