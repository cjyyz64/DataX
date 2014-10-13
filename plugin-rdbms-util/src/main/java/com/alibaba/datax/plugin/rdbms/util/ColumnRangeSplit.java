package com.alibaba.datax.plugin.rdbms.util;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.exception.DataXException;

public final class ColumnRangeSplit {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(ColumnRangeSplit.class);

	private static final String FILTER_NUMBER_SQL_TEMPLATE = " (%s IS NOT NULL AND %s >= %s AND %s < %s) ";

	private static final String FILTER_NUMBER_SQL_LAST_TEMPLATE = " (%s IS NOT NULL AND %s >= %s and %s <= %s) ";

	private static final String FILTER_STRING_SQL_TEMPLATE = " (%s IS NOT NULL AND %s >= '%s' and %s < '%s') ";

	private static final String FILTER_STRING_SQL_LAST_TEMPLATE = " (%s IS NOT NULL AND %s >= '%s' and %s <= '%s') ";

	private static final String FILTER_COLUMN_IS_NULL_TEMPLATE = " (%s IS NULL) ";

	public static List<String> split(final Connection connection,
			final String table, final String column, final String quote, int slice) {
		return ColumnRangeSplit.split(connection, table, quote + column + quote, slice);
	}

	public static List<String> split(final Connection connection,
			final String table, final String column, int slice) {

		String type = ColumnRangeSplit.getSplitColumnType(connection, table,
				column);

		boolean isLegal = ("string".equals(type) || "number".equals(type));
		if (!isLegal) {
			throw new DataXException(
					DBUtilErrorCode.COLUMN_SPLIT_ERROR,
					String.format(
							"Type of Column [%s] in Table [%s] is [%s], it MUST be STRING or NUMBER !",
							column, table, type));
		}

		List<String> filterSql = new ArrayList<String>();

		boolean isNumberType = "number".equals(type);
		if (isNumberType) {
			List<BigInteger> bound = ColumnRangeSplit.getNumberColumnBound(
					connection, table, column);

			boolean isEmptyTable = bound.isEmpty();

			if (!isEmptyTable) {
				List<BigInteger> range = RangeSplit.splitBigIntegerRange(
						bound.get(0), bound.get(1), slice);

				for (int i = 0; i < range.size() - 1; i++) {
					boolean isLast = (i == range.size() - 2);
					if (isLast) {
						filterSql
								.add(String.format(
										FILTER_NUMBER_SQL_LAST_TEMPLATE,
										column, column, range.get(i), column,
										range.get(i + 1)));
					} else {
						filterSql.add(String.format(FILTER_NUMBER_SQL_TEMPLATE,
								column, column, range.get(i), column,
								range.get(i + 1)));
					}
				}
			}
		}

		boolean isStringType = "string".equals(type);
		if (isStringType) {
			List<String> bound = ColumnRangeSplit.getStringColumnBound(
					connection, table, column);

			boolean isEmptyTable = bound.isEmpty();

			if (!isEmptyTable) {
				List<String> range = RangeSplit.splitStringRange(bound.get(0),
						bound.get(1), slice);

				for (int i = 0; i < range.size() - 1; i++) {
					boolean isLast = (i == range.size() - 2);
					if (isLast) {
						filterSql
								.add(String.format(
										FILTER_STRING_SQL_LAST_TEMPLATE,
										column, column, range.get(i), column,
										range.get(i + 1)));
					} else {
						filterSql.add(String.format(FILTER_STRING_SQL_TEMPLATE,
								column, column, range.get(i), column,
								range.get(i + 1)));
					}
				}
			}
		}

		filterSql.add(String.format(FILTER_COLUMN_IS_NULL_TEMPLATE, column));

		return filterSql;
	}

	private static String getColumnTypeSQL(final String table,
			final String column) {
		String template = "SELECT max(%s) FROM %s WHERE 1 != 1";
		return String.format(template, column, table);
	}

	private static String getColumnBoundSQL(final String table,
			final String column) {
		String template = "SELECT min(%s), max(%s) FROM %s WHERE %s IS NOT NULL ";
		return String.format(template, column, column, table, column);
	}

	@SuppressWarnings("unchecked")
	private static List<BigInteger> getNumberColumnBound(
			final Connection connection, final String table, final String column) {
		ResultSet resultSet = null;
		try {

			LOGGER.info(String.format("Use [%s] to find [%s.%s] column bound ",
					ColumnRangeSplit.getColumnBoundSQL(table, column), table,
					column));

			resultSet = DBUtil.query(connection,
					ColumnRangeSplit.getColumnBoundSQL(table, column), 1);

			List<BigInteger> bound = new ArrayList<BigInteger>();
			while (resultSet.next()) {
				boolean isEmptyTable = resultSet.getString(1) == null;
				if (isEmptyTable) {
					return Collections.EMPTY_LIST;
				}

				bound.add(new BigInteger(resultSet.getString(1)));
				bound.add(new BigInteger(resultSet.getString(2)));
				break;
			}

			LOGGER.info(String.format("Column bound [%s, %s] .", bound.get(0),
					bound.get(1)));

			return bound;
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			return Collections.EMPTY_LIST;
		} finally {
			DBUtil.closeResultSet(resultSet);
		}
	}

	@SuppressWarnings("unchecked")
	private static List<String> getStringColumnBound(
			final Connection connection, final String table, final String column) {
		ResultSet resultSet = null;
		try {

			LOGGER.info(String.format("Use [%s] to find [%s.%s] column bound ",
					ColumnRangeSplit.getColumnBoundSQL(table, column), table,
					column));

			resultSet = DBUtil.query(connection,
					ColumnRangeSplit.getColumnBoundSQL(table, column), 1);

			List<String> bound = new ArrayList<String>();
			while (resultSet.next()) {
				boolean isEmptyTable = resultSet.getString(1) == null;
				if (isEmptyTable) {
					return Collections.EMPTY_LIST;
				}

				bound.add(resultSet.getString(1));
				bound.add(resultSet.getString(2));
				break;
			}

			LOGGER.info(String.format("Column bound [%s, %s] .", bound.get(0),
					bound.get(1)));

			return bound;
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			return Collections.EMPTY_LIST;
		} finally {
			DBUtil.closeResultSet(resultSet);
		}
	}

	private static String getSplitColumnType(final Connection connection,
			final String table, final String splitColumn) {
		ResultSet rs = null;

		try {
			rs = DBUtil.query(connection,
					ColumnRangeSplit.getColumnTypeSQL(table, splitColumn), 1);

			int type = rs.getMetaData().getColumnType(1);
			switch (type) {
			case Types.BIGINT:
			case Types.INTEGER:
			case Types.SMALLINT:
			case Types.TINYINT:
				return "number";

			case Types.NCHAR:
			case Types.NVARCHAR:
			case Types.VARCHAR:
			case Types.CHAR:
			case Types.LONGNVARCHAR:
			case Types.LONGVARCHAR:
				return "string";

			default:
				return rs.getMetaData().getColumnTypeName(1);
			}
		} catch (Exception e) {
			LOGGER.warn(e.toString());
			return e.getMessage();
		} finally {
			DBUtil.closeResultSet(rs);
		}

	}
}
