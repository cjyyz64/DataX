package com.alibaba.datax.common.element;

import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.util.Date;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

import com.alibaba.datax.common.util.Configuration;

public final class ColumnCast {

	public static void bind(final Configuration configuration) {
		StringCast.init(configuration);
		DateCast.init(configuration);
		BytesCast.init(configuration);
	}

	public static Date string2Date(final StringColumn column)
			throws ParseException {
		return StringCast.asDate(column);
	}

	public static byte[] string2Bytes(final StringColumn column)
			throws UnsupportedEncodingException {
		return StringCast.asBytes(column);
	}

	public static String date2String(final DateColumn column) {
		return DateCast.asString(column);
	}

	public static String bytes2String(final BytesColumn column)
			throws UnsupportedEncodingException {
		return BytesCast.asString(column);
	}
}

class StringCast {
	static String timeFormat = "yyyy-MM-dd HH:mm:ss";

	static String timeZone = "GMT+8";

	static String encoding = "UTF-8";

	static void init(final Configuration configuration) {
		StringCast.timeFormat = configuration
				.getString("data.column.string.date.timeFormat");
		StringCast.timeZone = configuration
				.getString("data.column.string.date.timeZone");
		StringCast.encoding = configuration
				.getString("data.column.string.bytes.encoding");
	}

	static Date asDate(final StringColumn column) throws ParseException {
		if (null == column.asString()) {
			return null;
		}

		return DateUtils.parseDate(column.asString(), StringCast.timeFormat);
	}

	static byte[] asBytes(final StringColumn column)
			throws UnsupportedEncodingException {
		if (null == column.asString()) {
			return null;
		}

		return column.asString().getBytes(StringCast.encoding);
	}
}

/**
 * 后续为了可维护性，可以考虑直接使用 apache 的DateFormatUtils.
 * 
 * 迟南已经修复了该问题，但是为了维护性，还是直接使用apache的内置函数
 */
class DateCast {
	static String timeFormat = "yyyy-MM-dd HH:mm:ss";

	static String timeZone = "GMT+8";

	static void init(final Configuration configuration) {
		DateCast.timeFormat = configuration
				.getString("data.column.date.string.timeFormat");
		DateCast.timeZone = configuration
				.getString("data.column.date.string.timeZone");
	}

	static String asString(final DateColumn column) {
		if (null == column.asDate()) {
			return null;
		}
		return DateFormatUtils.format(column.asDate(), DateCast.timeFormat);
	}
}

class BytesCast {
	static String encoding = "utf-8";

	static void init(final Configuration configuration) {
		BytesCast.encoding = configuration
				.getString("data.column.bytes.encoding");
		return;
	}

	static String asString(final BytesColumn column)
			throws UnsupportedEncodingException {
		if (null == column.asBytes()) {
			return null;
		}

		return new String(column.asBytes(), encoding);
	}
}