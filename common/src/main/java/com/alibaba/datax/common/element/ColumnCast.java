package com.alibaba.datax.common.element;

import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.time.DateUtils;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.util.Configuration;

public final class ColumnCast {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	static String findKeyPositive(final Map mapping) {
		assert null != mapping;
		for (final String key : (Set<String>) mapping.keySet()) {
			if (!key.startsWith("!")) {
				return key;
			}
		}
		throw new DataXException(CommonErrorCode.CONFIG_ERROR,
				"Data transform illegal, need ! configuration .");
	}

	public static void bind(final Configuration configuration) {
		NumberCast.init(configuration);
		StringCast.init(configuration);
		DateCast.init(configuration);
		BoolCast.init(configuration);
		BytesCast.init(configuration);
	}

	public static Boolean number2Bool(final NumberColumn column) {
		return NumberCast.asBool(column);
	}

	public static Date string2Date(final StringColumn column) {
		return StringCast.asDate(column);
	}

	public static Boolean string2Bool(final StringColumn column) {
		return StringCast.asBoolean(column);
	}

	public static byte[] string2Bytes(final StringColumn column) {
		return StringCast.asBytes(column);
	}

	public static String date2String(final DateColumn column) {
		return DateCast.asString(column);
	}

	public static Long bool2Long(final BoolColumn column) {
		return BoolCast.asLong(column);
	}

	public static String bool2String(final BoolColumn column) {
		return BoolCast.asString(column);
	}

	public static String bytes2String(final BytesColumn column) {
		return BytesCast.asString(column);
	}
}

final class NumberCast {
	static Map<String, Boolean> Number2Bool = new HashMap<String, Boolean>();
	static {
		Number2Bool.put("0", false);
		Number2Bool.put("!0", true);
	}

	static void init(final Configuration configuration) {
		Map<String, Boolean> mapping = configuration.getMap(
				"data.column.number.bool", Boolean.class);
		Number2Bool.putAll(mapping);
		return;
	}

	static Boolean asBool(final NumberColumn number) {
		String s = number.toString();
		if (null == s) {
			return null;
		}

		s = s.toLowerCase();
		if (Number2Bool.containsKey(s)) {
			return Number2Bool.get(s);
		}

		String key = ColumnCast.findKeyPositive(Number2Bool);
		return !Number2Bool.get(key);
	}
}

final class StringCast {
	static String timeFormat = "yyyy-MM-dd HH:mm:ss";

	static String timeZone = "GMT+8";

	static String encoding = "UTF-8";

	static Map<String, Boolean> String2Bool = new HashMap<String, Boolean>();

	static {
		String2Bool.put("true", true);
		String2Bool.put("!true", false);
	}

	static void init(final Configuration configuration) {
		StringCast.timeFormat = configuration
				.getString("data.column.string.date.timeFormat");
		StringCast.timeZone = configuration
				.getString("data.column.string.date.timeZone");
		StringCast.encoding = configuration
				.getString("data.column.string.bytes.encoding");

		Map<String, Boolean> string2Bool = configuration.getMap(
				"data.column.string.bool", Boolean.class);
		StringCast.String2Bool.putAll(string2Bool);

		return;
	}

	static Date asDate(final StringColumn column) {
		if (null == column.toString()) {
			return null;
		}

		try {
			return DateUtils
					.parseDate(column.toString(), StringCast.timeFormat);
		} catch (ParseException e) {
			throw new DataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
					e.getMessage());
		}
	}

	static Boolean asBoolean(final StringColumn column) {
		if (null == column.toString()) {
			return null;
		}

		String string = column.toString().toLowerCase();
		if (String2Bool.containsKey(string)) {
			return String2Bool.get(string);
		}

		String key = ColumnCast.findKeyPositive(String2Bool);
		return !String2Bool.get(key);
	}

	static byte[] asBytes(final StringColumn column) {
		if (null == column.toString()) {
			return null;
		}

		try {
			return column.toString().getBytes(StringCast.encoding);
		} catch (UnsupportedEncodingException e) {
			// it cannot be happen, since we test charset at first
			throw new DataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
					e.getMessage());
		}
	}
}

class DateCast {
	static String timeFormat = "yyyy-MM-dd HH:mm:ss";

	static DateFormat dateFormatter = new SimpleDateFormat(timeFormat);

	static String timeZone = "GMT+8";

	static void init(final Configuration configuration) {
		DateCast.timeFormat = configuration
				.getString("data.column.date.string.timeFormat");
		DateCast.timeZone = configuration
				.getString("data.column.date.string.timeZone");
		DateCast.dateFormatter = new SimpleDateFormat(timeFormat);
		return;
	}

	static String asString(final DateColumn column) {
		if (null == column.asDate()) {
			return null;
		}

		return dateFormatter.format(column.asDate());
	}
}

class BoolCast {
	static Map<Boolean, Integer> Bool2Integer = new HashMap<Boolean, Integer>();
	static {
		Bool2Integer.put(true, 1);
		Bool2Integer.put(false, 0);
	}

	static Map<Boolean, String> Bool2String = new HashMap<Boolean, String>();
	static {
		Bool2String.put(true, "true");
		Bool2String.put(false, "false");
	}

	static void init(final Configuration configuration) {
		Map<String, Integer> bool2Integer = configuration.getMap(
				"data.column.bool.number", Integer.class);
		for (final String key : bool2Integer.keySet()) {
			BoolCast.Bool2Integer.put(Boolean.valueOf(key),
					bool2Integer.get(key));
		}

		Map<String, String> bool2String = configuration.getMap(
				"data.column.bool.string", String.class);
		for (final String key : bool2String.keySet()) {
			BoolCast.Bool2String
					.put(Boolean.valueOf(key), bool2String.get(key));
		}
	}

	static Long asLong(final BoolColumn bool) {
		if (null == bool.asBoolean()) {
			return null;
		}

		return Long.valueOf(Bool2Integer.get(bool.asBoolean()).toString());
	}

	static Integer asInteger(final BoolColumn bool) {
		if (null == bool.asBoolean()) {
			return null;
		}

		return Bool2Integer.get(bool.asBoolean());
	}

	static String asString(final BoolColumn bool) {
		if (null == bool.asBoolean()) {
			return null;
		}

		return Bool2String.get(bool.asBoolean());
	}
}

class BytesCast {
	static String encoding = "utf-8";

	static void init(final Configuration configuration) {
		BytesCast.encoding = configuration
				.getString("data.column.bytes.encoding");
		return;
	}

	static String asString(final BytesColumn column) {
		if (null == column.asBytes()) {
			return null;
		}

		try {
			return new String(column.asBytes(), encoding);
		} catch (UnsupportedEncodingException e) {
			// it cannot be happen, since we test charset at first
			throw new DataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
					e.getMessage());
		}
	}
}