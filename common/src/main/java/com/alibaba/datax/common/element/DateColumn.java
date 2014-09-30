package com.alibaba.datax.common.element;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.fastjson.JSON;

/**
 * Created by jingxing on 14-8-24.
 */
public class DateColumn extends Column {
	public DateColumn() {
		this(null);
	}

	public DateColumn(final long stamp) {
		this(new Date(stamp));
	}

	public DateColumn(final Date date) {
		super(date, Column.Type.DATE, (null == date ? 0 : 4));
	}

	@Override
	public Long asLong() {
		if (null == this.getRawData()) {
			return null;
		}

		return this.asDate().getTime();
	}

	@Override
	public Double asDouble() {
		throw new DataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
				"Date cannot cast to Double .");
	}

	@Override
	public String asString() {
		return ColumnCast.date2String(this);
	}

	@Override
	public Date asDate() {
		if (null == this.getRawData()) {
			return null;
		}

		return (Date) this.getRawData();
	}

	@Override
	public byte[] asBytes() {
		throw new DataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
				"Date cannot cast to Bytes .");
	}

	@Override
	public Boolean asBoolean() {
		throw new DataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
				"Date cannot cast to Boolean .");
	}

	@Override
	public String toString() {

		Map<String, String> map = new HashMap<String, String>();

		map.put("type", "date");
		if (null == this.getRawData()) {
			map.put("value", null);
		} else {
			map.put("value",
					new String(String.valueOf(((Date) this.getRawData())
							.getTime())));
		}

		return JSON.toJSONString(map);
	}
}
