package com.alibaba.datax.common.element;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.math.NumberUtils;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.fastjson.JSON;

/**
 * Created by jingxing on 14-8-24.
 */
public class NumberColumn extends Column {

	public NumberColumn() {
		this(null);
	}

	public NumberColumn(long l) {
		this(String.valueOf(l));
	}

	public NumberColumn(double d) {
		this(String.valueOf(d));
	}

	public NumberColumn(int i) {
		this(String.valueOf(i));
	}

	public NumberColumn(final String numberInString) {
		super(numberInString, Column.Type.NUMBER, (null == numberInString ? 0
				: numberInString.length()));

		if (null == numberInString) {
			return;
		}

		boolean isLegalNumber = NumberUtils.isNumber(numberInString);
		if (!isLegalNumber) {
			throw new DataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
					String.format("[%s] illegal number format .",
							numberInString));
		}

	}

	@Override
	public String toString() {
		Map<String, String> map = new HashMap<String, String>();

		map.put("type", "number");
		map.put("value",
				null == this.getRawData() ? null : (String) this.getRawData());

		return JSON.toJSONString(map);
	}

	@Override
	public Long asLong() {
		if (null == this.asString()) {
			return null;
		}

		return (long) (double) (this.asDouble());
	}

	@Override
	public Double asDouble() {
		if (null == this.asString()) {
			return null;
		}

		return Double.valueOf(this.asString());
	}

	@Override
	public String asString() {
		if (null == this.getRawData()) {
			return null;
		}

		return (String) this.getRawData();
	}

	@Override
	public Date asDate() {
		if (null == this.asString()) {
			return null;
		}

		boolean isDouble = this.asString().indexOf(".") >= 0;
		if (isDouble) {
			throw new DataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
					"Double cannot cast to Date .");
		}

		return new Date(this.asLong());
	}

	@Override
	public Boolean asBoolean() {
		if (null == this.asLong()) {
			return null;
		}

		return ColumnCast.number2Bool(this);
	}

	@Override
	public byte[] asBytes() {
		throw new DataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
				"Number cannot cast to Bytes .");
	}
}
