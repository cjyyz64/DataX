package com.alibaba.datax.common.element;

import java.util.Date;

import org.apache.commons.lang3.math.NumberUtils;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;

/**
 * Created by jingxing on 14-8-24.
 */
public class NumberColumn extends Column {

	public NumberColumn(long l) {
		this(String.valueOf(l));
	}

	public NumberColumn(double d) {
		this(String.valueOf(d));
	}

	public NumberColumn(int i) {
		this(String.valueOf(i));
	}

	public NumberColumn(final String content) {
		super(content, Column.Type.NUMBER, content.length());

		boolean isLegalNumber = (null == content || NumberUtils
				.isNumber(content));
		if (!isLegalNumber) {
			throw new DataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
					String.format("[%s] illegal number format .", content));
		}

	}

	@Override
	public Long asLong() {
		if (null == this.toString()) {
			return null;
		}

		return (long) (double) (this.asDouble());
	}

	@Override
	public Double asDouble() {
		if (null == this.toString()) {
			return null;
		}

		return NumberUtils.toDouble(this.toString());
	}

	@Override
	public String toString() {
		if (null == this.getContent()) {
			return null;
		}

		return (String) this.getContent();
	}

	@Override
	public Date asDate() {
		if (null == this.toString()) {
			return null;
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
