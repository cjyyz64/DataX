package com.alibaba.datax.common.element;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;

public class DoubleColumn extends Column {

	public DoubleColumn(final String data) {
		this(data, null == data ? 0 : data.length());
		this.validate(data);
	}

	public DoubleColumn(double data) {
		this(String.valueOf(data), 8);
	}

	public DoubleColumn(float data) {
		this(String.valueOf(data), 4);
	}

	public DoubleColumn(BigDecimal data) {
		this(null == data ? null : data.toPlainString(), null == data ? 0
				: data.toPlainString().length());
	}

	public DoubleColumn(BigInteger data) {
		this(null == data ? null : data.toString());
	}

	public DoubleColumn() {
		this(null, 0);
	}

	private DoubleColumn(final String data, int byteSize) {
		super(data, Column.Type.DOUBLE, byteSize);
	}

	@Override
	public BigDecimal asBigDecimal() {
		if (null == this.getRawData()) {
			return null;
		}

		return new BigDecimal((String) this.getRawData());
	}

	@Override
	public Double asDouble() {
		if (null == this.getRawData()) {
			return null;
		}

		return this.asBigDecimal().doubleValue();
	}

	@Override
	public BigInteger asBigInteger() {
		if (null == this.getRawData()) {
			return null;
		}

		return this.asBigDecimal().toBigInteger();
	}

	@Override
	public Long asLong() {
		if (null == this.getRawData()) {
			return null;
		}

		return this.asBigInteger().longValue();
	}

	@Override
	public String asString() {
		if (null == this.getRawData()) {
			return null;
		}
		return (String) this.getRawData();
	}

	@Override
	public Boolean asBoolean() {
		throw new DataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
				"Double cannot cast to Boolean .");
	}

	@Override
	public Date asDate() {
		throw new DataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
				"Double cannot cast to Date .");
	}

	@Override
	public byte[] asBytes() {
		throw new DataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
				"Double cannot cast to Bytes .");
	}

	private void validate(final String data) {
		if (null == data) {
			return;
		}

		try {
			new BigDecimal(data);
		} catch (Exception e) {
			throw new DataXException(
					CommonErrorCode.CONVERT_NOT_SUPPORT,
					String.format("String[%s] cannot convert to Double .", data));
		}
	}
}
