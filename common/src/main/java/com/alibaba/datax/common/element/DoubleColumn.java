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

		try {
			return new BigDecimal((String) this.getRawData());
		} catch (NumberFormatException e) {
			throw DataXException.asDataXException(
					CommonErrorCode.CONVERT_NOT_SUPPORT,
					String.format("String[%s] 无法转换为Double类型 .",
							(String) this.getRawData()));
		}
	}

	@Override
	public Double asDouble() {
		if (null == this.getRawData()) {
			return null;
		}

		String string = (String) this.getRawData();

		boolean isDoubleSpecific = string.equals("NaN")
				|| string.equals("-Infinity") || string.equals("+Infinity");
		if (isDoubleSpecific) {
			return Double.valueOf(string);
		}

		BigDecimal result = this.asBigDecimal();
		OverFlowUtil.validateDoubleNotOverFlow(result);

		return result.doubleValue();
	}

	@Override
	public Long asLong() {
		if (null == this.getRawData()) {
			return null;
		}

		BigDecimal result = this.asBigDecimal();
		OverFlowUtil.validateLongNotOverFlow(result.toBigInteger());

		return result.longValue();
	}

	@Override
	public BigInteger asBigInteger() {
		if (null == this.getRawData()) {
			return null;
		}

		return this.asBigDecimal().toBigInteger();
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
		throw DataXException.asDataXException(
				CommonErrorCode.CONVERT_NOT_SUPPORT, "Double类型无法转为Bool .");
	}

	@Override
	public Date asDate() {
		throw DataXException.asDataXException(
				CommonErrorCode.CONVERT_NOT_SUPPORT, "Double类型无法转为Date类型 .");
	}

	@Override
	public byte[] asBytes() {
		throw DataXException.asDataXException(
				CommonErrorCode.CONVERT_NOT_SUPPORT, "Double类型无法转为Bytes类型 .");
	}

	private void validate(final String data) {
		if (null == data) {
			return;
		}

		if (data.equalsIgnoreCase("NaN") || data.equalsIgnoreCase("-Infinity")
				|| data.equalsIgnoreCase("Infinity")) {
			return;
		}

		try {
			new BigDecimal(data);
		} catch (Exception e) {
			throw DataXException.asDataXException(
					CommonErrorCode.CONVERT_NOT_SUPPORT,
					String.format("字符串[%s] 无法转为Double类型 .", data));
		}
	}

}
