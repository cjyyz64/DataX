package com.alibaba.datax.common.element;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import org.apache.commons.lang3.math.NumberUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

public class LongColumn extends Column {

	/**
	 * 从整形字符串表示转为LongColumn，支持Java科学计数法
	 * 
	 * NOTE: <br>
	 * 如果data为浮点类型的字符串表示，数据将会失真，请使用DoubleColumn对接浮点字符串
	 * 
	 * */
	public LongColumn(final String data) {
		super(null, Column.Type.LONG, 0);
		if (null == data) {
			return;
		}

		try {
			BigInteger rawData = NumberUtils.createBigDecimal(data)
					.toBigInteger();
			super.setRawData(rawData);
			super.setByteSize(rawData.bitLength() / 8);
		} catch (Exception e) {
			throw new DataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
					"Data: " + data + " is not legal Long format .");
		}
	}

	public LongColumn(long data) {
		this(BigInteger.valueOf(data), 8);
	}

	public LongColumn(int data) {
		this(BigInteger.valueOf(data), 4);
	}

	public LongColumn(BigInteger data) {
		this(data, null == data ? 0 : data.bitLength() / 8);
	}

	private LongColumn(BigInteger data, int byteSize) {
		super(data, Column.Type.LONG, byteSize);
	}

	public LongColumn() {
		this((BigInteger) null);
	}

	@Override
	public BigInteger asBigInteger() {
		if (null == this.getRawData()) {
			return null;
		}

		return (BigInteger) this.getRawData();
	}

	@Override
	public Long asLong() {
		if (null == this.getRawData()) {
			return null;
		}

		return this.asBigInteger().longValue();
	}

	@Override
	public Boolean asBoolean() {
		if (null == this.getRawData()) {
			return null;
		}
		return this.asLong() != 0 ? true : false;
	}

	@Override
	public BigDecimal asBigDecimal() {
		if (null == this.getRawData()) {
			return null;
		}

		return new BigDecimal(this.asBigInteger());
	}

	@Override
	public Double asDouble() {
		if (null == this.getRawData()) {
			return null;
		}

		return this.asBigDecimal().doubleValue();
	}

	@Override
	public String asString() {
		if (null == this.getRawData()) {
			return null;
		}
		return ((BigInteger) this.getRawData()).toString();
	}

	@Override
	public Date asDate() {
		if (null == this.getRawData()) {
			return null;
		}
		return new Date(this.asLong());
	}

	@Override
	public byte[] asBytes() {
		throw new DataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
				"Long cannot cast to Bytes .");
	}

}
