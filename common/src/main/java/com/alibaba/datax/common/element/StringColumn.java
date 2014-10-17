package com.alibaba.datax.common.element;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;

/**
 * Created by jingxing on 14-8-24.
 */

public class StringColumn extends Column {

	public StringColumn() {
		this(null);
	}

	public StringColumn(final String rawData) {
		super(rawData, Column.Type.STRING, (null == rawData ? 0 : rawData
				.length()));
	}

	@Override
	public String asString() {
		if (null == this.getRawData()) {
			return null;
		}

		return (String) this.getRawData();
	}

	@Override
	public BigInteger asBigInteger() {
		if (null == this.getRawData()) {
			return null;
		}

		try {
			return this.asBigDecimal().toBigInteger();
		} catch (Exception e) {
			throw new DataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
					"String convert to BigInteger failed, for "
							+ e.getMessage());
		}
	}

	@Override
	public Long asLong() {
		if (null == this.getRawData()) {
			return null;
		}

		try {
			return this.asBigInteger().longValue();
		} catch (Exception e) {
			throw new DataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
					"String convert to Long failed, for " + e.getMessage());
		}
	}

	@Override
	public BigDecimal asBigDecimal() {
		if (null == this.getRawData()) {
			return null;
		}

		try {
			return new BigDecimal(this.asString());
		} catch (Exception e) {
			throw new DataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
					"String convert to BigDecimal failed, for "
							+ e.getMessage());
		}
	}

	@Override
	public Double asDouble() {
		if (null == this.getRawData()) {
			return null;
		}

		return this.asBigDecimal().doubleValue();
	}

	@Override
	public Date asDate() {
		return ColumnCast.string2Date(this);
	}

	@Override
	public byte[] asBytes() {
		return ColumnCast.string2Bytes(this);
	}

	@Override
	public Boolean asBoolean() {
		return ColumnCast.string2Bool(this);
	}
}
