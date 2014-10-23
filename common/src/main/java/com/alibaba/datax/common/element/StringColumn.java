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
					String.format("String[%s] convert to BigInteger failed",
							this.asString()));
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
					String.format("String[%s] convert to Long failed",
							this.asString()));
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
					String.format("String[%s] convert to BigDecimal failed",
							this.asString()));
		}
	}

	@Override
	public Boolean asBoolean() {
		if (null == this.getRawData()) {
			return null;
		}

		if ("true".equals(this.asString())) {
			return true;
		}

		if ("false".equals(this.asString())) {
			return false;
		}

		throw new DataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
				String.format("String[%s] convert to Boolean failed .",
						this.asString()));
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
		try {
			return ColumnCast.string2Date(this);
		} catch (Exception e) {
			throw new DataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
					String.format("String[%s] convert to Date failed .",
							this.asString()));
		}
	}

	@Override
	public byte[] asBytes() {
		try {
			return ColumnCast.string2Bytes(this);
		} catch (Exception e) {
			throw new DataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
					String.format("String[%s] convert to Bytes failed .",
							this.asString()));
		}
	}
}
