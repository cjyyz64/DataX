package com.alibaba.datax.common.element;

import java.util.Date;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.exception.CommonErrorCode;

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

	private NumberColumn(final String content) {
		super(content, Column.Type.NUMBER, content.length());
	}

	@Override
	public Long asLong() {
		if (null == this.toString()) {
			return null;
		}

		return Long.valueOf(this.toString());
	}

	@Override
	public Double asDouble() {
		if (null == this.toString()) {
			return null;
		}

		return Double.valueOf(this.toString());
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
