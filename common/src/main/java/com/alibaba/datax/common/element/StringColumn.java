package com.alibaba.datax.common.element;

import java.util.Date;

/**
 * Created by jingxing on 14-8-24.
 */

public class StringColumn extends Column {

	public StringColumn(final String content) {
		super(content, Column.Type.STRING, content.length());
	}

	@Override
	public String asString() {
		if (null == this.getContent()) {
			return null;
		}

		return (String) this.getContent();
	}

	@Override
	public Long asLong() {
		if (null == this.getContent()) {
			return null;
		}

		return Long.valueOf(this.asString());
	}

	@Override
	public Double asDouble() {
		if (null == this.getContent()) {
			return null;
		}

		return Double.valueOf(this.asString());
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
