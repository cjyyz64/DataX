package com.alibaba.datax.common.element;

import java.util.Date;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.exception.CommonErrorCode;

/**
 * Created by jingxing on 14-8-24.
 */
public class BoolColumn extends Column {

	public BoolColumn(boolean bool) {
		super(bool, Column.Type.BOOL, 1);
	}

	@Override
	public Boolean asBoolean() {
		if (null == super.getContent()) {
			return null;
		}

		return (Boolean) super.getContent();
	}

	@Override
	public Long asLong() {
		return ColumnCast.bool2Long(this);
	}

	@Override
	public Double asDouble() {
		if (null == this.asLong()) {
			return null;
		}

		return this.asLong() == 1L ? 1.0 : 0.0;
	}

	@Override
	public String asString() {
		return ColumnCast.bool2String(this);
	}

	@Override
	public Date asDate() {
		throw new DataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
				"Boolean cannot cast to Date .");
	}

	@Override
	public byte[] asBytes() {
		throw new DataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
				"Boolean cannot cast to Bytes .");
	}
}
