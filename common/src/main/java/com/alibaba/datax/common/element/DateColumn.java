package com.alibaba.datax.common.element;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;

import java.util.Date;

/**
 * Created by jingxing on 14-8-24.
 */
public class DateColumn extends Column {
	public DateColumn(final Date date) {
		this(date.getTime());
	}

	public DateColumn(final long stamp) {
		super(new Date(stamp), Type.DATE, 4);
	}

	@Override
	public Long asLong() {
		if (null == this.getContent()) {
			return null;
		}

		return this.asDate().getTime();
	}

	@Override
	public Double asDouble() {
		throw new DataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
				"Date cannot cast to Double .");
	}

	@Override
	public String asString() {
		return ColumnCast.date2String(this);
	}

	@Override
	public Date asDate() {
		if (null == this.getContent()) {
			return null;
		}

		return (Date) this.getContent();
	}

	@Override
	public byte[] asBytes() {
		throw new DataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
				"Date cannot cast to Bytes .");
	}

	@Override
	public Boolean asBoolean() {
		throw new DataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
				"Date cannot cast to Boolean .");
	}

}
