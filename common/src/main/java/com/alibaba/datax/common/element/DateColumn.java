package com.alibaba.datax.common.element;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;

/**
 * Created by jingxing on 14-8-24.
 */
public class DateColumn extends Column {

	private DateType subType = DateType.DATETIME;

	public static enum DateType {
		DATE, TIME, DATETIME
	}

	/**
	 * 构建值为null的DateColumn，使用Date子类型为DATETIME
	 * */
	public DateColumn() {
		this((Date) null);
	}

	/**
	 * 构建值为stamp(Unix时间戳)的DateColumn，使用Date子类型为DATETIME
	 * */
	public DateColumn(final long stamp) {
		this(new Date(stamp));
	}

	/**
	 * 构建值为date(java.util.Date)的DateColumn，使用Date子类型为DATETIME
	 * */
	public DateColumn(final Date date) {
		super(date, Column.Type.DATE, (null == date ? 0 : 4));
	}

	/**
	 * 构建值为date(java.sql.Date)的DateColumn，使用Date子类型为DATE，只有日期，没有时间
	 * */
	public DateColumn(final java.sql.Date date) {
		super(date, Column.Type.DATE, (null == date ? 0 : 4));
		this.setSubType(DateType.DATE);
	}

	/**
	 * 构建值为time(java.sql.Time)的DateColumn，使用Date子类型为TIME，只有时间，没有日期
	 * */
	public DateColumn(final java.sql.Time time) {
		super(time, Column.Type.DATE, (null == time ? 0 : 4));
		this.setSubType(DateType.TIME);
	}

	/**
	 * 构建值为ts(java.sql.Timestamp)的DateColumn，使用Date子类型为DATETIME
	 * */
	public DateColumn(final java.sql.Timestamp ts) {
		super(ts, Column.Type.DATE, (null == ts ? 0 : 4));
		this.setSubType(DateType.DATETIME);
	}

	@Override
	public Long asLong() {
		if (null == this.getRawData()) {
			return null;
		}

		return this.asDate().getTime();
	}

	@Override
	public String asString() {
		try {
			return ColumnCast.date2String(this);
		} catch (Exception e) {
			throw new DataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
					String.format("Date[%d] format to String failed .",
							this.toString()));
		}
	}

	@Override
	public Date asDate() {
		if (null == this.getRawData()) {
			return null;
		}

		return (Date) this.getRawData();
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

	@Override
	public Double asDouble() {
		throw new DataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
				"Date cannot cast to Double .");
	}

	@Override
	public BigInteger asBigInteger() {
		throw new DataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
				"Date cannot cast to BigInteger .");
	}

	@Override
	public BigDecimal asBigDecimal() {
		throw new DataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
				"Date cannot cast to BigDecimal .");
	}

	public DateType getSubType() {
		return subType;
	}

	public void setSubType(DateType subType) {
		this.subType = subType;
	}
}