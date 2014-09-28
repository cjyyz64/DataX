package com.alibaba.datax.common.element;

import java.util.Date;

import org.apache.commons.lang3.ArrayUtils;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.exception.CommonErrorCode;

/**
 * Created by jingxing on 14-8-24.
 */
public class BytesColumn extends Column {

	public BytesColumn(byte[] bytes) {
		super(ArrayUtils.clone(bytes), Column.Type.BYTES, null == bytes ? 0
				: bytes.length);
	}

	@Override
	public Long asLong() {
		throw new DataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
				"Bytes cannot cast to Long .");
	}

	@Override
	public Double asDouble() {
		throw new DataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
				"Bytes cannot cast to Long .");
	}

	@Override
	public String asString() {
		return ColumnCast.bytes2String(this);
	}

	@Override
	public Date asDate() {
		throw new DataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
				"Bytes cannot cast to Date .");
	}

	@Override
	public byte[] asBytes() {
		if (null == this.getContent()) {
			return null;
		}

		return (byte[]) this.getContent();
	}

	@Override
	public Boolean asBoolean() {
		throw new DataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
				"Bytes cannot cast to Boolean .");
	}
}
