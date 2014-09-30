package com.alibaba.datax.common.element;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.ArrayUtils;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.fastjson.JSON;

/**
 * Created by jingxing on 14-8-24.
 */
public class BytesColumn extends Column {

	public BytesColumn() {
		this(null);
	}

	public BytesColumn(byte[] bytes) {
		super(ArrayUtils.clone(bytes), Column.Type.BYTES, null == bytes ? 0
				: bytes.length);
	}

	@Override
	public byte[] asBytes() {
		if (null == this.getRawData()) {
			return null;
		}

		return (byte[]) this.getRawData();
	}

	@Override
	public String asString() {
		return ColumnCast.bytes2String(this);
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
	public Date asDate() {
		throw new DataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
				"Bytes cannot cast to Date .");
	}

	@Override
	public Boolean asBoolean() {
		throw new DataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
				"Bytes cannot cast to Boolean .");
	}

	@Override
	public String toString() {
		Map<String, String> map = new HashMap<String, String>();

		map.put("type", "bytes");
		if (null == this.getRawData()) {
			map.put("value", null);
		} else {
			map.put("value",
					"0x"
							+ new String(Hex.encodeHex((byte[]) this
									.getRawData())));
		}

		return JSON.toJSONString(map);
	}
}
