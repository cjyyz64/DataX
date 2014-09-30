package com.alibaba.datax.common.element;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.fastjson.JSON;

/**
 * Created by jingxing on 14-8-24.
 */
public class BoolColumn extends Column {

	public BoolColumn(boolean bool) {
		super(bool, Column.Type.BOOL, 1);
	}

	public BoolColumn() {
		super(null, Column.Type.BOOL, 1);
	}

	@Override
	public Boolean asBoolean() {
		if (null == super.getRawData()) {
			return null;
		}

		return (Boolean) super.getRawData();
	}

	@Override
	public Long asLong() {
		if (null == this.asBoolean()) {
			return null;
		}

		return this.asBoolean() ? 1L : 0L;
	}

	@Override
	public Double asDouble() {
		if (null == this.asBoolean()) {
			return null;
		}

		return this.asBoolean() ? 1.0d : 0.0d;
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

	@Override
	public String toString() {
		Map<String, String> map = new HashMap<String, String>();

		map.put("type", "bool");
		if (null == this.getRawData()) {
			map.put("value", null);
		} else {
			map.put("value", (((Boolean) this.getRawData()) ? "true" : "false"));
		}

		return JSON.toJSONString(map);
	}
}
