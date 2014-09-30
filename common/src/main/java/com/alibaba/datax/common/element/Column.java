package com.alibaba.datax.common.element;

import java.util.Date;

import com.alibaba.fastjson.JSON;

/**
 * Created by jingxing on 14-8-24.
 * <p/>
 * 暂时不支持
 */
public abstract class Column {

	private Type type;

	private Object rawData;

	private int byteSize;

	public Column(final Object object, final Type type, int byteSize) {
		this.rawData = object;
		this.type = type;
		this.byteSize = byteSize;
	}

	public Object getRawData() {
		return this.rawData;
	}

	public Type getType() {
		return this.type;
	}

	public int getByteSize() {
		return this.byteSize;
	}

	protected void setType(Type type) {
		this.type = type;
	}

	protected void setRawData(Object rawData) {
		this.rawData = rawData;
	}

	protected void setByteSize(int byteSize) {
		this.byteSize = byteSize;
	}

	public abstract Long asLong();

	public abstract Double asDouble();

	public abstract String asString();

	public abstract Date asDate();

	public abstract byte[] asBytes();

	public abstract Boolean asBoolean();

	@Override
	public String toString() {
		return JSON.toJSONString(this);
	}

	public enum Type {
		NULL, STRING, NUMBER, BOOL, DATE, BYTES
	}
}
