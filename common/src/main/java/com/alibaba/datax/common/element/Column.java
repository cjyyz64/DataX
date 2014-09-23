package com.alibaba.datax.common.element;

import java.util.Date;

/**
 * Created by jingxing on 14-8-24.
 * 
 * 暂时不支持
 */
public abstract class Column {

	private Type type;

	private Object content;

	private int byteSize;

	public Column(final Object object, final Type type, int byteSize) {
		this.content = object;
		this.type = type;
		this.byteSize = byteSize;
	}

	public Object getContent() {
		return this.content;
	}

	public Type getType() {
		return this.type;
	}

	public int getByteSize() {
		return this.byteSize;
	}

	public abstract Long asLong();

	public abstract Double asDouble();

	public abstract String toString();

	public abstract Date asDate();

	public abstract byte[] asBytes();

	public abstract Boolean asBoolean();

	public enum Type {
		NULL, STRING, NUMBER, BOOL, DATE, BYTES
	}
}
