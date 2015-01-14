package com.alibaba.datax.plugin.writer.oceanbasewriter.advance.ast;

import com.alibaba.datax.common.element.Record;

public class DoubleExpression implements Expression {

	private final Double value;

	public DoubleExpression(String value) {
		this.value = Double.valueOf(value);
	}

	@Override
	public Double evaluate(Record record) {
		return value;
	}

}