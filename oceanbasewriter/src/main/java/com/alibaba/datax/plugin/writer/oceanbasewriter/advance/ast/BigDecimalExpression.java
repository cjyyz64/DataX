package com.alibaba.datax.plugin.writer.oceanbasewriter.advance.ast;

import com.alibaba.datax.common.element.Record;

import java.math.BigDecimal;

public class BigDecimalExpression implements Expression {

	private final BigDecimal value;

	public BigDecimalExpression(String value) {
		this.value = new BigDecimal(value);
	}

	@Override
	public BigDecimal evaluate(Record record) {
		return value;
	}
}