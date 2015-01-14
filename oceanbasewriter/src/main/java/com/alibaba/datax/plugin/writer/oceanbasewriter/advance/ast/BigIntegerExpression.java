package com.alibaba.datax.plugin.writer.oceanbasewriter.advance.ast;

import com.alibaba.datax.common.element.Record;

import java.math.BigInteger;

public class BigIntegerExpression implements Expression {

	private final BigInteger value;

	public BigIntegerExpression(String value) {
		this.value = new BigInteger(value);
	}

	@Override
	public BigInteger evaluate(Record record) {
		return value;
	}

}