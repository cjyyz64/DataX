package com.alibaba.datax.plugin.writer.oceanbasewriter.advance.ast;

import com.alibaba.datax.common.element.Record;

import java.math.BigInteger;

public class LongExpression implements Expression {

	private final Long value;
	
	public LongExpression(String value){
		this.value = Long.valueOf(value);
	}
	
	@Override
	public BigInteger evaluate(Record record) {
		return BigInteger.valueOf(value);
	}

}