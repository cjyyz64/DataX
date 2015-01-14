package com.alibaba.datax.plugin.writer.oceanbasewriter.advance.ast;

import com.alibaba.datax.common.element.Record;

public class ValueExpression implements Expression {
	
	private final Expression expression;

	public ValueExpression(String value){
		expression = doubleLiteral(value) ? new DoubleExpression(value) : new LongExpression(value);
	}
	
	private static final boolean doubleLiteral(String value){
		return value.indexOf('.') != -1;
	}

	@Override
	public Object evaluate(Record record) {
		return expression.evaluate(record);
	}
}