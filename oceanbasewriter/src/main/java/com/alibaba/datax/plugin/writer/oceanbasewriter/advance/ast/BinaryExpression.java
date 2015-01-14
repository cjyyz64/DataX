package com.alibaba.datax.plugin.writer.oceanbasewriter.advance.ast;

import com.alibaba.datax.common.element.Record;

public class BinaryExpression implements Expression {

	private final BinaryOperator op;
	private final Expression a;
	private final Expression b;

	public BinaryExpression(Expression a,BinaryOperator op,Expression b){
		this.a = a;
		this.b = b;
		this.op = op;
	}
	
	@Override
	public Object evaluate(Record record) {
		return op.evaluate(record, a, b);
	}


}