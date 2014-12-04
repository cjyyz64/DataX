package com.alibaba.datax.plugin.reader.oceanbasereader.ast;

public class UnaryExpression extends Expression {

	public final Operator operator;
	public final Expression operand;

	public UnaryExpression(Operator operator, Expression operand) {
		this.operator = operator;
		this.operand = operand;
	}

	@Override
	public String toString() {
		return operator.format(operand, null);
	}
}