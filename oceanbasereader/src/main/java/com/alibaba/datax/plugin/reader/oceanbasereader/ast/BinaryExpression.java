package com.alibaba.datax.plugin.reader.oceanbasereader.ast;

import com.alibaba.datax.plugin.reader.oceanbasereader.visitor.Visitor;

public class BinaryExpression extends Expression {

	public Expression left;
	public Expression right;
	public Operator operator;

	public BinaryExpression(Expression left, Operator op, Expression right) {
		this.left = left;
		this.operator = op;
		this.right = right;
	}

	public String toString(){
		return operator.format(left, right);
	}

	@Override
	public void accept(Visitor<?> visitor) {
		visitor.visitBinaryExpression(this);
	}
}