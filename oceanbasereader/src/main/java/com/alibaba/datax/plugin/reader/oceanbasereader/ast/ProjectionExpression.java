package com.alibaba.datax.plugin.reader.oceanbasereader.ast;

public class ProjectionExpression extends Expression{

	private final String alias;
	private final Expression expression;

	public ProjectionExpression(Expression expression, String alias) {
		this.alias = alias;
		this.expression = expression;
	}

	public String toString() {
		if (alias == null) {
			return expression.toString();
		} else {
			return String.format("%s as %s", expression, alias);
		}
	}
}