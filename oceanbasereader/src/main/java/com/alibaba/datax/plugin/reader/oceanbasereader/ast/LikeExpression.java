package com.alibaba.datax.plugin.reader.oceanbasereader.ast;

public class LikeExpression extends Expression {

	public final Expression expression;
	public final boolean like; // like or not like
	public final Expression pattern;

	public LikeExpression(Expression expression, boolean like,
			Expression pattern) {
		this.expression = expression;
		this.like = like;
		this.pattern = pattern;
	}

	public String toString() {
		return String.format("%s %s %s", expression, like ? "like"
				: "not like", pattern);
	}
}