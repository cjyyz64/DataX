package com.alibaba.datax.plugin.reader.oceanbasereader.ast;

public class NullExpression extends Expression {

	private NullExpression() {
	}

	public static final NullExpression instance = new NullExpression();

	public String toString() {
		return "null";
	}


}