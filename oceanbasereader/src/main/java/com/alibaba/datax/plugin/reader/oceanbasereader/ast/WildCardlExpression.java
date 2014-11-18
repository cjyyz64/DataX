package com.alibaba.datax.plugin.reader.oceanbasereader.ast;

public class WildCardlExpression extends Expression {

	private WildCardlExpression() {
	}

	public static final WildCardlExpression instance = new WildCardlExpression();

	public String toString() {
		return "*";
	}


}