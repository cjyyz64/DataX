package com.alibaba.datax.plugin.reader.oceanbasereader.ast;

public class NumberExpression extends Expression {

	public final String number;

	public NumberExpression(String number) {
		this.number = number;
	}
	
	public String toString(){
		return number;
	}

}