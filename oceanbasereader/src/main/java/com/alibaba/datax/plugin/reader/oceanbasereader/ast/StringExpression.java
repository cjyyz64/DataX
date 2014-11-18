package com.alibaba.datax.plugin.reader.oceanbasereader.ast;

public class StringExpression extends Expression{

	public final String value;
	
	public StringExpression(String value){
		this.value = value;
	}
	
	public String toString(){
		return "'" + value + "'";
	}

}