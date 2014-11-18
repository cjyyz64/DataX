package com.alibaba.datax.plugin.reader.oceanbasereader.ast;

public class IdentifierExpression extends Expression{

	public final String name;
	
	public IdentifierExpression(String name){
		this.name = name;
	}
	
	public String toString(){
		return name;
	}

}