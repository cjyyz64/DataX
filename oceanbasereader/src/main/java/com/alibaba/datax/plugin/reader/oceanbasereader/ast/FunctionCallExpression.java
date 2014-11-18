package com.alibaba.datax.plugin.reader.oceanbasereader.ast;

import com.google.common.base.Joiner;

import java.util.List;

public class FunctionCallExpression extends Expression {

	private final String name;
	private final List<Expression> args;

	public FunctionCallExpression(String name, List<Expression> args) {
		this.name = name;
		this.args = args;
	}
	
	public String toString(){
		return String.format("%s(%s)", name, Helper.joiner.join(args));
	}
	
	private static class Helper{
		  static final Joiner joiner= Joiner.on(",");
	  }

}