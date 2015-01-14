package com.alibaba.datax.plugin.writer.oceanbasewriter.advance.ast;

import com.alibaba.datax.common.element.Record;

public class StringExpression implements Expression {

	private final String value;
	
	public StringExpression(String value){
		this.value = value;
	}
	
	@Override
	public String evaluate(Record record) {
		return value;
	}

}