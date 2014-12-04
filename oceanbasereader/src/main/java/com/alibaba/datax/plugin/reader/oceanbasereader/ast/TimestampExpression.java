package com.alibaba.datax.plugin.reader.oceanbasereader.ast;

public class TimestampExpression extends Expression {
	
	public final String date;
	
	public TimestampExpression(String date){
		this.date = date;
	}
	
	public String toString(){
		return String.format("timestamp'%s'", date);
	}
	
}