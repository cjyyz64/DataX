package com.alibaba.datax.plugin.writer.oceanbasewriter.advance.ast;

public class ConfigurationItem {

	public final String name;
	public final Expression expression;

	public ConfigurationItem(String column, Expression expression) {
		this.name = column;
		this.expression = expression;
	}

	@Override
	public String toString() {
		return name;
	}

}