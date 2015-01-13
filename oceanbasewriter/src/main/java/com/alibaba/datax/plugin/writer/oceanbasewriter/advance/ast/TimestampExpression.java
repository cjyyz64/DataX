package com.alibaba.datax.plugin.writer.oceanbasewriter.advance.ast;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.plugin.writer.oceanbasewriter.utils.DateUtil;
import com.google.common.base.Preconditions;

import java.sql.Timestamp;

public class TimestampExpression implements Expression {

	private final String format;
	private final Expression expression;

	public TimestampExpression(String format, Expression expression) {
		this.checkRule(expression);
		this.expression = expression;
		this.format = format != "" ? format : DateUtil.DEFAULT_DATE_FORMAT;
	}

	@Override
	public Timestamp evaluate(Record record) {
		return DateUtil.parseTimestamp((String) expression.evaluate(record), format);
	}

	private void checkRule(Expression expression){
		boolean pass = expression instanceof PredefinedExpression || expression instanceof StringExpression;
		Preconditions.checkArgument(pass , "only predefined variable / string support cast timestamp");
	}
}