package com.alibaba.datax.plugin.writer.oceanbasewriter.advance.ast;

import com.alibaba.datax.common.element.Record;

public class PredefinedExpression implements Expression {

	private final int index;

	public PredefinedExpression(String value) {
		this.index = Integer.valueOf(value.substring(1));
	}

	@Override
	public Object evaluate(Record record) {
		if (index >= record.getColumnNumber())
			throw new IllegalArgumentException(
					String.format(
                            "%s out of range. Tip: offset from 0 to N-1. source record [%s]",
                            "F" + index, record.toString()));
		return record.getColumn(index).getRawData();
	}

}