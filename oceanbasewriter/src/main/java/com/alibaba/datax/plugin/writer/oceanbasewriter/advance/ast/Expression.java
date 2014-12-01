package com.alibaba.datax.plugin.writer.oceanbasewriter.advance.ast;

import com.alibaba.datax.common.element.Record;

public interface Expression {

	public Object evaluate(Record record);

}