package com.alibaba.datax.plugin.writer.oceanbasewriter.column;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;

public interface ColumnMeta {

	public abstract String name();
	public abstract Object value(Column field, Record record) throws Exception;
}