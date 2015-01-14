package com.alibaba.datax.plugin.writer.oceanbasewriter.column;

import com.alibaba.datax.plugin.writer.oceanbasewriter.strategy.Context;

import java.util.List;

public abstract class ColumnMetaFactory {

	protected abstract List<ColumnMeta> product(Context context);

	public static List<ColumnMeta> ColumnMeta(Context context) {
		if (context.useDsl()) {
			return new DSLColumnMetaFactory().product(context);
		} else {
			return new NormalColumnMetaFactory().product(context);
		}
	}
}