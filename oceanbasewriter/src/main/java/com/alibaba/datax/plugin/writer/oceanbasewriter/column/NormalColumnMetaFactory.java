package com.alibaba.datax.plugin.writer.oceanbasewriter.column;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.plugin.writer.oceanbasewriter.strategy.Context;
import com.alibaba.datax.plugin.writer.oceanbasewriter.utils.ConfigurationChecker;
import com.google.common.collect.Lists;

import java.util.List;

public class NormalColumnMetaFactory extends ColumnMetaFactory {

	@Override
	public List<ColumnMeta> product(final Context context) {
		ConfigurationChecker.checkNormalConfig(context);
		List<ColumnMeta> columns = Lists.newArrayList();
		for (String column : context.normal()) {
			String name = column.trim().toLowerCase();
			columns.add(new ColumnMetaImpl(name));
		}
		return columns;
	}

	private static class ColumnMetaImpl implements ColumnMeta {

		private final String name;

		private ColumnMetaImpl(String name) {
			this.name = name;
		}

		@Override
		public String name() {
			return this.name;
		}

		@Override
		public Object value(Column field,Record record) throws Exception {
			return field == null ? null : field.getRawData();
		}
	}
}