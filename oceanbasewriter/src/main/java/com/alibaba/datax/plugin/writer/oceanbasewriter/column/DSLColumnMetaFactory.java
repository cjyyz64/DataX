package com.alibaba.datax.plugin.writer.oceanbasewriter.column;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.plugin.writer.oceanbasewriter.advance.ConfigurationParser;
import com.alibaba.datax.plugin.writer.oceanbasewriter.advance.ast.ConfigurationItem;
import com.alibaba.datax.plugin.writer.oceanbasewriter.advance.ast.Expression;
import com.alibaba.datax.plugin.writer.oceanbasewriter.strategy.Context;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

public class DSLColumnMetaFactory extends ColumnMetaFactory {

	@Override
	public List<ColumnMeta> product(final Context context) {
		List<String> dsl = context.dsl();
		Map<String, String> columnType = context.columnType();
		List<ColumnMeta> columns = Lists.newArrayList();
		for(String column : dsl){
            ConfigurationItem item = ConfigurationParser.parse(column);
			String name = item.name.trim().toLowerCase();
			String type = columnType.get(name);
            if(Strings.isNullOrEmpty(type)){
                throw new IllegalArgumentException(String.format("can not find column %s,maybe typo",name));
            }
			columns.add(new ColumnMetaImpl(name,type,item.expression));
		}
		return columns;
	}

	private static class ColumnMetaImpl implements ColumnMeta {

		private final String name;
		private final Type type;
		private final Expression expression;
		private ColumnMetaImpl(String name, String type,Expression expression) {
			this.name = name;
			this.type = Type.type(type);
			this.expression = expression;
		}

		@Override
		public String name() {
			return this.name;
		}

		@Override
		public Object value(Column field,Record record) throws Exception {
			return this.type.value(expression.evaluate(record));
		}

		private static enum Type {
			Int {
				@Override
				protected Class<?> expect() {
					return Long.class;
				}
				@Override
				public Object stringToValue(String value) throws Exception {
					return Long.valueOf(value);
				}
			},
			Varchar {
				@Override
				protected Class<?> expect() {
					return Object.class;
				}
				@Override
				public Object stringToValue(String value) throws Exception {
					return value;
				}
			},
			Timestamp {
				@Override
				protected Class<?> expect() {
					return Timestamp.class;
				}
				@Override
				public Object stringToValue(String value) throws Exception {
					DateFormat formater = new SimpleDateFormat(DEFAULT_DATE_FORMAT);
					return new Timestamp(formater.parse(value).getTime());
				}
			},
			Number {
				@Override
				protected Class<?> expect() {
					return BigDecimal.class;
				}
				@Override
				public Object stringToValue(String value) throws Exception {
					return new BigDecimal(value);
				}
			},
			Float {
				@Override
				protected Class<?> expect() {
					return Float.class;
				}

				@Override
				public Object stringToValue(String value) throws Exception {
					return java.lang.Float.valueOf(value);
				}
			},
			Double {
				@Override
				protected Class<?> expect() {
					return Double.class;
				}
				@Override
				public Object stringToValue(String value) throws Exception {
					return java.lang.Double.valueOf(value);
				}
			},
			Bool {
				@Override
				protected Class<?> expect() {
					return Boolean.class;
				}

				@Override
				public Object stringToValue(String value) throws Exception {
					return "0".equals(value) || "false".equalsIgnoreCase(value.toLowerCase()) ? false : true;
				}
			},
			Numeric {
				@Override
				protected Class<?> expect() {
					return BigDecimal.class;
				}
				@Override
				public Object stringToValue(String value) throws Exception {
					return new BigDecimal(value);
				}
			};
			
			private static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
			public final Object value(Object value) throws Exception{
				if(value == null) return null;
				Class<?> expect = this.expect();
				Class<?> actual = value.getClass();
				if(expect.isAssignableFrom(actual)){
					return value;
				}
				if(String.class.isAssignableFrom(actual)){
					return this.stringToValue((String)value);
				}
				throw new Exception(String.format("column expect %s but %s type", expect, actual));
			}
			
			protected abstract Class<?> expect();

			public abstract Object stringToValue(String value) throws Exception;
			
			public static Type type(String name) {
				for (Type type : Type.values()) {
					if (name.toLowerCase().startsWith(type.name().toLowerCase()))
						return type;
				}
				throw new IllegalArgumentException(String.format("unkonw column type [%s]", name));
			}
		}
	}

}