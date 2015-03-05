package com.alibaba.datax.plugin.writer.oceanbasewriter.column;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.plugin.writer.oceanbasewriter.strategy.Context;
import com.alibaba.datax.plugin.writer.oceanbasewriter.utils.ConfigurationChecker;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class NormalColumnMetaFactory extends ColumnMetaFactory {

	@Override
	public List<ColumnMeta> product(final Context context) {
		ConfigurationChecker.checkNormalConfig(context);
		List<ColumnMeta> columns = Lists.newArrayList();
        Map<String, String> columnType = context.columnType();
		for (String column : context.normal()) {
			String name = column.trim().toLowerCase();
            String type = columnType.get(name);
            if(Strings.isNullOrEmpty(type)){
                throw new IllegalArgumentException(String.format("can not find column %s,maybe typo",name));
            }
			columns.add(new ColumnMetaImpl(name, type));
		}
		return columns;
	}

	private static class ColumnMetaImpl implements ColumnMeta {

		private final String name;
        private final Type type;

		private ColumnMetaImpl(String name, String type) {
			this.name = name;
            this.type = Type.type(type);
		}

		@Override
		public String name() {
			return this.name;
		}

		@Override
		public Object value(Column field,Record record) throws Exception {
			return this.type.value(field);
		}

        private static enum Type{
            Int {
                @Override
                public Long value(Column field) {
                    return field.asLong();
                }
            },Varchar {
                @Override
                public String value(Column field) {
                    return field.asString();
                }
            },Timestamp {
                @Override
                public java.sql.Timestamp value(Column field) {
                    Date date = field.asDate();
                    return date == null? null : new java.sql.Timestamp(date.getTime());
                }
            },Number {
                @Override
                public BigDecimal value(Column field) {
                    return field.asBigDecimal();
                }
            },Float {
                @Override
                public Float value(Column field) {
                    Double d = field.asDouble();
                    return d == null? null : d.floatValue();
                }
            },Double {
                @Override
                public Double value(Column field) {
                    return field.asDouble();
                }
            },Bool {
                @Override
                public Boolean value(Column field) {
                    return field.asBoolean();
                }
            },Numeric {
                @Override
                public BigDecimal value(Column field) {
                    return field.asBigDecimal();
                }
            };

            public abstract <T> T value(Column field);

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