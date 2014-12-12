package com.alibaba.datax.plugin.writer.oceanbasewriter.advance.ast;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.google.common.collect.Maps;

import java.io.UnsupportedEncodingException;
import java.util.Map;

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
		return DataxType.fetchValue(record.getColumn(index));
	}

    private static enum DataxType{
        NULL {
            @Override
            public Object value(Column column) {
                return null;
            }
        }, LONG {
            @Override
            public Object value(Column column) {
                return column.asBigInteger();
            }
        }, DOUBLE {
            @Override
            public Object value(Column column) {
                return column.asBigDecimal();
            }
        }, STRING {
            @Override
            public Object value(Column column) {
                return column.asString();
            }
        }, BOOL {
            @Override
            public Object value(Column column) {
                return column.asBoolean();
            }
        }, DATE {
            @Override
            public Object value(Column column) {
                return column.asDate();
            }
        }, BYTES {
            @Override
            public Object value(Column column) {
                try {
                    return new String(column.asBytes(),"ISO-8859-1");
                } catch (UnsupportedEncodingException e) {
                    return null;
                }
            }
        };
        public abstract Object value(Column column);

        private static final Map<Column.Type,DataxType> types = Maps.newHashMap();
        static {
            types.put(Column.Type.NULL, NULL);
            types.put(Column.Type.LONG, LONG);
            types.put(Column.Type.DOUBLE, DOUBLE);
            types.put(Column.Type.STRING, STRING);
            types.put(Column.Type.BOOL, BOOL);
            types.put(Column.Type.DATE, DATE);
            types.put(Column.Type.BYTES, BYTES);
        }

        public static Object fetchValue(Column column){
            if (column == null) return null;
            if(types.containsKey(column.getType())){
                return types.get(column.getType()).value(column);
            }
            throw new UnsupportedOperationException("not support type " + column.toString());
        }
    }
}