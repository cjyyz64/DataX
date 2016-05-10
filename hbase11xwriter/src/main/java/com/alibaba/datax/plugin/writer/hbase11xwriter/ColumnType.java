package com.alibaba.datax.plugin.writer.hbase11xwriter;

import com.alibaba.datax.common.exception.DataXException;

import java.util.Arrays;

/**
 * 只对 normal 模式读取时有用，多版本读取时，不存在列类型的
 */
public enum ColumnType {
    STRING("string"),
    BOOLEAN("boolean"),
    SHORT("short"),
    INT("int"),
    LONG("long"),
    FLOAT("float"),
    DOUBLE("double")
    ;

    private String typeName;

    ColumnType(String typeName) {
        this.typeName = typeName;
    }

    public static ColumnType getByTypeName(String typeName) {
        for (ColumnType columnType : values()) {
            if (columnType.typeName.equalsIgnoreCase(typeName)) {
                return columnType;
            }
        }

        throw DataXException.asDataXException(Hbase11xWriterErrorCode.ILLEGAL_VALUE,
                String.format("Hbase11xwriter 不支持该类型:%s, 目前支持的类型是:%s", typeName, Arrays.asList(values())));
    }

    @Override
    public String toString() {
        return this.typeName;
    }
}
