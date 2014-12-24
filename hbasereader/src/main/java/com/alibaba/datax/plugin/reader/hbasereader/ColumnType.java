package com.alibaba.datax.plugin.reader.hbasereader;

import com.alibaba.datax.common.exception.DataXException;

import java.util.Arrays;

public enum ColumnType {
    STRING("string"),
    BYTES("bytes"),
    BOOLEAN("boolean"),
    SHORT("short"),
    INT("int"),
    LONG("long"),
    FLOAT("float"),
    DOUBLE("double"),
    DATE("date"),;

    private String typeName;

    ColumnType(String typeName) {
        this.typeName = typeName;
    }

    @Override
    public String toString() {
        return this.typeName;
    }

    public static ColumnType getByTypeName(String typeName) {
        for (ColumnType columnType : values()) {
            if (columnType.typeName.equalsIgnoreCase(typeName)) {
                return columnType;
            }
        }

        throw DataXException.asDataXException(HbaseReaderErrorCode.TEMP,
                String.format("Hbasereader 不支持该类型:%s, 目前支持的类型是:%s", typeName, Arrays.asList(values())));
    }

}
