package com.alibaba.datax.plugin.reader.hbasereader;

public enum ColumnType {
    STRING("string"),
    BYTES("bytes"),
    BOOLEAN("boolean"),
    SHORT("short"),
    INT("int"),
    LONG("long"),
    FLOAT("float"),
    DOUBLE("double"),;

    private String typeName;

    ColumnType(String typeName) {
        this.typeName = typeName;
    }

    @Override
    public String toString() {
        return this.typeName;
    }
}
