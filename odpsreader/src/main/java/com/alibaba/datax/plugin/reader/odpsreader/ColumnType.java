package com.alibaba.datax.plugin.reader.odpsreader;

public enum ColumnType {
    PARTITION, NORMAL, CONSTANT, ;

    @Override
    public String toString() {
        switch (this) {
        case PARTITION:
            return "partition column";
        case NORMAL:
            return "normal column";
        case CONSTANT:
            return "constant column";
        default:
            return "unknown";
        }
    }
}
