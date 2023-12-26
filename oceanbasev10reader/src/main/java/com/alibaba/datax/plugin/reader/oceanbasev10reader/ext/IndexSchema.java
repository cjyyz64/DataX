package com.alibaba.datax.plugin.reader.oceanbasev10reader.ext;

import static org.apache.commons.lang3.StringUtils.EMPTY;

import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IndexSchema implements Comparable<IndexSchema> {
    // primary's name is PRIMARY
    private String indexName;
    private List<String> columns;
    private Map<String, Boolean> columnIsNullMap;
    private int constraintType;
    public final static IndexSchema EMPTY_INDEX_SCHEMA = new IndexSchema();

    public IndexSchema() {
        this(EMPTY, -1);
    }

    public IndexSchema(String indexName, int constraintType) {
        this(indexName, Lists.newArrayList(), new HashMap<>(), constraintType);
    }

    public IndexSchema(String indexName, List<String> columns, Map<String, Boolean> columnsIsNullMap, int constraintType) {
        this.indexName = indexName;
        this.columns = columns;
        this.columnIsNullMap = columnsIsNullMap;
        this.constraintType = constraintType;
    }

    public List<String> getColumns() {
        return columns;
    }

    public List<String> getNotNullColumns() {
        if (columns == null || columns.isEmpty()) {
            return null;
        }
        return columns.stream().filter(e -> !columnIsNullMap.getOrDefault(e, true)).collect(Collectors.toList());
    }

    public int getAllColumnSize() {
        return columns.size();
    }

    public void addColumn(String columnName, boolean isNullable) {
        columns.add(columnName);
        columnIsNullMap.putIfAbsent(columnName, isNullable);
    }

    public boolean containColumn(String column) {
        return columnIsNullMap.containsKey(column);
    }

    public boolean isPrimary() {
        return "PRIMARY".equalsIgnoreCase(indexName) || constraintType == -2;
    }

    public boolean isUniqueIndex() {
        return !isPrimary() && constraintType <= 0;
    }

    public boolean isValueUnique() {
        return constraintType <= 0;
    }

    public String getIndexName() {
        return indexName;
    }

    @Override
    public String toString() {
        return "IndexSchema{" +
                "indexName='" + indexName + '\'' +
                ", columns=" + columns +
                ", columnIsNullMap=" + columnIsNullMap +
                ", constraintType=" + constraintType +
                '}';
    }

    @Override
    public int compareTo(IndexSchema o) {
        if (isPrimary()) {
            return 1;
        }
        if (o.isPrimary()) {
            return -1;
        }
        // choose the shortest index
        return o.getAllColumnSize() - getAllColumnSize();
    }
}
