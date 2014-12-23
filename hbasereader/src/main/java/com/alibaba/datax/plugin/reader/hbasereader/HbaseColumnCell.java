package com.alibaba.datax.plugin.reader.hbasereader;

import com.alibaba.datax.common.exception.DataXException;
import org.apache.commons.lang3.Validate;

/**
 * 描述 hbasereader 插件中，column 配置中的一个单元项实体
 */
public class HbaseColumnCell {
    private ColumnType columnType;

    // columnName 格式为：列族:列名
    private String columnName;

    //对于常量类型，其常量值放到 columnValue 里
    private String columnValue;

    //当配置了 columnValue 时，isConstant=true（这个成员变量是用于方便使用本类的地方判断是否是常量类型字段）
    private boolean isConstant;

    private HbaseColumnCell(Builder builder) {
        this.columnType = builder.columnType;

        //columnName 和 columnValue 必须有一个为 null
        Validate.isTrue(builder.columnName == null || builder.columnValue == null, "Hbasereader  中，column 不能同时配置 列名称 和 列值,二者选其一.");

        //columnName 和 columnValue 不能都为 null
        Validate.isTrue(builder.columnName == null && builder.columnValue == null, "Hbasereader  中，column 需要配置 列名称 或者 列值, 二者选其一.");

        if (builder.columnName != null) {
            this.isConstant = false;
            this.columnName = builder.columnName;

            // 如果 columnName 不是 rowkey，则必须配置为：列族:列名 格式
            if (!"rowkey".equalsIgnoreCase(this.columnName) && !this.columnName.contains(":")) {
                throw DataXException.asDataXException(HbaseReaderErrorCode.ILLEGAL_VALUE, "Hbasereader 中， column 的列配置格式应该是：列族:列名");
            }
        } else {
            this.isConstant = true;
            this.columnValue = builder.columnValue;
        }
    }

    public static class Builder {
        private ColumnType columnType;
        private String columnName;
        private String columnValue;

        public Builder(ColumnType columnType) {
            this.columnType = columnType;
        }

        public Builder columnName(String columnName) {
            this.columnName = columnName;
            return this;
        }

        public Builder columnValue(String columnValue) {
            this.columnValue = columnValue;
            return this;
        }

        public HbaseColumnCell build() {
            return new HbaseColumnCell(this);
        }
    }

    public ColumnType getColumnType() {
        return columnType;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getColumnValue() {
        return columnValue;
    }

}
