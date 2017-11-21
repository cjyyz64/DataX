package com.alibaba.datax.plugin.writer.hbase11xsqlwriter;

import com.alibaba.datax.common.exception.DataXException;

import java.util.Arrays;

/**
 * @author yanghan.y
 */
public enum VersionMode {
    Column("column"),           // 使用record中指定的列作为时间戳列
    Constant("constant")        // 使用指定的值
    ;

    private String name;

    VersionMode(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }

    /**
     * 将字符串的versionMode转化为枚举值，大小写无关
     */
    public static VersionMode getByName(String modeName) {
        for (VersionMode modeType : values()) {
            if (modeType.name.equalsIgnoreCase(modeName)) {
                return modeType;
            }
        }

        // 没有找到
        throw DataXException.asDataXException(HbaseSQLWriterErrorCode.ILLEGAL_VALUE,
                "Hbasewriter 不支持该 versionMode 类型:" + modeName + ", 目前支持的 nullMode 类型是:" + Arrays.asList(values()));
    }
}
