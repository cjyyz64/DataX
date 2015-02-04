package com.alibaba.datax.plugin.writer.otswriter.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.datax.plugin.writer.otswriter.model.OTSAttrColumn;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConst;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSErrorMessage;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSMode;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSOpType;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSPKColumn;
import com.aliyun.openservices.ots.internal.model.ColumnType;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyType;

/**
 * 解析配置中参数
 * @author redchen
 *
 */
public class WriterModelParser {
    
    public static PrimaryKeyType parsePrimaryKeyType(String type) {
        if (type.equalsIgnoreCase(OTSConst.TYPE_STRING)) {
            return PrimaryKeyType.STRING;
        } else if (type.equalsIgnoreCase(OTSConst.TYPE_INTEGER)) {
            return PrimaryKeyType.INTEGER;
        } else if (type.equalsIgnoreCase(OTSConst.TYPE_BINARY)) {
            return PrimaryKeyType.BINARY;
        } else {
            throw new IllegalArgumentException(String.format(OTSErrorMessage.PK_TYPE_ERROR, type));
        }
    }
    
    public static OTSPKColumn parseOTSPKColumn(Map<String, Object> column) {
        if (column.containsKey(OTSConst.NAME) && column.containsKey(OTSConst.TYPE) && column.size() == 2) {
            Object type = column.get(OTSConst.TYPE);
            Object name = column.get(OTSConst.NAME);
            if (type instanceof String && name instanceof String) {
                String typeStr = (String) type;
                String nameStr = (String) name;
                if (nameStr.isEmpty()) {
                    throw new IllegalArgumentException(OTSErrorMessage.PK_COLUMN_NAME_IS_EMPTY_ERROR);
                }
                return new OTSPKColumn(nameStr, parsePrimaryKeyType(typeStr));
            } else {
                throw new IllegalArgumentException(OTSErrorMessage.PK_MAP_NAME_TYPE_ERROR);
            }
        } else {
            throw new IllegalArgumentException(OTSErrorMessage.PK_MAP_INCLUDE_NAME_TYPE_ERROR);
        }
    }
    
    public static List<OTSPKColumn> parseOTSPKColumnList(List<Object> values) {
        List<OTSPKColumn> pks = new ArrayList<OTSPKColumn>();
        for (Object obj : values) {
            if (obj instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> column = (Map<String, Object>) obj;
                pks.add(parseOTSPKColumn(column));
            } else {
                throw new IllegalArgumentException(OTSErrorMessage.PK_ITEM_IS_NOT_MAP_ERROR);
            }
        }
        return pks;
    }
    
    public static ColumnType parseColumnType(String type) {
        if (type.equalsIgnoreCase(OTSConst.TYPE_STRING)) {
            return ColumnType.STRING;
        } else if (type.equalsIgnoreCase(OTSConst.TYPE_INTEGER)) {
            return ColumnType.INTEGER;
        } else if (type.equalsIgnoreCase(OTSConst.TYPE_BOOLEAN)) {
            return ColumnType.BOOLEAN;
        } else if (type.equalsIgnoreCase(OTSConst.TYPE_DOUBLE)) {
            return ColumnType.DOUBLE;
        } else if (type.equalsIgnoreCase(OTSConst.TYPE_BINARY)) {
            return ColumnType.BINARY;
        } else {
            throw new IllegalArgumentException(String.format(OTSErrorMessage.ATTR_TYPE_ERROR, type));
        }
    }
    
    public static OTSAttrColumn parseOTSAttrColumn(Map<String, Object> column, OTSMode mode) {
        if (mode == OTSMode.MULTI_VERSION) {
            if (column.containsKey(OTSConst.SRC_NAME) && column.containsKey(OTSConst.NAME) && column.containsKey(OTSConst.TYPE) && column.size() == 3) {
                Object type = column.get(OTSConst.TYPE);
                Object name = column.get(OTSConst.NAME);
                Object srcName = column.get(OTSConst.SRC_NAME);
                if (type instanceof String && name instanceof String && srcName instanceof String) {
                    String typeStr = (String) type;
                    String nameStr = (String) name;
                    String srcNameStr = (String) srcName;
                    if (nameStr.isEmpty() || srcNameStr.isEmpty()) {
                        throw new IllegalArgumentException(OTSErrorMessage.ATTR_COLUMN_NAME_IS_EMPTY_ERROR);
                    }
                    return new OTSAttrColumn(srcNameStr, nameStr, parseColumnType(typeStr));
                } else {
                    throw new IllegalArgumentException(OTSErrorMessage.ATTR_MAP_SRCNAME_NAME_TYPE_ERROR);
                }
            } else {
                throw new IllegalArgumentException(OTSErrorMessage.ATTR_MAP_INCLUDE_SRCNAME_NAME_TYPE_ERROR);
            }
        } else {
            if (column.containsKey(OTSConst.NAME) && column.containsKey(OTSConst.TYPE) && column.size() == 2) {
                Object type = column.get(OTSConst.TYPE);
                Object name = column.get(OTSConst.NAME);
                if (type instanceof String && name instanceof String) {
                    String typeStr = (String) type;
                    String nameStr = (String) name;
                    if (nameStr.isEmpty()) {
                        throw new IllegalArgumentException(OTSErrorMessage.ATTR_COLUMN_NAME_IS_EMPTY_ERROR);
                    }
                    return new OTSAttrColumn(nameStr, parseColumnType(typeStr));
                } else {
                    throw new IllegalArgumentException(OTSErrorMessage.ATTR_MAP_NAME_TYPE_ERROR);
                }
            } else {
                throw new IllegalArgumentException(OTSErrorMessage.ATTR_MAP_INCLUDE_NAME_TYPE_ERROR);
            }
        }
    }
    
    private static void checkMultiAttrColumn(List<OTSPKColumn> pk, List<OTSAttrColumn> attrs, OTSMode mode) {
        // duplicate column name
        {
            Set<String> pool = new HashSet<String>();
            for (OTSAttrColumn col : attrs) {
                if (pool.contains(col.getName())) {
                    throw new IllegalArgumentException(String.format(OTSErrorMessage.MULTI_ATTR_COLUMN_ERROR, col.getName()));
                } else {
                    pool.add(col.getName());
                }
            }
            for (OTSPKColumn col : pk) {
                if (pool.contains(col.getName())) {
                    throw new IllegalArgumentException(String.format(OTSErrorMessage.MULTI_PK_ATTR_COLUMN_ERROR, col.getName()));
                } else {
                    pool.add(col.getName());
                }
            }
        }
        // duplicate src column name
        if (mode == OTSMode.MULTI_VERSION) {
            Set<String> pool = new HashSet<String>();
            for (OTSAttrColumn col : attrs) {
                if (pool.contains(col.getSrcName())) {
                    throw new IllegalArgumentException(String.format(OTSErrorMessage.MULTI_ATTR_SRC_COLUMN_ERROR, col.getSrcName()));
                } else {
                    pool.add(col.getSrcName());
                }
            }
        }
    }
    
    public static List<OTSAttrColumn> parseOTSAttrColumnList(List<OTSPKColumn> pk, List<Object> values, OTSMode mode, int columnCountLimition) {
        List<OTSAttrColumn> attrs = new ArrayList<OTSAttrColumn>();
        for (Object obj : values) {
            if (obj instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> column = (Map<String, Object>) obj;
                attrs.add(parseOTSAttrColumn(column, mode));
            } else {
                throw new IllegalArgumentException(OTSErrorMessage.ATTR_ITEM_IS_NOT_MAP_ERROR);
            }
        }
        checkMultiAttrColumn(pk, attrs, mode);
        if (attrs.size() > columnCountLimition) {
            throw new IllegalArgumentException(String.format(OTSErrorMessage.INPUT_COLUMN_COUNT_LIMIT, attrs.size(), columnCountLimition));
        }
        return attrs;
    }

    public static OTSOpType parseOTSOpType(String value, OTSMode mode) {
        OTSOpType type = null;
        if (value.equalsIgnoreCase(OTSConst.OTS_OP_TYPE_PUT)) {
            type = OTSOpType.PUT_ROW;
        } else if (value.equalsIgnoreCase(OTSConst.OTS_OP_TYPE_UPDATE)) {
            type = OTSOpType.UPDATE_ROW;
        } else {
            throw new IllegalArgumentException(String.format(OTSErrorMessage.OPERATION_PARSE_ERROR, value));
        }
        
        if (mode == OTSMode.MULTI_VERSION && type == OTSOpType.PUT_ROW) {
            throw new IllegalArgumentException(String.format(OTSErrorMessage.MUTLI_MODE_OPERATION_PARSE_ERROR, value));
        }
        return type;
    }
    
    public static OTSMode parseOTSMode(String value) {
        if (value.equalsIgnoreCase(OTSConst.OTS_MODE_NORMAL)) {
            return OTSMode.NORMAL;
        } else if (value.equalsIgnoreCase(OTSConst.OTS_MODE_MULTI_VERSION)) {
            return OTSMode.MULTI_VERSION;
        } else {
            throw new IllegalArgumentException(String.format(OTSErrorMessage.MODE_PARSE_ERROR, value));
        }
    }
}
