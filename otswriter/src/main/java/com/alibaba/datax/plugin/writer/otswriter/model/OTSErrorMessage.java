package com.alibaba.datax.plugin.writer.otswriter.model;

public class OTSErrorMessage {
    
    public static final String OPERATION_PARSE_ERROR = "The 'operation' only support 'PutRow' and 'UpdateRow' not '%s'.";
    
    public static final String UNSUPPORT_PARSE = "Unsupport parse '%s' to '%s'.";
    
    public static final String RECORD_AND_COLUMN_SIZE_ERROR = "Size of record not equal size of config column. record size : %d, config column size : %d.";
    
    public static final String PK_TYPE_ERROR = "Primary key type only support 'string' and 'int', not support '%s'.";
    
    public static final String ATTR_TYPE_ERROR = "Column type only support 'string','int','double','bool' and 'binary', not support '%s'.";
    
    public static final String PK_COLUMN_MISSING_ERROR = "Missing the column '%s' in 'primaryKey'.";
    
    public static final String INPUT_PK_COUNT_NOT_EQUAL_META_ERROR = "The count of 'primaryKey' not equal meta, input count : %d, primary key count : %d in meta.";
    
    public static final String INPUT_PK_TYPE_NOT_MATCH_META_ERROR = "The type of 'primaryKey' not match meta, column name : %s, input type: %s, primary key type : %s in meta.";
    
    public static final String ATTR_REPEAT_COLUMN_ERROR = "Repeat column '%s' in 'column'.";
    
    public static final String MISSING_PARAMTER_ERROR = "The param '%s' is not exist.";
    
    public static final String PARAMTER_STRING_IS_EMPTY_ERROR = "The param length of '%s' is zero.";
    
    public static final String PARAMETER_LIST_IS_EMPTY_ERROR  = "The param '%s' is a empty json array.";
    
    public static final String PARAMETER_IS_NOT_ARRAY_ERROR  = "The param '%s' is not a json array.";
    
    public static final String PARAMETER_IS_NOT_MAP_ERROR  = "The param '%s' is not a json map.";
    
    public static final String PARSE_TO_LIST_ERROR = "Can not parse '%s' to list.";
    
    public static final String PK_MAP_NAME_TYPE_ERROR = "The 'name' and 'type only support string in json map of 'primaryKey'.";
    
    public static final String ATTR_MAP_NAME_TYPE_ERROR = "The 'name' and 'type only support string in json map of 'column'.";
    
    public static final String PK_MAP_INCLUDE_NAME_TYPE_ERROR = "The only support 'name' and 'type' fileds in json map of 'primaryKey'.";
    
    public static final String ATTR_MAP_INCLUDE_NAME_TYPE_ERROR = "The only support 'name' and 'type' fileds in json map of 'column'.";
    
    public static final String PK_ITEM_IS_NOT_MAP_ERROR = "The item is not map in 'primaryKey'.";
    
    public static final String ATTR_ITEM_IS_NOT_MAP_ERROR = "The item is not map in 'column'.";
    
    public static final String PK_COLUMN_NAME_IS_EMPTY_ERROR = "The name of item can not be a empty string in 'primaryKey'.";
    
    public static final String ATTR_COLUMN_NAME_IS_EMPTY_ERROR = "The name of item can not be a empty string in 'column'.";
    
    public static final String MULTI_ATTR_COLUMN_ERROR = "Multi item in 'column', column name : %s .";
    
    public static final String COLUMN_CONVERSION_ERROR = "Column coversion error, src type : %s, src value: %s, expect type: %s .";
}
