package com.alibaba.datax.plugin.reader.otsreader.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.NumberColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.adaptor.ColumnValueAdaptor;
import com.alibaba.datax.plugin.reader.otsreader.adaptor.PrimaryKeyValueAdaptor;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSPrimaryKeyColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSRange;
import com.aliyun.openservices.ClientException;
import com.aliyun.openservices.ots.OTSErrorCode;
import com.aliyun.openservices.ots.OTSException;
import com.aliyun.openservices.ots.model.ColumnValue;
import com.aliyun.openservices.ots.model.Direction;
import com.aliyun.openservices.ots.model.PrimaryKeyType;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.model.Row;
import com.aliyun.openservices.ots.model.RowPrimaryKey;
import com.aliyun.openservices.ots.model.TableMeta;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class CommonUtils {
    
    private static final Logger LOG = LoggerFactory.getLogger(CommonUtils.class);
    
    private static Gson gsonBuilder() {
        return new GsonBuilder()
                    .registerTypeAdapter(ColumnValue.class, new ColumnValueAdaptor())
                    .registerTypeAdapter(PrimaryKeyValue.class, new PrimaryKeyValueAdaptor())
                    .create();
    }

    public static String rangeToString (OTSRange range) {
        Gson g = gsonBuilder();
        return g.toJson(range);
    }
    
    public static OTSRange stringToRange (String jsonStr) {
        Gson g = gsonBuilder();
        return g.fromJson(jsonStr, OTSRange.class);
    }
    
    public static String confToString (OTSConf conf) {
        Gson g = gsonBuilder();
        return g.toJson(conf);
    }
    
    public static OTSConf stringToConf (String jsonStr) {
        Gson g = gsonBuilder();
        return g.fromJson(jsonStr, OTSConf.class);
    }
    
    public static String directionToString (Direction direction) {
        Gson g = gsonBuilder();
        return g.toJson(direction);
    }
    
    public static Direction stringToDirection (String jsonStr) {
        Gson g = gsonBuilder();
        return g.fromJson(jsonStr, Direction.class);
    }
    
    public static List<PrimaryKeyValue> getSplitPointByPartitionKey(PrimaryKeyValue begin, PrimaryKeyValue end, List<PrimaryKeyValue> target) {
        List<PrimaryKeyValue> result = new ArrayList<PrimaryKeyValue>();
                
        int cmp = primaryKeyValueCmp(begin, end); 
        if (cmp < 0) { // 正序
            Collections.sort(target, new Comparator<PrimaryKeyValue>(){
                public int compare(PrimaryKeyValue arg0, PrimaryKeyValue arg1) {
                    return primaryKeyValueCmp(arg0, arg1);
                }
                
            });
            for (PrimaryKeyValue value:target) {
                if (primaryKeyValueCmp(value, begin) > 0 && primaryKeyValueCmp(value, end) < 0) {
                    result.add(value);
                }
            }
        } else if (cmp > 0) { // 逆序
            Collections.sort(target, new Comparator<PrimaryKeyValue>(){
                public int compare(PrimaryKeyValue arg0, PrimaryKeyValue arg1) {
                    return primaryKeyValueCmp(arg1, arg0);
                }
                
            });
            for (PrimaryKeyValue value:target) {
                if (primaryKeyValueCmp(value, begin) < 0 && primaryKeyValueCmp(value, end) > 0) {
                    result.add(value);
                }
            }
        } else { // 这里是支持begin == end
            result.add(begin);
            result.add(end);
        }
        
        result.add(0, begin);
        result.add(end);
        
        return result;
    }
    
    public static int primaryKeyValueCmp(PrimaryKeyValue v1, PrimaryKeyValue v2) {
        if (v1.getType() != null && v2.getType() != null) {
            if (v1.getType() != v2.getType()) {
                throw new IllegalArgumentException(String.format(
                        "Not same column type, column1:%s, column2:%s", 
                        v1.getType(), 
                        v2.getType()
                        ));
            }
            PrimaryKeyType type = v1.getType();
            if (type == PrimaryKeyType.INTEGER) {
                Long l1 = Long.valueOf(v1.asLong());
                Long l2 = Long.valueOf(v2.asLong());
                return l1.compareTo(l2);
            } else if (type == PrimaryKeyType.STRING){
                return v1.asString().compareTo(v2.asString());
            } 
        } else {
            if (v1 == v2) {
                return 0;
            } else {
                if (v1 == PrimaryKeyValue.INF_MIN) {
                    return -1;
                } else if (v1 == PrimaryKeyValue.INF_MAX) {
                    return 1;
                } 
                
                if (v2 == PrimaryKeyValue.INF_MAX) {
                    return -1;
                } else if (v2 == PrimaryKeyValue.INF_MIN) {
                    return 1;
                }    
            }
        }
        return 0;
    }
   
    public static int getRetryTimes(Exception exception, int remainingRetryTimes) throws Exception {
        OTSException e = null;
        if (exception instanceof OTSException) {
            e = (OTSException) exception;
            LOG.warn(
                    "OTSException:ErrorCode:{}, ErrorMsg:{}, RequestId:{}", 
                    new Object[]{e.getErrorCode(), e.getMessage(), e.getHostId()}
                    );
        } else if (exception instanceof ClientException) {
            throw (ClientException) exception;
        } else {
            throw exception;
        }
        
        // 无限重试
        if (e.getErrorCode().equals(OTSErrorCode.SERVER_BUSY)){
            return remainingRetryTimes;
        } else if (e.getErrorCode().equals(OTSErrorCode.NOT_ENOUGH_CAPACITY_UNIT)){
            return remainingRetryTimes;
        } else if (e.getErrorCode().equals(OTSErrorCode.TABLE_NOT_READY)){
            return remainingRetryTimes;
        }
        
        // 可重试
        else if (e.getErrorCode().equals(OTSErrorCode.INTERNAL_SERVER_ERROR)){
            --remainingRetryTimes;
        } else if (e.getErrorCode().equals(OTSErrorCode.REQUEST_TIMEOUT)){
            --remainingRetryTimes;
        } else if (e.getErrorCode().equals(OTSErrorCode.PARTITION_UNAVAILABLE)){
            --remainingRetryTimes;
        } else if (e.getErrorCode().equals(OTSErrorCode.STORAGE_TIMEOUT)){
            --remainingRetryTimes;
        } else if (e.getErrorCode().equals(OTSErrorCode.SERVER_UNAVAILABLE)){
            --remainingRetryTimes;
        } else {
            throw e;
        }
        return remainingRetryTimes;
    }
    
    //==========================================================================
    // check function
    //==========================================================================
    
    public static String checkStringParamAndGet(Configuration param, String key) {
        String value = param.getString(key, (String)null);
        if (null == value) {
            throw new IllegalArgumentException(String.format("The param '%s' is not exist.", key));
        } else if (value.length() == 0) {
            throw new IllegalArgumentException(String.format("The param length of '%s' is zero.", key));
        }
        return value;
    }
    
    public static List<Object> checkListParamAndGet(Configuration param, String key, boolean isCheckEmpty) {
        List<Object> value = param.getList(key, (List<Object>)null);
        if (null == value) {
            throw new IllegalArgumentException(String.format("The param '%s' is not exist.", key));
        } else if (isCheckEmpty && value.isEmpty()) {
            throw new IllegalArgumentException(String.format("The param  '%s' is empty.", key));
        }
        return value;
    }
    
    public static RowPrimaryKey checkInputPrimaryKeyAndGet(TableMeta meta, List<PrimaryKeyValue> range) {
        if (meta.getPrimaryKey().size() != range.size()) {
            throw new IllegalArgumentException(String.format(
                    "Input size of values not equal size of primary key. input size:%d, primary key size:%d .",
                    range.size(), meta.getPrimaryKey().size()));
        }
        RowPrimaryKey pk = new RowPrimaryKey();
        int i = 0;
        for (Entry<String, PrimaryKeyType> e: meta.getPrimaryKey().entrySet()) {
            PrimaryKeyValue value = range.get(i);
            if (e.getValue() != value.getType() && value.getType() != null) {
                throw new IllegalArgumentException(String.format(
                        "Input range type not match primary key. Input type:%s, Primary Key Type:%s, Index:%d", 
                        value.getType(), e.getValue(), i)
                        );
            } else {
                pk.addPrimaryKeyColumn(e.getKey(), value);
            }
            i++;
        }
        return pk;
    }
    
    private static OTSPrimaryKeyColumn getPartitionKey(TableMeta meta) {
        List<String> keys = new ArrayList<String>();
        keys.addAll(meta.getPrimaryKey().keySet());

        String key = keys.get(0);

        OTSPrimaryKeyColumn col = new OTSPrimaryKeyColumn();
        col.setName(key);
        col.setType(meta.getPrimaryKey().get(key));
        return col;
    }
    
    /**
     * 检测用户的输入类型是否和PartitionKey一致，顺序是否和Range一致，是否有重复列
     * @param meta
     * @param range
     */
    public static void checkInputRangeSplit(TableMeta meta, Direction direction, List<PrimaryKeyValue> range) {
        if (range.size() > 1) {
            PrimaryKeyValue cur = null;
            OTSPrimaryKeyColumn part = getPartitionKey(meta);
            for (PrimaryKeyValue v : range) {
                if (cur == null) {
                    cur = v;
                } else {
                    if (v.getType() != part.getType()) {
                        throw new IllegalArgumentException("Input type of 'range-split' not match partition key. ");
                    }
                    int cmp = primaryKeyValueCmp(cur, v);
                    if (cmp >= 0 && direction == Direction.FORWARD) {
                        throw new IllegalArgumentException("Input direction of 'range-split' not match range or the multi same column.");
                    } else if (cmp <= 0 && direction == Direction.BACKWARD) {
                        throw new IllegalArgumentException("Input direction of 'range-split' not match range or the multi same column.");
                    }
                }
            }
        }
    }
    
    public static List<String> getPrimaryKeyNameList(TableMeta meta) {
        List<String> names = new ArrayList<String>();
        
        for (Entry<String, PrimaryKeyType> e:meta.getPrimaryKey().entrySet()) {
            names.add(e.getKey());
        }
        
        return names;
    }
    
    public static int compareRangeBeginAndEnd(TableMeta meta, RowPrimaryKey begin, RowPrimaryKey end) {
        for (String key : meta.getPrimaryKey().keySet()) {
            PrimaryKeyValue v1 = begin.getPrimaryKey().get(key);
            PrimaryKeyValue v2 = end.getPrimaryKey().get(key);
            int cmp = primaryKeyValueCmp(v1, v2);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }
    
    public static List<String> getNormalColumnNameList(List<OTSColumn> columns) {
        List<String> normalColumns = new ArrayList<String>();
        for (OTSColumn col : columns) {
            if (col.getType() == OTSColumn.OTSColumnType.NORMAL) {
                normalColumns.add(col.getValue().asString());
            }
        }
        return normalColumns;
    }
    
    public static Record parseRowToLine(Row row, List<OTSColumn> columns, Record line) {
        Map<String, ColumnValue> values = row.getColumns();
        for (OTSColumn col : columns) {
            if (col.getType() == OTSColumn.OTSColumnType.CONST) {
                switch(col.getValue().getType()) {
                case STRING:  line.addColumn(new StringColumn(col.getValue().asString())); break;
                case INTEGER: line.addColumn(new NumberColumn(col.getValue().asLong()));   break;
                case DOUBLE:  line.addColumn(new NumberColumn(col.getValue().asDouble())); break;
                case BOOLEAN: line.addColumn(new BoolColumn(col.getValue().asBoolean()));  break;
                case BINARY:  line.addColumn(new BytesColumn(col.getValue().asBinary()));  break;
                default:
                    throw new IllegalArgumentException(String.format("Unsuporrt tranform the type: %s.", col.getValue().getType()));
                }
            } else {
                ColumnValue v = values.get(col.getValue().asString());
                if (v == null) {
                    line.addColumn(null);
                } else {
                    switch(v.getType()) {
                    case STRING:  line.addColumn(new StringColumn(v.asString())); break;
                    case INTEGER: line.addColumn(new NumberColumn(v.asLong()));   break;
                    case DOUBLE:  line.addColumn(new NumberColumn(v.asDouble())); break;
                    case BOOLEAN: line.addColumn(new BoolColumn(v.asBoolean()));  break;
                    case BINARY:  line.addColumn(new BytesColumn(v.asBinary()));  break;
                    default:
                        throw new IllegalArgumentException(String.format("Unsuporrt tranform the type: %s.", v.getType()));
                    }
                }
            }
        }
        return line;
    }
}
