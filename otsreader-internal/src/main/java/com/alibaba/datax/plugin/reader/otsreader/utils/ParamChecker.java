package com.alibaba.datax.plugin.reader.otsreader.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.Constant;
import com.alibaba.datax.plugin.reader.otsreader.Key;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSCriticalException;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSMode;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSRange;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyColumn;
import com.aliyun.openservices.ots.internal.model.PrimaryKeySchema;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.internal.model.TableMeta;
import com.aliyun.openservices.ots.internal.model.TimeRange;

public class ParamChecker {
    
    private static void throwNotExistException() {
        throw new IllegalArgumentException("missing the key.");
    }
    
    private static void throwStringLengthZeroException() {
        throw new IllegalArgumentException("input the key is empty string.");
    }
    
    public static String checkStringAndGet(Configuration param, String key, boolean isTrim) throws OTSCriticalException {
        try {
            String value = param.getString(key);
            if (isTrim) {
                value = value != null ? value.trim() : null;
            }
            if (null == value) {
                throwNotExistException();
            } else if (value.length() == 0) {
                throwStringLengthZeroException();
            }
            return value;
        } catch(RuntimeException e) {
            throw new OTSCriticalException("Parse '"+ key +"' fail, " + e.getMessage(), e);
        }
    }
    
    public static OTSRange checkRangeAndGet(Configuration param) throws OTSCriticalException {
        try {
            OTSRange range = new OTSRange();
            Map<String, Object> value = param.getMap(Key.RANGE);
            // 用户可以不用配置range，默认表示导出全表
            if (value == null) {
                return range;
            }
            
            /**
             * Range格式：{
             *  "begin":[],
             *  "end":[]
             * }
             */
            
            // begin
            // 如果不存在，表示从表开始位置读取
            Object arrayObj = value.get(Constant.KEY.Range.BEGIN);
            if (arrayObj != null) {
                range.setBegin(ParamParser.parsePrimaryKeyColumnArray(arrayObj));
            }
            
            // end
            // 如果不存在，表示读取到表的结束位置
            arrayObj = value.get(Constant.KEY.Range.END);
            if (arrayObj != null) {
                range.setEnd(ParamParser.parsePrimaryKeyColumnArray(arrayObj));
            }
            
            // split
            // 如果不存在，表示不做切分
            arrayObj = value.get(Constant.KEY.Range.SPLIT);
            if (arrayObj != null) {
                List<PrimaryKeyColumn> pksPrefix = ParamParser.parsePrimaryKeyColumnArray(arrayObj);
                List<List<PrimaryKeyColumn>> pks = new ArrayList<List<PrimaryKeyColumn>>();
                for (PrimaryKeyColumn p : pksPrefix) {
                    List<PrimaryKeyColumn> pk = new ArrayList<PrimaryKeyColumn>();
                    pk.add(p);
                    pks.add(pk);
                }
                range.setSplit(pks);
            }
            
            return range;
        } catch (RuntimeException e) {
            throw new OTSCriticalException("Parse 'range' fail, " + e.getMessage(), e);
        }
        
    }
    
    public static TimeRange checkTimeRangeAndGet(Configuration param) throws OTSCriticalException {
        try {
            
            long begin = Constant.VALUE.TimeRange.MIN;
            long end = Constant.VALUE.TimeRange.MAX;
            
            Map<String, Object> value = param.getMap(Constant.KEY.TIME_RANGE);
            // 用户可以不用配置time range，默认表示导出全表
            if (value == null) {
                return new TimeRange(begin, end);
            }
            
            /**
             * TimeRange格式：{
             *  "begin":,
             *  "end":
             * }
             */

            // begin
            // 如果不存在，表示从表开始位置读取
            Object obj = value.get(Constant.KEY.TimeRange.BEGIN);
            if (obj != null) {
                begin = ParamParser.parseTimeRangeItem(obj, Constant.KEY.TimeRange.BEGIN);
            }
            
            // end
            // 如果不存在，表示读取到表的结束位置
            obj = value.get(Constant.KEY.TimeRange.END);
            if (obj != null) {
                end = ParamParser.parseTimeRangeItem(obj, Constant.KEY.TimeRange.END);
            }
            
            TimeRange range = new TimeRange(begin, end);
            return range;
        } catch (RuntimeException e) {
            throw new OTSCriticalException("Parse 'timeRange' fail, " + e.getMessage(), e);
        }
    }
    
    private static void checkColumnByMode(List<OTSColumn> columns , OTSMode mode) {
        if (mode == OTSMode.MULTI_VERSION) {
            for (OTSColumn c : columns) {
                if (c.getColumnType() != OTSColumn.OTSColumnType.NORMAL) {
                    throw new IllegalArgumentException("in mode:'multiVersion', the 'column' only support specify column_name not const column.");
                }
            }
        } else {
            if (columns.isEmpty()) {
                throw new IllegalArgumentException("in mode:'normal', the 'column' must specify at least one column_name or const column.");
            }
        }
    }
    
    public static List<OTSColumn> checkOTSColumnAndGet(Configuration param, OTSMode mode) throws OTSCriticalException {
        try {
            List<Object> value = param.getList(Key.COLUMN);
            // 用户可以不用配置Column
            if (value == null) {
                value = Collections.emptyList();
            }
            
            /**
             * Column格式：[
             *  {"Name":"pk1"},
             *  {"type":"Binary","value" : "base64()"}
             * ]
             */
            List<OTSColumn> columns = ParamParser.parseOTSColumnArray(value);
            checkColumnByMode(columns, mode);
            return columns;
        } catch (RuntimeException e) {
            throw new OTSCriticalException("Parse 'column' fail, " + e.getMessage(), e);
        }
    }
    
    public static OTSMode checkModeAndGet(Configuration param) throws OTSCriticalException {
        try {
            String modeValue = checkStringAndGet(param, Key.MODE, true);
            if (modeValue.equalsIgnoreCase(Constant.VALUE.Mode.NORMAL)) {
                return OTSMode.NORMAL;
            } else if (modeValue.equalsIgnoreCase(Constant.VALUE.Mode.MULTI_VERSION)) {
                return OTSMode.MULTI_VERSION;
            } else {
                throw new IllegalArgumentException("the 'mode' only support 'normal' and 'multiVersion' not '"+ modeValue +"'.");
            }
        } catch(RuntimeException e) {
            throw new OTSCriticalException("Parse 'mode' fail, " + e.getMessage(), e);
        }
    }
    
    private static List<PrimaryKeyColumn> checkAndGetPrimaryKey(
            List<PrimaryKeyColumn> pk, 
            List<PrimaryKeySchema> pkSchema,
            PrimaryKeyValue fillValue,
            String jsonKey){
        // 检查是否和PK类型一致
        List<PrimaryKeyColumn> result = new ArrayList<PrimaryKeyColumn>(pkSchema.size());
        if(pk != null) {
            if (pk.size() > pkSchema.size()) {
                throw new IllegalArgumentException("The '"+ jsonKey +"', input primary key column size more than table meta, input size: "+ pk.size() 
                        +" ,meta pk size:" + pkSchema.size());
            } else {
                //类型检查
                for (int i = 0; i < pk.size(); i++) {
                    if (pk.get(i).getValue().getType() != null) {
                        if (pk.get(i).getValue().getType() != pkSchema.get(i).getType()) {
                            throw new IllegalArgumentException(
                                    "The '"+ jsonKey +"', input primary key column type mismath table meta, input type:"+ pk.get(i).getValue().getType()  
                                    +" ,meta pk type:"+ pkSchema.get(i).getType() 
                                    +", index:" + i);
                        }
                    }
                    result.add(new PrimaryKeyColumn(pkSchema.get(i).getName(), pk.get(i).getValue()));
                }
                //填充
                for (int i = pk.size(); i < pkSchema.size(); i++) {
                    result.add(new PrimaryKeyColumn(pkSchema.get(i).getName(), fillValue));
                }
            }
        } else {
            // 用户没有填写，系统将默认将begin置为无限小
            for (PrimaryKeySchema c : pkSchema) {
                result.add(new PrimaryKeyColumn(c.getName(), fillValue));
            }
        }
        return result;
    }
    
    /**
     * 检查split的类型是否和PartitionKey一致，是否是递增序列
     * @param points
     * @param pkSchema
     */
    private static List<List<PrimaryKeyColumn>> checkAndGetSplit(
            List<List<PrimaryKeyColumn>> points, 
            List<PrimaryKeySchema> pkSchema){
        List<List<PrimaryKeyColumn>> result = new ArrayList<List<PrimaryKeyColumn>>();
        if (points == null) {
            return result;
        }
        
        // fill
        for (List<PrimaryKeyColumn> p : points) {
            result.add(checkAndGetPrimaryKey(p, pkSchema, PrimaryKeyValue.INF_MIN, Constant.KEY.Range.SPLIT));
        }
        
        // 检查是否是升序
        for (int i = 0 ; i < result.size() - 1; i++) {
            List<PrimaryKeyColumn> before = result.get(i);
            List<PrimaryKeyColumn> after = result.get(i + 1);
            if (CompareHelper.comparePrimaryKeyColumnList(before, after) != -1) { // 升序
                throw new IllegalArgumentException("In 'split', the item value is not increasing, index: " + i);
            }
        }
        
        return result;
    }
    
    /**
     * 检查Begin、End、Split 3者之间的关系是否符合预期
     * @param begin
     * @param end
     * @param split
     */
    private static void checkBeginAndEndAndSplit(
            List<PrimaryKeyColumn> begin, 
            List<PrimaryKeyColumn> end, 
            List<List<PrimaryKeyColumn>> split) {
        
        // begin < split < end
        if (split.size() > 0) { // 填写Split时
            // 分开比较，可以明确区分错误消息
            if (CompareHelper.comparePrimaryKeyColumnList(begin, split.get(0)) != -1) {
                throw new IllegalArgumentException("The 'begin' must be less than head of 'split'.");
            }
            if (CompareHelper.comparePrimaryKeyColumnList(split.get(split.size() - 1), end) != -1) {
                throw new IllegalArgumentException("tail of 'split' must be less than 'end'.");
            }
        } else { // 不填Split时
            if (CompareHelper.comparePrimaryKeyColumnList(begin, end) != -1) {
                throw new IllegalArgumentException("The 'begin' must be less than 'end'.");
            }
        }
    }
    
    public static void checkAndSetOTSRange(OTSRange range, TableMeta meta) throws OTSCriticalException {
        try {
            List<PrimaryKeySchema> pkSchema = meta.getPrimaryKeyList();
            
            // 检查是begin和end否和PK类型一致
            range.setBegin(checkAndGetPrimaryKey(range.getBegin(), pkSchema, PrimaryKeyValue.INF_MIN, Constant.KEY.Range.BEGIN));
            range.setEnd(checkAndGetPrimaryKey(range.getEnd(), pkSchema, PrimaryKeyValue.INF_MAX, Constant.KEY.Range.END));        
            range.setSplit(checkAndGetSplit(range.getSplit(), pkSchema));
            
            // 检查begin,end,split顺序是否正确
            checkBeginAndEndAndSplit(range.getBegin(), range.getEnd(), range.getSplit());
        } catch(RuntimeException e) {
            throw new OTSCriticalException("Parse 'range' fail, " + e.getMessage(), e);
        }
    }
    
    public static void checkAndSetOTSConf(OTSConf conf, TableMeta meta) throws OTSCriticalException {
        checkAndSetOTSRange(conf.getRange(), meta);
    }
}
