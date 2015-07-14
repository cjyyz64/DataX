package com.alibaba.datax.plugin.reader.otsreader.common;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.Constant;
import com.alibaba.datax.plugin.reader.otsreader.Key;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSMode;
import com.aliyun.openservices.ots.internal.utils.Base64;

public class ConfigurationHelper {
    
    public static void setDefaultConfig(Configuration param) {
        param.set(Constant.ConfigKey.RETRY, "20");
        param.set(Constant.ConfigKey.RETRY_PAUSE_IN_MILLISECOND, "120");
        param.set(Constant.ConfigKey.IO_THREAD_COUNT, "2");
        param.set(Constant.ConfigKey.MAX_CONNECTION_COUNT, "2");
        param.set(Constant.ConfigKey.SOCKET_TIMEOUTIN_MILLISECOND, "20000");
        param.set(Constant.ConfigKey.CONNECT_TIMEOUT_IN_MILLISECOND, "20000");
    }
    
    public static void setRange(Configuration param) {
        Map<String, Object> range = new LinkedHashMap<String, Object>();
        
        // "begin":[{"type":"string", "value":"a"},{"type":"INF_MIN", "value":""}],
        // "end":[{"type":"string", "value":"c"},{"type":"INF_MAX", "value":""}],
        // "split":[{"type":"string", "value":"b"}]
        {
            List<Map<String, Object>> pks = new ArrayList<Map<String, Object>>();
            {
                Map<String, Object> column = new LinkedHashMap<String, Object>();
                column.put("type", "string");
                column.put("value", "a");
                pks.add(column);
            }
            {
                Map<String, Object> column = new LinkedHashMap<String, Object>();
                column.put("type", "INF_MIN");
                pks.add(column);
            }
            range.put("begin", pks);
        }
        {
            List<Map<String, Object>> pks = new ArrayList<Map<String, Object>>();
            {
                Map<String, Object> column = new LinkedHashMap<String, Object>();
                column.put("type", "string");
                column.put("value", "c");
                pks.add(column);
            }
            {
                Map<String, Object> column = new LinkedHashMap<String, Object>();
                column.put("type", "INF_MAX");
                pks.add(column);
            }
            range.put("end", pks);
        }
        {
            List<Map<String, Object>> pks = new ArrayList<Map<String, Object>>();
            {
                Map<String, Object> column = new LinkedHashMap<String, Object>();
                column.put("type", "string");
                column.put("value", "b");
                pks.add(column);
            }
            range.put("split", pks);
        }
        
        param.set(Key.RANGE, range);
    }
    

    
    public static List<Map<String, Object>> getNormalColumn() {
        List<Map<String, Object>> columns = new ArrayList<Map<String, Object>>();
        {
            Map<String, Object> column = new LinkedHashMap<String, Object>();
            column.put("name", "pk1");
            columns.add(column);
        }
        {
            Map<String, Object> column = new LinkedHashMap<String, Object>();
            column.put("name", "pk2");
            columns.add(column);
        }
        {
            Map<String, Object> column = new LinkedHashMap<String, Object>();
            column.put("name", "attr1");
            columns.add(column);
        }
        return columns;
    }
    
    public static List<Map<String, Object>> getConstColumn() {
        /**
         *      {"type":"string","value" : "string_value"} 
                {"type":"int","value" : "-10"}
                {"type":"double","value" : "10.001"}
                {"type":"binary","value" : "base64()"}
         */
        List<Map<String, Object>> columns = new ArrayList<Map<String, Object>>();
        {
            Map<String, Object> column = new LinkedHashMap<String, Object>();
            column.put("type", "string");
            column.put("value", "string_value");
            columns.add(column);
        }
        {
            Map<String, Object> column = new LinkedHashMap<String, Object>();
            column.put("type", "int");
            column.put("value", "-10");
            columns.add(column);
        }
        {
            Map<String, Object> column = new LinkedHashMap<String, Object>();
            column.put("type", "double");
            column.put("value", "10.001");
            columns.add(column);
        }
        {
            Map<String, Object> column = new LinkedHashMap<String, Object>();
            column.put("type", "bool");
            column.put("value", "true");
            columns.add(column);
        }
        {
            Map<String, Object> column = new LinkedHashMap<String, Object>();
            column.put("type", "binary");
            column.put("value", Base64.toBase64String("hello".getBytes()));
            columns.add(column);
        }
        return columns;
    }
    
    public static void setColumnConfig(
            Configuration param, 
            List<Map<String, Object>> normalColumn, 
            List<Map<String, Object>> constColumn) {
        List<Map<String, Object>> columns = new ArrayList<Map<String, Object>>();
        
        if (normalColumn != null) {
            columns.addAll(normalColumn);
        }
        if (constColumn != null) {
            columns.addAll(constColumn);
        }
        
        param.set(Key.COLUMN, columns);
    }
    
    public static void setTimeRange(Configuration param, Long begin, Long end) {
        // TimeRange
        Map<String, Object> timeRange = new LinkedHashMap<String, Object>();
        if (begin != null) {
            timeRange.put(Constant.ConfigKey.TimeRange.BEGIN, begin);
        }
        if (end != null) {
            timeRange.put(Constant.ConfigKey.TimeRange.END, end);
        }
        param.set(Constant.ConfigKey.TIME_RANGE, timeRange);
    }
    
    public static void setMaxVersion(Configuration param, Integer maxVersion) {
        // MaxVersion
        param.set(Constant.ConfigKey.MAX_VERSION, maxVersion);
    }
    
    public static Configuration getDefaultConfiguration(OTSMode mode) {
        Configuration param = Configuration.newDefault();
        param.set(Key.OTS_ENDPOINT, " endpoint ");
        param.set(Key.OTS_ACCESSID, " accessid ");
        param.set(Key.OTS_ACCESSKEY, " accesskey ");
        param.set(Key.OTS_INSTANCE_NAME, " instancename ");
        param.set(Key.TABLE_NAME, " tablename ");
        if (mode == OTSMode.MULTI_VERSION) {
            param.set(Key.MODE, Constant.ConfigDefaultValue.Mode.MULTI_VERSION);
        } else {
            param.set(Key.MODE, Constant.ConfigDefaultValue.Mode.NORMAL);
        }
        
        param.set(Key.COLUMN, Collections.EMPTY_LIST);
        return param;
    }
    
    //
    
    public static Configuration loadConf() {
        String path = "src/test/resources/conf.json";
        InputStream f;
        try {
            f = new FileInputStream(path);
            Configuration p = Configuration.from(f);
            return p;
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    } 
}
