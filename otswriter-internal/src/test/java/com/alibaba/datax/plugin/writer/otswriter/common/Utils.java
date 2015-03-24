package com.alibaba.datax.plugin.writer.otswriter.common;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.otswriter.OTSCriticalException;
import com.alibaba.datax.plugin.writer.otswriter.utils.Common;
import com.aliyun.openservices.ots.internal.OTS;
import com.aliyun.openservices.ots.internal.OTSClient;
import com.aliyun.openservices.ots.internal.model.ColumnType;
import com.aliyun.openservices.ots.internal.model.ColumnValue;
import com.aliyun.openservices.ots.internal.model.PrimaryKeySchema;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyType;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.internal.model.TableMeta;

public class Utils {
    public static Configuration loadConf() {
        String path = "src/test/resources/conf.json";
        InputStream f;
        try {
            f = new FileInputStream(path);
            Configuration p = Configuration.from(f);
            return p;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    } 
    
    public static OTS getOTSClient() {
        Configuration p = Utils.loadConf();
        OTS ots = new OTSClient(
                p.getString("endpoint"), 
                p.getString("accessid"), 
                p.getString("accesskey"), 
                p.getString("instance-name"));
        return ots;
    }
    
    public static Column getPKColumn(PrimaryKeyType type, int value) throws UnsupportedEncodingException {
        switch (type) {
        case INTEGER:
            return new LongColumn(value);
        case STRING:
            return new StringColumn(String.valueOf(value));
        case BINARY:
            return new BytesColumn(("" + value ).getBytes("UTF-8"));
        default:
            break;
        }
        return null;
    }
    
    public static Column getAttrColumn(ColumnType type, int value) throws Exception {
        switch (type) {
        case BINARY:
            return new BytesColumn(("" + value ).getBytes("UTF-8"));
        case BOOLEAN:
            return new BoolColumn(value%2 == 0 ? true : false);
        case DOUBLE:
            return new DoubleColumn(value);
        case INTEGER:
            return new LongColumn(value);
        case STRING:
            return new StringColumn(String.valueOf(value));
        default:
            break;
        }
        return null;
    }
    
    public static PrimaryKeyValue getPKColumnValue(PrimaryKeyType type, int value) throws UnsupportedEncodingException {
        switch (type) {
        case INTEGER:
            return PrimaryKeyValue.fromLong(value);
        case STRING:
            return PrimaryKeyValue.fromString(String.valueOf(value));
        case BINARY:
            return PrimaryKeyValue.fromBinary(("" + value ).getBytes("UTF-8"));
        default:
            break;
        }
        return null;
    }
    
    public static ColumnValue getAttrColumnValue(ColumnType type, int value) throws UnsupportedEncodingException {
        switch (type) {
        case BINARY:
            return ColumnValue.fromBinary(("" + value ).getBytes("UTF-8"));
        case BOOLEAN:
            return ColumnValue.fromBoolean(value%2 == 0 ? true : false);
        case DOUBLE:
            return ColumnValue.fromDouble(value);
        case INTEGER:
            return ColumnValue.fromLong(value);
        case STRING:
            return ColumnValue.fromString(String.valueOf(value));
        default:
            break;
        }
        return null;
    }
    
    public static Map<String, Integer> getPkColumnMapping(List<PrimaryKeySchema> pks) throws OTSCriticalException {
        TableMeta meta = new TableMeta("xx");
        for (PrimaryKeySchema p : pks) {
            meta.addPrimaryKeyColumn(p.getName(), p.getType());
        }
        return Common.getEncodePkColumnMapping(meta, pks);
    }
}
