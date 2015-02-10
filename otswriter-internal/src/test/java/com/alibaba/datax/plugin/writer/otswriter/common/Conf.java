package com.alibaba.datax.plugin.writer.otswriter.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSAttrColumn;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSMode;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSOpType;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf.RestrictConf;
import com.aliyun.openservices.ots.internal.model.ColumnType;
import com.aliyun.openservices.ots.internal.model.PrimaryKeySchema;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyType;

public class Conf {
    
    private static Configuration p = Utils.loadConf();
    
    public static OTSConf getConf(
            String tableName, 
            Map<String, PrimaryKeyType> pk, 
            Map<String, ColumnType> attr, 
            OTSOpType op,
            OTSMode mode) {
        OTSConf conf = new OTSConf();
        conf.setEndpoint(p.getString("endpoint"));
        conf.setAccessId(p.getString("accessid"));
        conf.setAccessKey(p.getString("accesskey"));
        conf.setInstanceName(p.getString("instance-name"));
        conf.setTableName(tableName);

        List<PrimaryKeySchema> primaryKeyColumn = new ArrayList<PrimaryKeySchema>();
        for (Entry<String, PrimaryKeyType> en : pk.entrySet()) {
            primaryKeyColumn.add(new PrimaryKeySchema(en.getKey(), en.getValue()));
        }
        conf.setPrimaryKeyColumn(primaryKeyColumn);

        List<OTSAttrColumn> attributeColumn = new ArrayList<OTSAttrColumn>();
        for (Entry<String, ColumnType> en : attr.entrySet()) {
            attributeColumn.add(new OTSAttrColumn(en.getKey(), en.getKey(), en.getValue()));
        }
        conf.setAttributeColumn(attributeColumn);

        conf.setOperation(op);
        
        conf.setMode(mode);
        
        conf.setTimestamp(-1);

        conf.setRetry(18);
        conf.setSleepInMillisecond(100);
        conf.setBatchWriteCount(100);
        conf.setConcurrencyWrite(5);
        conf.setIoThreadCount(1);
        conf.setSocketTimeoutInMillisecond(60000);
        conf.setConnectTimeoutInMillisecond(60000);
        
        RestrictConf restrictConf = conf.new RestrictConf();
        restrictConf.setRequestTotalSizeLimitation(1024*1024);
        restrictConf.setRowCellCountLimitation(128);
        conf.setRestrictConf(restrictConf);
        return conf;
    }

    /**
     * TODO remove
     * @param tableName
     * @param pk
     * @param attr
     * @param op
     * @return
     */
    public static OTSConf getConf(
            String tableName, 
            Map<String, PrimaryKeyType> pk, 
            Map<String, ColumnType> attr, 
            OTSOpType op) {

        OTSConf conf = new OTSConf();
        conf.setEndpoint(p.getString("endpoint"));
        conf.setAccessId(p.getString("accessid"));
        conf.setAccessKey(p.getString("accesskey"));
        conf.setInstanceName(p.getString("instance-name"));
        conf.setTableName(tableName);

        List<PrimaryKeySchema> primaryKeyColumn = new ArrayList<PrimaryKeySchema>();
        for (Entry<String, PrimaryKeyType> en : pk.entrySet()) {
            primaryKeyColumn.add(new PrimaryKeySchema(en.getKey(), en.getValue()));
        }
        conf.setPrimaryKeyColumn(primaryKeyColumn);

        List<OTSAttrColumn> attributeColumn = new ArrayList<OTSAttrColumn>();
        for (Entry<String, ColumnType> en : attr.entrySet()) {
            attributeColumn.add(new OTSAttrColumn(en.getKey(), en.getKey(), en.getValue()));
        }
        conf.setAttributeColumn(attributeColumn);

        conf.setOperation(op);
        conf.setMode(OTSMode.NORMAL);
        
        conf.setTimestamp(-1);

        conf.setRetry(18);
        conf.setSleepInMillisecond(100);
        conf.setBatchWriteCount(100);
        conf.setConcurrencyWrite(5);
        conf.setIoThreadCount(1);
        conf.setSocketTimeoutInMillisecond(60000);
        conf.setConnectTimeoutInMillisecond(60000);
        
        RestrictConf restrictConf = conf.new RestrictConf();
        restrictConf.setRequestTotalSizeLimitation(1024*1024);
        conf.setRestrictConf(restrictConf);
        return conf;
    }
}
