package com.alibaba.datax.plugin.reader.otsreader.common;

import java.util.Map;
import java.util.Map.Entry;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSErrorCode;
import com.aliyun.openservices.ots.internal.OTS;
import com.aliyun.openservices.ots.internal.OTSClient;
import com.aliyun.openservices.ots.internal.OTSException;
import com.aliyun.openservices.ots.internal.model.CreateTableRequest;
import com.aliyun.openservices.ots.internal.model.DeleteTableRequest;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyType;
import com.aliyun.openservices.ots.internal.model.TableMeta;
import com.aliyun.openservices.ots.internal.model.TableOptions;

public class OtsHelper {
    
    public static OTS getOTSInstance() {
        Configuration p = ConfigurationHelper.loadConf();
        OTS ots = new OTSClient(
                p.getString("endpoint"), 
                p.getString("accessid"), 
                p.getString("accesskey"), 
                p.getString("instance-name"));
        return ots;
    }
    
    /**
     * 创建表。如果表已经存在，则删除原来的表，在重新新建
     * @param ots
     * @param meta
     * @throws Exception
     */
    public static void createTableSafe(OTS ots, TableMeta meta) {
        {
            DeleteTableRequest deleteTableRequest = new DeleteTableRequest(meta.getTableName());
            try {
                ots.deleteTable(deleteTableRequest);
            } catch (OTSException e) {
                if (!OTSErrorCode.OBJECT_NOT_EXIST.equals(e.getErrorCode())) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            }
        }
        {
            TableOptions tableOptions = new TableOptions();
            tableOptions.setMaxVersions(Integer.MAX_VALUE);
            tableOptions.setTimeToLive(-1);
            
            CreateTableRequest createTableRequest = new CreateTableRequest(meta);
            createTableRequest.setTableOptions(tableOptions);
            ots.createTable(createTableRequest);
        }
        try {
            Thread.sleep(5 * 1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
    
    public static void createTableSafe(OTS ots, String tableName, Map<String, PrimaryKeyType> pk) {
        TableMeta meta = new TableMeta(tableName);
        for (Entry<String, PrimaryKeyType> s : pk.entrySet()) {
            meta.addPrimaryKeyColumn(s.getKey(), s.getValue());
        }
        createTableSafe(ots, meta);
    }
}
