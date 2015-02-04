package com.alibaba.datax.plugin.writer.otswriter.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.alibaba.datax.plugin.writer.otswriter.model.OTSAttrColumn;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSPKColumn;
import com.aliyun.openservices.ots.internal.ClientException;
import com.aliyun.openservices.ots.internal.OTS;
import com.aliyun.openservices.ots.internal.OTSErrorCode;
import com.aliyun.openservices.ots.internal.OTSException;
import com.aliyun.openservices.ots.internal.model.ColumnType;
import com.aliyun.openservices.ots.internal.model.CreateTableRequest;
import com.aliyun.openservices.ots.internal.model.DeleteTableRequest;
import com.aliyun.openservices.ots.internal.model.Direction;
import com.aliyun.openservices.ots.internal.model.GetRangeRequest;
import com.aliyun.openservices.ots.internal.model.GetRangeResult;
import com.aliyun.openservices.ots.internal.model.PrimaryKey;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyColumn;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyType;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.internal.model.RangeRowQueryCriteria;
import com.aliyun.openservices.ots.internal.model.Row;
import com.aliyun.openservices.ots.internal.model.TableMeta;
import com.aliyun.openservices.ots.internal.model.TableOptions;

public class OTSHelper {
    
    /**
     * 创建表。如果表已经存在，则删除原来的表，在重新新建
     * @param ots
     * @param meta
     * @throws Exception
     */
    public static void createTableSafe(OTS ots, TableMeta meta) throws Exception {
        {
            DeleteTableRequest deleteTableRequest = new DeleteTableRequest(meta.getTableName());
            try {
                ots.deleteTable(deleteTableRequest);
            } catch (OTSException e) {
                if (!OTSErrorCode.OBJECT_NOT_EXIST.equals(e.getErrorCode())) {
                    throw e;
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
        Thread.sleep(5 * 1000);
    }
    
    public static void createTableSafe(OTS ots, String tableName, Map<String, PrimaryKeyType> pk) throws Exception {
        TableMeta meta = new TableMeta(tableName);
        for (Entry<String, PrimaryKeyType> s : pk.entrySet()) {
            meta.addPrimaryKeyColumn(s.getKey(), s.getValue());
        }
        createTableSafe(ots, meta);
    }
    
    public static void createTableSafe(OTS ots, String tableName, Map<String, PrimaryKeyType> pk, int readeCU, int writeCU) throws Exception {
        throw new RuntimeException("Unimplements");
    }
    
    public static void prepareData(
            OTS ots, 
            String tableName, 
            Map<String, PrimaryKeyType> pk,
            Map<String, ColumnType> attr, 
            long begin, 
            long rowCount, 
            double nullPercent) throws Exception {
        throw new RuntimeException("Unimplements");
    }
    
    public static List<Row> getAllData(OTS ots, OTSConf conf) throws ClientException, OTSException {
        List<Row> results = new ArrayList<Row>();
        List<PrimaryKeyColumn> begin  = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> end  = new ArrayList<PrimaryKeyColumn>();
        
        List<String> cc = new ArrayList<String>();
        
        for (OTSPKColumn col : conf.getPrimaryKeyColumn()) {
            begin.add(new PrimaryKeyColumn(col.getName(), PrimaryKeyValue.INF_MIN));
            end.add(new PrimaryKeyColumn(col.getName(), PrimaryKeyValue.INF_MAX));
        }
        for (OTSAttrColumn col : conf.getAttributeColumn()) {
            cc.add(col.getName());
        }
        
        PrimaryKey token =  new PrimaryKey(begin);
        do {
            RangeRowQueryCriteria cur = new RangeRowQueryCriteria(conf.getTableName());
            cur.setDirection(Direction.FORWARD);
            cur.addColumnsToGet(cc);
            cur.setInclusiveStartPrimaryKey(token);
            cur.setExclusiveEndPrimaryKey(new PrimaryKey(end));
            cur.setMaxVersions(Integer.MAX_VALUE);
            
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(cur);
            
            GetRangeResult result = ots.getRange(request);
            token = result.getNextStartPrimaryKey();
            results.addAll(result.getRows());
        } while (token != null);
        return results;
    }
}
