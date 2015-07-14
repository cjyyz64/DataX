package com.alibaba.datax.plugin.reader.otsreader.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSErrorCode;
import com.aliyun.openservices.ots.internal.OTS;
import com.aliyun.openservices.ots.internal.OTSClient;
import com.aliyun.openservices.ots.internal.OTSException;
import com.aliyun.openservices.ots.internal.model.ColumnType;
import com.aliyun.openservices.ots.internal.model.ColumnValue;
import com.aliyun.openservices.ots.internal.model.CreateTableRequest;
import com.aliyun.openservices.ots.internal.model.DeleteTableRequest;
import com.aliyun.openservices.ots.internal.model.ListTableResult;
import com.aliyun.openservices.ots.internal.model.PrimaryKey;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyColumn;
import com.aliyun.openservices.ots.internal.model.PrimaryKeySchema;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyType;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.internal.model.RowUpdateChange;
import com.aliyun.openservices.ots.internal.model.TableMeta;
import com.aliyun.openservices.ots.internal.model.TableOptions;
import com.aliyun.openservices.ots.internal.model.UpdateRowRequest;

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
            ListTableResult result = ots.listTable();
            for (String table : result.getTableNames()) {
                try {
                    DeleteTableRequest deleteTableRequest = new DeleteTableRequest(table);
                    ots.deleteTable(deleteTableRequest);
                } catch (OTSException e) {
                    if (!OTSErrorCode.OBJECT_NOT_EXIST.equals(e.getErrorCode())) {
                        throw new RuntimeException(e.getMessage(), e);
                    }
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
    
    private static PrimaryKeyValue getPKV(PrimaryKeyType type, int value) {
        switch(type) {
            case BINARY:
                return PrimaryKeyValue.fromBinary(String.format("%06d", value).getBytes());
            case INTEGER:
                return PrimaryKeyValue.fromLong(value);
            case STRING:
                return PrimaryKeyValue.fromString(String.format("%06d", value));
            default:
                break;
                
        }
        return null;
    }
    
    private static ColumnValue getCV(ColumnType type, int value) {
        switch(type) {
            case BINARY:
                return ColumnValue.fromBinary(String.format("%06d", value).getBytes());
            case BOOLEAN:
                return ColumnValue.fromBoolean(value / 2 == 0 ? true : false);
            case DOUBLE:
                return ColumnValue.fromDouble(value);
            case INTEGER:
                return ColumnValue.fromLong(value);
            case STRING:
                return ColumnValue.fromString(String.format("%06d", value));
            default:
                break;
        }
        return null;
    }
    
    public static void prepareData(OTS ots, TableMeta meta, int range_begin, int range_end, int column_begin, int column_end) {
        List<PrimaryKeySchema> pks = meta.getPrimaryKeyList();
        // update
        for (int i = range_begin; i < range_end; i++ ) {
            List<PrimaryKeyColumn> pk = new ArrayList<PrimaryKeyColumn>();
            for (PrimaryKeySchema c : pks) {
                pk.add(new PrimaryKeyColumn(c.getName(), getPKV(c.getType(), i)));
            }
            RowUpdateChange rowChange = new RowUpdateChange(meta.getTableName(), new PrimaryKey(pk));
            for (int j = column_begin; j < column_end; j++) {
                Random r = new Random();
                int v = r.nextInt(5);
                if (v == 0) {
                    rowChange.put(String.format("col_%05d", j), getCV(ColumnType.STRING, i));
                } else if (v == 1) {
                    rowChange.put(String.format("col_%05d", j), getCV(ColumnType.INTEGER, i));
                } else if (v == 2) {
                    rowChange.put(String.format("col_%05d", j), getCV(ColumnType.BINARY, i));
                } else if (v == 3) {
                    rowChange.put(String.format("col_%05d", j), getCV(ColumnType.BOOLEAN, i));
                } else if (v == 4) {
                    rowChange.put(String.format("col_%05d", j), getCV(ColumnType.DOUBLE, i));
                }
            }
            UpdateRowRequest updateRowRequest = new UpdateRowRequest();
            updateRowRequest.setRowChange(rowChange);
            ots.updateRow(updateRowRequest);
        }
    }
}
