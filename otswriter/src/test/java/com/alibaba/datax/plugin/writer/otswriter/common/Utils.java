package com.alibaba.datax.plugin.writer.otswriter.common;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Column.Type;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.plugin.writer.otswriter.callable.CreateTableCallable;
import com.alibaba.datax.plugin.writer.otswriter.callable.DeleteTableCallable;
import com.alibaba.datax.plugin.writer.otswriter.common.TestPluginCollector.RecordAndMessage;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSAttrColumn;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSPKColumn;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSRowPrimaryKey;
import com.alibaba.datax.plugin.writer.otswriter.utils.RetryHelper;
import com.aliyun.openservices.ots.OTSClient;
import com.aliyun.openservices.ots.model.CapacityUnit;
import com.aliyun.openservices.ots.model.ColumnType;
import com.aliyun.openservices.ots.model.ColumnValue;
import com.aliyun.openservices.ots.model.CreateTableRequest;
import com.aliyun.openservices.ots.model.DeleteTableRequest;
import com.aliyun.openservices.ots.model.Direction;
import com.aliyun.openservices.ots.model.GetRangeRequest;
import com.aliyun.openservices.ots.model.GetRangeResult;
import com.aliyun.openservices.ots.model.MockOTSClient;
import com.aliyun.openservices.ots.model.OTSRow;
import com.aliyun.openservices.ots.model.PrimaryKeyType;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.model.RangeRowQueryCriteria;
import com.aliyun.openservices.ots.model.Row;
import com.aliyun.openservices.ots.model.RowPrimaryKey;
import com.aliyun.openservices.ots.model.TableMeta;

public class Utils {
    
    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);
    
    public static String getRowPKString(Map<String, PrimaryKeyValue> pk) {
        Set<String> keys = pk.keySet();
        StringBuilder sb = new StringBuilder();
        for (String key:keys) {
            PrimaryKeyValue value = pk.get(key);
            sb.append("[");
            if (value.equals(PrimaryKeyValue.INF_MIN)) {
                sb.append("("+ key +")INF_MIN");
            } else if (value.equals(PrimaryKeyValue.INF_MAX)) {
                sb.append("("+ key +")INF_MAX");
            } else if (value.getType() == PrimaryKeyType.INTEGER) {
                sb.append("("+ key +")INTEGER.");
                sb.append(value.asLong());
            }else {
                sb.append("("+ key +")STRING.");
                sb.append(value.asString());
                sb.append("=");
                sb.append(value.asString().length());
            }
            sb.append("]");
            sb.append(",");
        }
        return sb.toString();
    }
    
    public static String getRowPKString(RowPrimaryKey pk) {
        return getRowPKString(pk.getPrimaryKey());
    }
    
    public static String getRowString(Row row) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (Entry<String, ColumnValue> en : row.getColumns().entrySet()) {
            sb.append(en.getKey() + "=" +en.getValue().toString() +",");
        }
        sb.append("]");
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    public static Configuration parseConf(String jobConfPath) {
        Configuration root = ConfigParser.parseJobConfig(jobConfPath);
        Map<String, Object> job = root.getMap("job");
        List<Map<String, Object>> conetent = (List<Map<String, Object>>) job.get("content");
        Map<String, Object> reader = (Map<String, Object>) conetent.get(0).get("reader");
        Map<String, Object> parameters = (Map<String, Object>) reader.get("parameter");
        return Configuration.from(parameters);
    }

    public static void createTable(OTSClient ots, String tableName, TableMeta tableMeta) throws Exception{
        // drop table
        DeleteTableRequest deleteTableRequest = new DeleteTableRequest();
        deleteTableRequest.setTableName(tableName);
        try {
            RetryHelper.executeWithRetry(
                    new DeleteTableCallable(ots, deleteTableRequest),
                    2,
                    1000
                    );
        } catch (Exception e) {
            //e.printStackTrace();
        }

        Thread.sleep(5 * 1000);

        // create table
        CapacityUnit capacityUnit = new CapacityUnit(5000, 5000);
        CreateTableRequest createTableRequest = new CreateTableRequest();
        createTableRequest.setTableMeta(tableMeta);
        createTableRequest.setReservedThroughput(capacityUnit);
        RetryHelper.executeWithRetry(
                new CreateTableCallable(ots, createTableRequest),
                5,
                1000
                );

        Thread.sleep(10000);
    }

    public static Configuration loadConf() {
        String path = "src/test/resources/conf.json";
        InputStream f;
        try {
            f = new FileInputStream(path);
            Configuration p = Configuration.from(f);
            return p;
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    } 

    public static int compareBytes(byte[] b1, byte[] b2) {
        int size = b1.length < b2.length ? b1.length : b2.length;
        for (int i = 0; i < size; i++) {
            int r = b1[i] - b2[i];
            if (r != 0) {
                return r;
            }
        }
        if (b1.length > b2.length) {
            return 1;
        } else if (b1.length < b2.length){
            return -1;
        } else {
            return 0;
        }
    }
    
    public static String getRecordString(Record r) {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < r.getColumnNumber(); i++) {
            Column c = r.getColumn(i);
            if (c == null) {
                sb.append("N/A, ");
            } else {
                switch (c.getType()) {
                case STRING:  sb.append(c.asString() + "(STRING), "); break;
                case LONG:  sb.append(c.asLong() + "(LONG), "); break;
                case DOUBLE:  sb.append(c.asDouble() + "(DOUBLE), "); break;
                case BOOL:  sb.append(c.asBoolean() + "(BOOL), "); break;
                case BYTES: sb.append("(BYTES), "); ; break;
                default:
                    throw new RuntimeException("Unsupport the type.");
                }
            }
            
        }
        return sb.toString();
    }
    
    public static String getDataConfigJson(OTSConf conf) {
        String json = 
                "{\"job\":{\"setting\":{\"speed\":10}, \"content\":["
                        + "{\"writer\":{\"name\":\"otswriter\", \"parameter\":"+ TestGsonParser.confToJson(conf) +"}, "
                        + "\"reader\":{}}]}}";
        return json;
    }
    
    private static List<Row> getData(OTSClient ots, OTSConf conf) {
        List<Row> results = new ArrayList<Row>();
        RowPrimaryKey begin  = new RowPrimaryKey();
        RowPrimaryKey end  = new RowPrimaryKey();
        
        List<String> cc = new ArrayList<String>();
        
        for (OTSPKColumn col : conf.getPrimaryKeyColumn()) {
            begin.addPrimaryKeyColumn(col.getName(), PrimaryKeyValue.INF_MIN);
            end.addPrimaryKeyColumn(col.getName(), PrimaryKeyValue.INF_MAX);
            cc.add(col.getName());
        }
        for (OTSAttrColumn col : conf.getAttributeColumn()) {
            cc.add(col.getName());
        }
        
        RowPrimaryKey token =  begin;
        do {
            RangeRowQueryCriteria cur = new RangeRowQueryCriteria(conf.getTableName());
            cur.setDirection(Direction.FORWARD);
            cur.setColumnsToGet(cc);
            cur.setLimit(-1);
            cur.setInclusiveStartPrimaryKey(token);
            cur.setExclusiveEndPrimaryKey(end);

            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(cur);

            GetRangeResult result = ots.getRange(request);
            token = result.getNextStartPrimaryKey();
            results.addAll(result.getRows());
        } while (token != null);
        return results;
    }
    
    private static String getPKValue(OTSConf conf, Map<String, ColumnValue> row) {
        List<OTSPKColumn> pks = conf.getPrimaryKeyColumn();
        StringBuilder sb = new StringBuilder();
        for (OTSPKColumn pk : pks) {
            sb.append(row.get(pk.getName()) + ",");
        }
        return sb.toString();
    }
    
    private static Map<String, Map<String, ColumnValue>> mapping(OTSConf conf, List<Map<String, ColumnValue>> input) {
        Map<String, Map<String, ColumnValue>> m = new HashMap<String, Map<String, ColumnValue>>();
        
        for (Map<String, ColumnValue> row : input) {
            m.put(getPKValue(conf, row), row);
        }
        
        return m;
    }
    
    private static boolean cmpRow(Map<String, ColumnValue> src, Map<String, ColumnValue> target) {
        if (src.size() != target.size()) {
            LOG.error("src size({}) not equal target size({}).", src.size(), target.size());
            return false;
        }
        for (Entry<String, ColumnValue> entry : src.entrySet()) {
            if (!target.containsKey(entry.getKey())) {
                LOG.error("{} is exist.", entry.getKey());
                return false;
            }
            ColumnValue targetValue = target.get(entry.getKey());
            ColumnValue srcValue = entry.getValue();
            
            if (targetValue == null && srcValue == null) {
                continue;
            } else if (targetValue == null && srcValue != null) {
                LOG.error("targetValue is null, but src is not null");
                return false;
            } else if (targetValue != null && srcValue == null) {
                LOG.error("srcValue is null, but targetValue is not null.");
                return false;
            }
            
            if (srcValue.getType() == ColumnType.BINARY && targetValue.getType() == ColumnType.BINARY) {
                if (compareBytes(srcValue.asBinary(), targetValue.asBinary()) != 0) {
                    LOG.error("Binary not equal.");
                    return false;
                }
            } else {
                if (!srcValue.equals(targetValue)) {
                    LOG.error("{} not equal, v1={}, v2={}", new String[] {entry.getKey(), targetValue.toString(), srcValue.toString()});
                    return false;
                }
            }
        }
        return true;
    }
    
    private static boolean cmpRecord(Record src, Record target) {
        if (src.getColumnNumber() != target.getColumnNumber()) {
            LOG.error("src size({}) not equal target size({}).", src.getColumnNumber() , target.getColumnNumber());
            return false;
        }
        for (int i = 0; i < src.getColumnNumber(); i++) {
            Column srcValue = src.getColumn(i);
            Column targetValue = target.getColumn(i);

            if (srcValue.getType() == Type.BYTES && targetValue.getType() == Type.BYTES) {
                if (compareBytes(srcValue.asBytes(), targetValue.asBytes()) != 0) {
                    LOG.error("Binary not equal.");
                    return false;
                }
            } else {
                if (srcValue.getType() != targetValue.getType()) {
                    LOG.error("targetValue type({}) not equal srcValue type({}), . index({}) in record.", new Object[] {targetValue.getType(), srcValue.getType(), i});
                    return false;
                } else {
                    
                    if (srcValue.getRawData() == null || targetValue.getRawData() == null) {
                        if (!(srcValue.getRawData() == null && targetValue.getRawData() == null)) {
                            LOG.error("targetValue({}) not equal srcValue({}), . index({}) in record.", new Object[] {targetValue.asDouble(), srcValue.asDouble(), i});
                            return false;
                        }
                    } else {
                        switch (srcValue.getType()) {
                            case BOOL:
                                if (srcValue.asBoolean().booleanValue() != targetValue.asBoolean().booleanValue()) {
                                    LOG.error("targetValue({}) not equal srcValue({}), . index({}) in record.", new Object[] {targetValue.asBoolean(), srcValue.asBoolean(), i});
                                    return false;
                                }
                                break;
                            case DOUBLE:
                                if (srcValue.asDouble().doubleValue() != targetValue.asDouble().doubleValue()) {
                                    LOG.error("targetValue({}) not equal srcValue({}), . index({}) in record.", new Object[] {targetValue.asDouble(), srcValue.asDouble(), i});
                                    return false;
                                }
                                break;
                            case LONG:
                                if (srcValue.asLong().longValue() != targetValue.asLong().longValue()) {
                                    LOG.error("targetValue({}) not equal srcValue({}), . index({}) in record.", new Object[] {targetValue.asLong(), srcValue.asLong(), i});
                                    return false;
                                }
                                break;
                            case STRING:
                                if (!srcValue.asString().equals(targetValue.asString())) {
                                    LOG.error("targetValue({}) not equal srcValue({}), . index({}) in record.", new Object[] {targetValue.asString(), srcValue.asString(), i});
                                    return false;
                                }
                                break;
                            default:
                                break;
                            
                        }
                    }
                }
            }
        }
        return true;
    }
    
    public static boolean checkInput(List<Record> expect, List<Record> src) {
        if (src.size() != expect.size()) {
            LOG.error("Expect size({}) not equal size({}) of src.", expect.size(), src.size());
            return false;
        }
        int size = src.size();
        for (int i = 0; i < size; i++) {
            Record expectRecord = expect.get(i);
            Record srcRecord = src.get(i);
            
            if (expectRecord.getColumnNumber() != srcRecord.getColumnNumber()) {
                LOG.error("Expect number({}) not equal number({}) of src, Index : {}", new Object[]{expectRecord.getColumnNumber(), srcRecord.getColumnNumber(), i});
                return false;
            }
            
            int number = expectRecord.getColumnNumber();
            for (int j = 0; j < number; j++) {
                if (!cmpRecord(srcRecord, expectRecord)) {
                    return false;
                }
            }
        }
        return true;
    }
    
    public static boolean checkInputWithMessage(List<RecordAndMessage> expect, List<RecordAndMessage> src) {
        if (src.size() != expect.size()) {
            LOG.error("Expect size({}) not equal size({}) of src.", expect.size(), src.size());
            return false;
        }
        int size = src.size();
        for (int i = 0; i < size; i++) {
            Record expectRecord = expect.get(i).getDirtyRecord();
            Record srcRecord = src.get(i).getDirtyRecord();
            
            if (expectRecord.getColumnNumber() != srcRecord.getColumnNumber()) {
                LOG.error("Expect number({}) not equal number({}) of src, Index : {}", new Object[]{expectRecord.getColumnNumber(), srcRecord.getColumnNumber(), i});
                return false;
            }
            
            int number = expectRecord.getColumnNumber();
            for (int j = 0; j < number; j++) {
                if (!cmpRecord(srcRecord, expectRecord)) {
                    return false;
                }
            }
            
            String expectMsg = expect.get(i).getErrorMessage();
            String srcMsg = src.get(i).getErrorMessage();
            
            if (!expectMsg.equals(srcMsg)) {
                LOG.error("Expect Message({}) not equal Message({}) of src, Index : {}", new Object[]{expectMsg, srcMsg, i});
                return false;
            }
        }
        return true;
    }
    
    /**
     * 
     * @param ots
     * @param conf
     * @param expect 用户期望的行
     * @return
     */
    public static boolean checkInput(
            OTSClient ots, 
            OTSConf conf,
            List<Map<String, ColumnValue>> expect) {
        List<Row> rows = getData(ots, conf);

        if (expect.size() != rows.size()) {
            LOG.error("Expect size not equal size in ots, expect size : {}, size in ots : {}", expect.size(), rows.size());
            return false;
        }
        
        Map<String, Map<String, ColumnValue>> m = mapping(conf, expect);
        
        for (Row row : rows) {
            String key = getPKValue(conf, row.getColumns());
            if (!m.containsKey(key)) {
                LOG.error("Can not get value ('{}') from expect inputs.", key);
                return false;
            }
            LOG.info("\nExpect : {} \nRow    : {}", m.get(key), row.getColumns());
            if (!cmpRow(m.get(key), row.getColumns())){
                return false;
            }
        }
        return true;
    }
    
    public static boolean checkInput(
            MockOTSClient ots, 
            List<OTSRow> expect
            ) {
        Map<OTSRowPrimaryKey, Row> rows = ots.getData();
        if (rows.size() != expect.size()) {
            LOG.error("Expect size not equal size in ots, expect size : {}, size in ots : {}", expect.size(), rows.size());
            return false;
        } 
        
        for (OTSRow or : expect) {
            Row r = rows.get(or.getPK());
            if (r == null) {
                LOG.error("Can not get row ('{}') from ots.", getRowPKString(or.getPK().getColumns()));
                return false;
            }
            //
            if (!cmpRow(r.getColumns(), or.getRow().getColumns())){
                return false;
            }
        }
        
        return true;
    }
    
    public static boolean checkRows(List<Integer> expect, List<Integer> src) {
        if (expect.size() != src.size()) {
            LOG.error("Expect size not equal size in ots, expect size : {}, size in ots : {}", expect.size(), src.size());
            return false;
        }
        
        for (int i = 0; i < expect.size(); i++) {
            if (expect.get(i) != src.get(i)) {
                LOG.error("Expect({}) not equal src({}), index : {}", new Object[]{expect.get(i), src.get(i), i});
                return false;
            }
        }
        return true;
    }
    
    public static Column getPKColumn(PrimaryKeyType type, int value) {
        switch (type) {
        case INTEGER:
            return new LongColumn(value);
        case STRING:
            return new StringColumn(String.valueOf(value));
        default:
            break;
        }
        return null;
    }
    
    public static Column getAttrColumn(ColumnType type, int value) throws Exception {
        switch (type) {
        case BINARY:
            return new BytesColumn("hello".getBytes("UTF-8"));
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
    
    public static ColumnValue getPKColumnValue(PrimaryKeyType type, int value) {
        switch (type) {
        case INTEGER:
            return ColumnValue.fromLong(value);
        case STRING:
            return ColumnValue.fromString(String.valueOf(value));
        default:
            break;
        }
        return null;
    }
    
    public static ColumnValue getAttrColumnValue(ColumnType type, int value) throws Exception {
        switch (type) {
        case BINARY:
            return ColumnValue.fromBinary("hello".getBytes("UTF-8"));
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
    
    public static List<PrimaryKeyType> getPKTypeList(List<OTSPKColumn> pks) {
        List<PrimaryKeyType> types = new ArrayList<PrimaryKeyType>();
        for (OTSPKColumn pk : pks) {
            types.add(pk.getType());
        }
        return types;
    }
}
