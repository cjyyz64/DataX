package com.alibaba.datax.plugin.reader.otsreader.common;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSRange;
import com.alibaba.datax.plugin.reader.otsreader.utils.ParamChecker;
import com.alibaba.datax.plugin.reader.otsreader.utils.Common;
import com.aliyun.openservices.ots.OTSClient;
import com.aliyun.openservices.ots.model.CapacityUnit;
import com.aliyun.openservices.ots.model.ColumnType;
import com.aliyun.openservices.ots.model.ColumnValue;
import com.aliyun.openservices.ots.model.CreateTableRequest;
import com.aliyun.openservices.ots.model.DeleteTableRequest;
import com.aliyun.openservices.ots.model.DescribeTableRequest;
import com.aliyun.openservices.ots.model.DescribeTableResult;
import com.aliyun.openservices.ots.model.Direction;
import com.aliyun.openservices.ots.model.GetRangeRequest;
import com.aliyun.openservices.ots.model.GetRangeResult;
import com.aliyun.openservices.ots.model.PrimaryKeyType;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.model.PutRowRequest;
import com.aliyun.openservices.ots.model.RangeRowQueryCriteria;
import com.aliyun.openservices.ots.model.Row;
import com.aliyun.openservices.ots.model.RowPrimaryKey;
import com.aliyun.openservices.ots.model.RowPutChange;
import com.aliyun.openservices.ots.model.TableMeta;

public class Utils {
    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);
    
    public static String getRowPKString(RowPrimaryKey pk) {
        Set<String> keys = pk.getPrimaryKey().keySet();
        StringBuilder sb = new StringBuilder();
        for (String key:keys) {
            PrimaryKeyValue value = pk.getPrimaryKey().get(key);
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

    public static String getCriateriaString(RangeRowQueryCriteria criteria) {
        StringBuilder sb = new StringBuilder();
        sb.append(getRowPKString(criteria.getInclusiveStartPrimaryKey()));
        sb.append("\n");
        sb.append(getRowPKString(criteria.getExclusiveEndPrimaryKey()));
        sb.append("\n");
        sb.append("Table    :" + criteria.getTableName());
        sb.append("\n");
        sb.append("Direction:" + criteria.getDirection().toString());
        sb.append("\n");
        sb.append("Column   :" + criteria.getColumnsToGet());
        sb.append("\n");
        sb.append("Limit    :" + criteria.getLimit());
        sb.append("\n");
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

    public static String getRangeString(OTSClient ots, RangeRowQueryCriteria criteria) {
        StringBuilder ss = new StringBuilder();
        RowPrimaryKey token = criteria.getInclusiveStartPrimaryKey();
        do {
            RangeRowQueryCriteria cur = new RangeRowQueryCriteria(criteria.getTableName());
            cur.setDirection(criteria.getDirection());
            cur.setColumnsToGet(criteria.getColumnsToGet());
            cur.setLimit(criteria.getLimit());
            cur.setInclusiveStartPrimaryKey(token);
            cur.setExclusiveEndPrimaryKey(criteria.getExclusiveEndPrimaryKey());

            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(cur);

            GetRangeResult result = ots.getRange(request);
            token = result.getNextStartPrimaryKey();
            List<Row> rows = result.getRows();
            for (Row row:rows) {
                Map<String, ColumnValue> cols = row.getColumns();
                Set<String> keys = cols.keySet();
                StringBuilder sb = new StringBuilder();
                for (String key:keys) {
                    ColumnValue v = cols.get(key);
                    if (v == null) {
                        sb.append(String.format("%s(%s)", key, "N/A"));
                    } else {
                        if (v.getType() == ColumnType.STRING) {
                            sb.append(String.format("%s(%s)", key, v.asString()));
                        } else if (v.getType() == ColumnType.INTEGER) {
                            sb.append(String.format("%s(%d)", key, v.asLong()));
                        } else if (v.getType() == ColumnType.DOUBLE) {
                            sb.append(String.format("%s(%f)", key, v.asDouble()));
                        } else if (v.getType() == ColumnType.BOOLEAN) {
                            sb.append(String.format("%s(%s)", key, v.asBoolean()));
                        } else if (v.getType() == ColumnType.BINARY) {
                            sb.append(String.format("%s(%s)", key, v.asBinary()));
                        } else {
                            throw new IllegalArgumentException(String.format("Unsuporrt tranform the type: %s.", v.getType()));
                        }
                    }

                }
                ss.append(sb);
            }
        } while (token != null);
        return ss.toString();
    }

    public static void createTable(OTSClient ots, String tableName, TableMeta tableMeta) throws Exception{
        // drop table
        DeleteTableRequest deleteTableRequest = new DeleteTableRequest();
        deleteTableRequest.setTableName(tableName);
        
        Thread.sleep(5 * 1000);
        try {
            ots.deleteTable(deleteTableRequest);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Thread.sleep(5 * 1000);

        // create table
        CapacityUnit capacityUnit = new CapacityUnit(5000, 5000);
        CreateTableRequest createTableRequest = new CreateTableRequest();
        createTableRequest.setTableMeta(tableMeta);
        createTableRequest.setReservedThroughput(capacityUnit);
        try {
            ots.createTable(createTableRequest);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        Thread.sleep(5 * 1000);
    }

    public static void prepareData(OTSClient ots, String tableName) throws Exception{
        for (int i  = 0; i < 1000; i++) {
            RowPutChange rowChange = new RowPutChange(tableName);
            RowPrimaryKey primaryKey = new RowPrimaryKey();
            primaryKey.addPrimaryKeyColumn("Uid", PrimaryKeyValue.fromString(String.format("%d", i)));
            primaryKey.addPrimaryKeyColumn("Pid", PrimaryKeyValue.fromLong(i));
            primaryKey.addPrimaryKeyColumn("Gid", PrimaryKeyValue.fromString(String.format("%d", i)));
            primaryKey.addPrimaryKeyColumn("Mid", PrimaryKeyValue.fromLong(i));
            rowChange.setPrimaryKey(primaryKey);

            Person p = new Person();
            p.setName(String.format("name_%d", i));
            p.setAge(i);
            p.setMale(true);
            p.setHeight(Double.valueOf(i));

            byte [] person = Person.toByte(p);

            rowChange.addAttributeColumn("name", ColumnValue.fromString(p.getName()));
            rowChange.addAttributeColumn("age", ColumnValue.fromLong(p.getAge()));
            if (i % 2 == 1) { 
                rowChange.addAttributeColumn("male", ColumnValue.fromBoolean(p.isMale()));
            }
            rowChange.addAttributeColumn("height", ColumnValue.fromDouble(p.getHeight()));
            rowChange.addAttributeColumn("hash", ColumnValue.fromBinary(person));

            PutRowRequest putRowRequest = new PutRowRequest();
            putRowRequest.setRowChange(rowChange);
            ots.putRow(putRowRequest);
        }

        for (int i  = 0; i < 1000; i++) {
            RowPutChange rowChange = new RowPutChange(tableName);
            RowPrimaryKey primaryKey = new RowPrimaryKey();
            primaryKey.addPrimaryKeyColumn("Uid", PrimaryKeyValue.fromString(String.format("杭州%d", i)));
            primaryKey.addPrimaryKeyColumn("Pid", PrimaryKeyValue.fromLong(i));
            primaryKey.addPrimaryKeyColumn("Gid", PrimaryKeyValue.fromString(String.format("余杭%d", i)));
            primaryKey.addPrimaryKeyColumn("Mid", PrimaryKeyValue.fromLong(i));
            rowChange.setPrimaryKey(primaryKey);

            Person p = new Person();
            p.setName(String.format("name_%d", i));
            p.setAge(i);
            p.setMale(true);
            p.setHeight(Double.valueOf(i));

            byte [] person = Person.toByte(p);

            rowChange.addAttributeColumn("name", ColumnValue.fromString(p.getName()));
            rowChange.addAttributeColumn("age", ColumnValue.fromLong(p.getAge()));
            if (i % 2 == 1) { 
                rowChange.addAttributeColumn("male", ColumnValue.fromBoolean(p.isMale()));
            }
            rowChange.addAttributeColumn("height", ColumnValue.fromDouble(p.getHeight()));
            rowChange.addAttributeColumn("hash", ColumnValue.fromBinary(person));

            PutRowRequest putRowRequest = new PutRowRequest();
            putRowRequest.setRowChange(rowChange);
            ots.putRow(putRowRequest);
        }
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

    public static String getJsonConf(ReaderConf conf) {
        String json = 
                "{\"job\":{\"setting\":{\"speed\":10}, \"content\":["
                        + "{\"reader\":{\"name\":\"otsreader\", \"parameter\":"+ conf.toString() +"}, "
                        + "\"writer\":{}}]}}";
        return json;
    }

    public static void dumpConf(String path, ReaderConf conf) {
        String json = getJsonConf(conf);
        OutputStream f;
        try {
            f = new FileOutputStream(path);
            f.write(json.getBytes());
            f.flush();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static TableMeta getTableMeta(OTSClient ots, OTSConf conf) {
        DescribeTableRequest describeTableRequest = new DescribeTableRequest();
        describeTableRequest.setTableName(conf.getTableName());
        DescribeTableResult result = ots.describeTable(describeTableRequest);
        TableMeta tableMeta = result.getTableMeta();
        return tableMeta;
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
    
    private static Map<String, Record> getMapping (List<Record> records) {
        Map<String, Record> mapping = new LinkedHashMap<String, Record>();
        for (Record r: records) {
            StringBuilder key = new StringBuilder();
            key.append(r.getColumn(0).asString() + "_");
            mapping.put(key.toString(), r);
        }
        return mapping;
    }
    
    private static Record getRecord(Map<String, Record> mapping, Record r) {
        StringBuilder key = new StringBuilder();
        key.append(r.getColumn(0).asString() + "_");
        return mapping.get(key.toString());
    }
    
    /**
     * 注意：使用该方法时，column里面一定要提供完整的完整的PK，且PK一定在column的最前面
     * @param ots
     * @param conf
     * @param output
     * @return
     */
    public static boolean checkOutput(
            OTSClient ots, 
            OTSConf conf,
            List<Record> output) {
        Direction direction = null;
        TableMeta meta = getTableMeta(ots, conf);

        OTSRange range = ParamChecker.checkRangeAndGet(meta, conf.getRangeBegin(), conf.getRangeEnd());

        direction = ParamChecker.checkDirectionAndEnd(meta, range.getBegin(), range.getEnd());

        List<Record> results = new ArrayList<Record>();
        List<String> cc = Common.getNormalColumnNameList(conf.getColumns());
        RowPrimaryKey token = range.getBegin();
        do {
            RangeRowQueryCriteria cur = new RangeRowQueryCriteria(conf.getTableName());
            cur.setDirection(direction);
            cur.setColumnsToGet(cc);
            cur.setLimit(-1);
            cur.setInclusiveStartPrimaryKey(token);
            cur.setExclusiveEndPrimaryKey(range.getEnd());

            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(cur);

            GetRangeResult result = ots.getRange(request);
            token = result.getNextStartPrimaryKey();
            for (Row row : result.getRows()) {
                Record line = new DefaultRecord();
                results.add(Common.parseRowToLine(row, conf.getColumns(), line));
            }
        } while (token != null);
        if (results.size() != output.size()) {
            LOG.error("Results size({}) not equal Output size({})", results.size(), output.size());
            return false;
        }
        Map<String, Record> m = getMapping(output);
        for (int i = 0; i < results.size(); i++) {
            Record r1 = results.get(i);
            Record r2 = getRecord(m, r1);
            LOG.debug("\nRecord1:{} \nRecord2:{}", getRecordString(r1), getRecordString(r2));
            if (r1.getColumnNumber() != r2.getColumnNumber()) {
                LOG.error("Size({}) of Record 1  not equal size({}) of Record 2", r1.getColumnNumber(), r2.getColumnNumber());
                return false;
            }
            for (int j = 0; j < r1.getColumnNumber(); j++) {
                Column c1 = r1.getColumn(j);
                Column c2 = r2.getColumn(j);
                if (c1 == null || c2 == null) {
                    if (c1 != c2) {
                        return false;
                    }
                    continue;
                }
                
                if (c1.getType() != c2.getType()) {
                    LOG.error("Type 1 not match type 2, {}, {}", c1.getType(), c2.getType());
                    return false;
                }
                switch (c1.getType()) {
                case STRING: 
                    if (!(c1.asString().equals(c2.asString()))) {
                        LOG.error("not equal. {} != {}", c1.asString(), c2.asString());
                        return false;
                    }
                    break;
                case LONG:
                    if (!(c1.asLong().longValue() == c2.asLong().longValue())) {
                        LOG.error("not equal. {} != {}", c1.asLong(), c2.asLong());
                        return false;
                    }
                    break;
                case DOUBLE:
                    if (!(c1.asDouble().doubleValue() == c2.asDouble().doubleValue())) {
                        LOG.error("not equal. {} != {}", c1.asDouble(), c2.asDouble());
                        return false;
                    }
                    break;
                case BOOL:
                    if (!(c1.asBoolean().booleanValue() == c2.asBoolean().booleanValue())) {
                        LOG.error("not equal. {} != {}", c1.asBoolean(), c2.asBoolean());
                        return false;
                    }
                    break;
                case BYTES:  
                    if (!(compareBytes(c1.asBytes(), c2.asBytes()) == 0)) {
                        LOG.error("not equal bytes.");
                        return false;
                    }
                    break;
                default:
                    throw new RuntimeException("Unsupport the type.");
                }
            }
        }
        return true;
    }
    public static void main(String [] args) throws FileNotFoundException {
        
        RowPrimaryKey pk = new RowPrimaryKey();
        pk.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("1"));
        pk.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.INF_MIN);
        pk.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.INF_MIN);
        pk.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.INF_MIN);
        System.out.println(getRowPKString(pk));
    }


}
