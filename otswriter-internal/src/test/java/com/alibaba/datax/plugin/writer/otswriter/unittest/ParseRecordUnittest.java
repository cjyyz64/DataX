package com.alibaba.datax.plugin.writer.otswriter.unittest;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.plugin.writer.otswriter.common.DataChecker;
import com.alibaba.datax.plugin.writer.otswriter.common.OTSPrimaryKeyBuilder;
import com.alibaba.datax.plugin.writer.otswriter.common.TestPluginCollector;
import com.alibaba.datax.plugin.writer.otswriter.common.TestPluginCollector.RecordAndMessage;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSAttrColumn;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSLine;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSOpType;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSPKColumn;
import com.alibaba.datax.plugin.writer.otswriter.utils.ParseRecord;
import com.aliyun.openservices.ots.internal.model.Column;
import com.aliyun.openservices.ots.internal.model.ColumnType;
import com.aliyun.openservices.ots.internal.model.PrimaryKey;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyType;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.internal.model.RowPutChange;
import com.aliyun.openservices.ots.internal.model.RowUpdateChange;
import com.aliyun.openservices.ots.internal.model.RowUpdateChange.Type;
import com.aliyun.openservices.ots.internal.utils.Pair;

/**
 * 测试目的：测试在各种模式组合下的解析行为
 * 
 * 关注点：
 * 1.模式
 *      1.1 普通模式
 *      1.2 多版本模式
 * 2.在多版本模式下输入是否合法
 *      2.1 合法
 *      2.2 Column未定义
 * 3.在普通模式下，空Column
 *      3.1 不为空
 *      3.2 部分列位空
 *      3.3 column数目不匹配
 * @author redchen
 *
 */
public class ParseRecordUnittest {

    /**
     * 输入：配置两个PK，两个Attr，构造一行数据，采用Put方式调用转换函数，期望获得RowPutChange，PK符合预期，Column符合预期。覆盖点：1.1 3.1
     */
    @Test
    public void testCase1ForPut() {
        // user configurtion
        List<OTSPKColumn> pkColumns = new ArrayList<OTSPKColumn>();
        pkColumns.add(new OTSPKColumn("pk_0", PrimaryKeyType.STRING));
        pkColumns.add(new OTSPKColumn("pk_1", PrimaryKeyType.INTEGER));
        
        List<OTSAttrColumn> attrColumns = new ArrayList<OTSAttrColumn>();
        attrColumns.add(new OTSAttrColumn("attr_0", ColumnType.STRING));
        attrColumns.add(new OTSAttrColumn("attr_1", ColumnType.INTEGER));
        
        // input data
        Record record = new DefaultRecord();
        record.addColumn(new StringColumn("hello"));
        record.addColumn(new LongColumn(1));
        record.addColumn(new StringColumn("中文"));
        record.addColumn(new LongColumn(-1));
        
        TestPluginCollector collector = new TestPluginCollector(Configuration.newDefault(), null, null);
        OTSLine line = ParseRecord.parseNormalRecordToOTSLine("xx", OTSOpType.PUT_ROW, pkColumns, attrColumns, record, -1, collector);
        
        {
            PrimaryKey pk = OTSPrimaryKeyBuilder.newInstance()
                    .add("pk_0", PrimaryKeyValue.fromString("hello"))
                    .add("pk_1", PrimaryKeyValue.fromLong(1))
                    .toPrimaryKey();
            
            assertEquals(pk, line.getPk());
        }
        
        {
            RowPutChange change = (RowPutChange) line.getRowChange();
            
            List<Column> columns = change.getColumnsToPut();
            
            assertEquals(2, columns.size());
            assertEquals("attr_0", columns.get(0).getName());
            assertEquals(ColumnType.STRING, columns.get(0).getValue().getType());
            assertEquals("中文", columns.get(0).getValue().asString());
            assertEquals("attr_1", columns.get(1).getName());
            assertEquals(ColumnType.INTEGER, columns.get(1).getValue().getType());
            assertEquals(-1, columns.get(1).getValue().asLong());
        }
        
        assertEquals(0, collector.getRecord().size());
    }

    /**
     * 输入：配置两个PK，两个Attr，构造一行数据，采用Update方式调用转换函数，期望获得RowUpdateChange，PK符合预期，Column符合预期。覆盖点：1.1 3.1
     */    
    @Test
    public void testCase1ForUpdate() {
        // user configurtion
        List<OTSPKColumn> pkColumns = new ArrayList<OTSPKColumn>();
        pkColumns.add(new OTSPKColumn("pk_0", PrimaryKeyType.STRING));
        pkColumns.add(new OTSPKColumn("pk_1", PrimaryKeyType.INTEGER));
        
        List<OTSAttrColumn> attrColumns = new ArrayList<OTSAttrColumn>();
        attrColumns.add(new OTSAttrColumn("attr_0", ColumnType.STRING));
        attrColumns.add(new OTSAttrColumn("attr_1", ColumnType.INTEGER));
        
        // input data
        Record record = new DefaultRecord();
        record.addColumn(new StringColumn("hello"));
        record.addColumn(new LongColumn(1));
        record.addColumn(new StringColumn("中文"));
        record.addColumn(new LongColumn(-1));
        
        TestPluginCollector collector = new TestPluginCollector(Configuration.newDefault(), null, null);
        OTSLine line = ParseRecord.parseNormalRecordToOTSLine("xx", OTSOpType.UPDATE_ROW, pkColumns, attrColumns, record, -1, collector);
        
        {
            PrimaryKey pk = OTSPrimaryKeyBuilder.newInstance()
                    .add("pk_0", PrimaryKeyValue.fromString("hello"))
                    .add("pk_1", PrimaryKeyValue.fromLong(1))
                    .toPrimaryKey();
            
            assertEquals(pk, line.getPk());
        }
        
        {
            RowUpdateChange change = (RowUpdateChange) line.getRowChange();
            
            List<Pair<Column, Type>> columns = change.getColumnsToUpdate();
            
            assertEquals(2, columns.size());
            assertEquals(Type.PUT, columns.get(0).getSecond());
            assertEquals("attr_0", columns.get(0).getFirst().getName());
            assertEquals(ColumnType.STRING, columns.get(0).getFirst().getValue().getType());
            assertEquals("中文", columns.get(0).getFirst().getValue().asString());
            
            assertEquals(Type.PUT, columns.get(1).getSecond());
            assertEquals("attr_1", columns.get(1).getFirst().getName());
            assertEquals(ColumnType.INTEGER, columns.get(1).getFirst().getValue().getType());
            assertEquals(-1, columns.get(1).getFirst().getValue().asLong());
        }
        assertEquals(0, collector.getRecord().size());
    }
    
    /**
     * 输入：配置两个PK，两个Attr，构造一行数据，数据中有空洞（NullColumn），采用Put方式调用转换函数，期望获得RowPutChange，PK符合预期，Column符合预期。覆盖点：1.1 3.2
     */
    @Test
    public void testCase2ForPut() {
        // user configurtion
        List<OTSPKColumn> pkColumns = new ArrayList<OTSPKColumn>();
        pkColumns.add(new OTSPKColumn("pk_0", PrimaryKeyType.STRING));
        pkColumns.add(new OTSPKColumn("pk_1", PrimaryKeyType.INTEGER));
        
        List<OTSAttrColumn> attrColumns = new ArrayList<OTSAttrColumn>();
        attrColumns.add(new OTSAttrColumn("attr_0", ColumnType.STRING));
        attrColumns.add(new OTSAttrColumn("attr_1", ColumnType.INTEGER));
        
        // input data
        Record record = new DefaultRecord();
        record.addColumn(new StringColumn("hello"));
        record.addColumn(new LongColumn(1));
        record.addColumn(new StringColumn("中文"));
        record.addColumn(new LongColumn());
        
        TestPluginCollector collector = new TestPluginCollector(Configuration.newDefault(), null, null);
        OTSLine line = ParseRecord.parseNormalRecordToOTSLine("xx", OTSOpType.PUT_ROW, pkColumns, attrColumns, record, 100, collector);
        
        {
            PrimaryKey pk = OTSPrimaryKeyBuilder.newInstance()
                    .add("pk_0", PrimaryKeyValue.fromString("hello"))
                    .add("pk_1", PrimaryKeyValue.fromLong(1))
                    .toPrimaryKey();
            
            assertEquals(pk, line.getPk());
        }
        
        {
            RowPutChange change = (RowPutChange) line.getRowChange();
            
            List<Column> columns = change.getColumnsToPut();
            
            assertEquals(1, columns.size());
            assertEquals("attr_0", columns.get(0).getName());
            assertEquals(ColumnType.STRING, columns.get(0).getValue().getType());
            assertEquals("中文", columns.get(0).getValue().asString());
            assertEquals(100, columns.get(0).getTimestamp());
        }
        assertEquals(0, collector.getRecord().size());
    }
    
    /**
     * 输入：配置两个PK，两个Attr，构造一行数据，数据中有空洞（NullColumn），采用Update方式调用转换函数，期望获得RowUpdateChange，PK符合预期，Column符合预期。覆盖点：1.1 3.2
     */
    @Test
    public void testCase2ForUpdate() {
        // user configurtion
        List<OTSPKColumn> pkColumns = new ArrayList<OTSPKColumn>();
        pkColumns.add(new OTSPKColumn("pk_0", PrimaryKeyType.STRING));
        pkColumns.add(new OTSPKColumn("pk_1", PrimaryKeyType.INTEGER));
        
        List<OTSAttrColumn> attrColumns = new ArrayList<OTSAttrColumn>();
        attrColumns.add(new OTSAttrColumn("attr_0", ColumnType.STRING));
        attrColumns.add(new OTSAttrColumn("attr_1", ColumnType.INTEGER));
        
        // input data
        Record record = new DefaultRecord();
        record.addColumn(new StringColumn("hello"));
        record.addColumn(new LongColumn(1));
        record.addColumn(new StringColumn("中文"));
        record.addColumn(new LongColumn());
        
        TestPluginCollector collector = new TestPluginCollector(Configuration.newDefault(), null, null);
        OTSLine line = ParseRecord.parseNormalRecordToOTSLine("xx", OTSOpType.UPDATE_ROW, pkColumns, attrColumns, record, 100, collector);
        
        {
            PrimaryKey pk = OTSPrimaryKeyBuilder.newInstance()
                    .add("pk_0", PrimaryKeyValue.fromString("hello"))
                    .add("pk_1", PrimaryKeyValue.fromLong(1))
                    .toPrimaryKey();
            
            assertEquals(pk, line.getPk());
        }
        
        {
            RowUpdateChange change = (RowUpdateChange) line.getRowChange();
            
            List<Pair<Column, Type>> columns = change.getColumnsToUpdate();
            
            assertEquals(2, columns.size());
            
            assertEquals(Type.PUT, columns.get(0).getSecond());
            assertEquals("attr_0", columns.get(0).getFirst().getName());
            assertEquals(ColumnType.STRING, columns.get(0).getFirst().getValue().getType());
            assertEquals("中文", columns.get(0).getFirst().getValue().asString());
            assertEquals(100, columns.get(0).getFirst().getTimestamp());
            
            assertEquals(Type.DELETE_ALL, columns.get(1).getSecond());
            assertEquals("attr_1", columns.get(1).getFirst().getName());
        }
        assertEquals(0, collector.getRecord().size());
    }
    
    /**
     * 输入：配置两个PK，两个Attr，构造一行数据，采用Put，多版本的方式调用转换函数，期望获得RowPutChange，PK符合预期，Column符合预期。覆盖点：1.2 2.1
     */
    @Test
    public void testCase3ForPut() throws Exception {
        // user configurtion
        List<OTSPKColumn> pkColumns = new ArrayList<OTSPKColumn>();
        pkColumns.add(new OTSPKColumn("pk_0", PrimaryKeyType.STRING));
        pkColumns.add(new OTSPKColumn("pk_1", PrimaryKeyType.INTEGER));
        
        List<OTSAttrColumn> attrColumns = new ArrayList<OTSAttrColumn>();
        attrColumns.add(new OTSAttrColumn("attr:0", "attr_0", ColumnType.STRING));
        attrColumns.add(new OTSAttrColumn("attr:1", "attr_1", ColumnType.INTEGER));
        
        // input data
        List<Record> records = new ArrayList<Record>();
        {
            Record record = new DefaultRecord();
            record.addColumn(new StringColumn("hello"));  // pk 0
            record.addColumn(new LongColumn(1));          // pk 1
            record.addColumn(new StringColumn("attr:0")); // column name
            record.addColumn(new LongColumn(1000));       // ts
            record.addColumn(new StringColumn("中文"));    // value
            
            records.add(record);
        }
        
        {
            Record record = new DefaultRecord();
            record.addColumn(new StringColumn("hello"));  // pk 0
            record.addColumn(new LongColumn(1));          // pk 1
            record.addColumn(new StringColumn("attr:0")); // column name
            record.addColumn(new LongColumn(1001));       // ts
            record.addColumn(new LongColumn(-1));         // value
            
            records.add(record);
        }

        TestPluginCollector collector = new TestPluginCollector(Configuration.newDefault(), null, null);
        OTSLine line = ParseRecord.parseMultiVersionRecordToOTSLine("xx", OTSOpType.PUT_ROW, pkColumns, attrColumns, records, collector);
        
        {
            PrimaryKey pk = OTSPrimaryKeyBuilder.newInstance()
                    .add("pk_0", PrimaryKeyValue.fromString("hello"))
                    .add("pk_1", PrimaryKeyValue.fromLong(1))
                    .toPrimaryKey();
            
            assertEquals(pk, line.getPk());
        }
        
        {
            RowPutChange change = (RowPutChange) line.getRowChange();
            
            List<Column> columns = change.getColumnsToPut();
            
            assertEquals(2, columns.size());
            assertEquals("attr_0", columns.get(0).getName());
            assertEquals(ColumnType.STRING, columns.get(0).getValue().getType());
            assertEquals("中文", columns.get(0).getValue().asString());
            assertEquals(1000, columns.get(0).getTimestamp());
            
            assertEquals("attr_0", columns.get(1).getName());
            assertEquals(ColumnType.STRING, columns.get(1).getValue().getType());
            assertEquals("-1", columns.get(1).getValue().asString());
            assertEquals(1001, columns.get(1).getTimestamp());
        }
        assertEquals(0, collector.getRecord().size());
    }
    
    
    /**
     * 输入：配置两个PK，两个Attr，构造一行数据，采用Update，多版本的方式调用转换函数，期望获得RowUpdateChange，PK符合预期，Column符合预期。覆盖点：1.2 2.1
     */
    @Test
    public void testCase3ForUpdate() throws Exception {
        // user configurtion
        List<OTSPKColumn> pkColumns = new ArrayList<OTSPKColumn>();
        pkColumns.add(new OTSPKColumn("pk_0", PrimaryKeyType.STRING));
        pkColumns.add(new OTSPKColumn("pk_1", PrimaryKeyType.INTEGER));
        
        List<OTSAttrColumn> attrColumns = new ArrayList<OTSAttrColumn>();
        attrColumns.add(new OTSAttrColumn("attr:0", "attr_0", ColumnType.STRING));
        attrColumns.add(new OTSAttrColumn("attr:1", "attr_1", ColumnType.INTEGER));
        
        // input data
        List<Record> records = new ArrayList<Record>();
        {
            Record record = new DefaultRecord();
            record.addColumn(new StringColumn("hello"));  // pk 0
            record.addColumn(new LongColumn(1));          // pk 1
            record.addColumn(new StringColumn("attr:0")); // column name
            record.addColumn(new LongColumn(1000));       // ts
            record.addColumn(new StringColumn("中文"));    // value
            
            records.add(record);
        }
        
        {
            Record record = new DefaultRecord();
            record.addColumn(new StringColumn("hello"));  // pk 0
            record.addColumn(new LongColumn(1));          // pk 1
            record.addColumn(new StringColumn("attr:0")); // column name
            record.addColumn(new LongColumn(1001));       // ts
            record.addColumn(new LongColumn(-1));         // value
            
            records.add(record);
        }

        TestPluginCollector collector = new TestPluginCollector(Configuration.newDefault(), null, null);
        OTSLine line = ParseRecord.parseMultiVersionRecordToOTSLine("xx", OTSOpType.UPDATE_ROW, pkColumns, attrColumns, records, collector);
        
        {
            PrimaryKey pk = OTSPrimaryKeyBuilder.newInstance()
                    .add("pk_0", PrimaryKeyValue.fromString("hello"))
                    .add("pk_1", PrimaryKeyValue.fromLong(1))
                    .toPrimaryKey();
            
            assertEquals(pk, line.getPk());
        }
        
        {
            RowUpdateChange change = (RowUpdateChange) line.getRowChange();
            
            List<Pair<Column, Type>> columns = change.getColumnsToUpdate();
            
            assertEquals(2, columns.size());
            assertEquals(Type.PUT, columns.get(0).getSecond());
            assertEquals("attr_0", columns.get(0).getFirst().getName());
            assertEquals(ColumnType.STRING, columns.get(0).getFirst().getValue().getType());
            assertEquals("中文", columns.get(0).getFirst().getValue().asString());
            assertEquals(1000, columns.get(0).getFirst().getTimestamp());
            
            assertEquals(Type.PUT, columns.get(1).getSecond());
            assertEquals("attr_0", columns.get(1).getFirst().getName());
            assertEquals(ColumnType.STRING, columns.get(1).getFirst().getValue().getType());
            assertEquals("-1", columns.get(1).getFirst().getValue().asString());
            assertEquals(1001, columns.get(1).getFirst().getTimestamp());
        }
        assertEquals(0, collector.getRecord().size());
    }
    
    // 普通模式下，采用Put模式输入的Column number少于用户配置的数目，期望报错退出，覆盖点：1.1 3.3
    @Test
    public void testCase4ForPut() {
        // user configurtion
        List<OTSPKColumn> pkColumns = new ArrayList<OTSPKColumn>();
        pkColumns.add(new OTSPKColumn("pk_0", PrimaryKeyType.STRING));
        pkColumns.add(new OTSPKColumn("pk_1", PrimaryKeyType.INTEGER));
        
        List<OTSAttrColumn> attrColumns = new ArrayList<OTSAttrColumn>();
        attrColumns.add(new OTSAttrColumn("attr_0", ColumnType.STRING));
        attrColumns.add(new OTSAttrColumn("attr_1", ColumnType.INTEGER));
        
        // input data
        Record record = new DefaultRecord();
        record.addColumn(new StringColumn("hello"));
        record.addColumn(new LongColumn(1));
        record.addColumn(new StringColumn("中文"));
        

        TestPluginCollector collector = new TestPluginCollector(Configuration.newDefault(), null, null);
        
        try {
            ParseRecord.parseNormalRecordToOTSLine("xx", OTSOpType.PUT_ROW, pkColumns, attrColumns, record, -1, collector);
            assertTrue(false);
        } catch (NullPointerException e) {
            assertTrue(true);
        }
    }
    
    // 普通模式下，采用Update模式输入的Column number少于用户配置的数目，期望报错退出，覆盖点：1.1 3.3
    @Test
    public void testCase4ForUpdate() {
        // user configurtion
        List<OTSPKColumn> pkColumns = new ArrayList<OTSPKColumn>();
        pkColumns.add(new OTSPKColumn("pk_0", PrimaryKeyType.STRING));
        pkColumns.add(new OTSPKColumn("pk_1", PrimaryKeyType.INTEGER));
        
        List<OTSAttrColumn> attrColumns = new ArrayList<OTSAttrColumn>();
        attrColumns.add(new OTSAttrColumn("attr_0", ColumnType.STRING));
        attrColumns.add(new OTSAttrColumn("attr_1", ColumnType.INTEGER));
        
        // input data
        Record record = new DefaultRecord();
        record.addColumn(new StringColumn("hello"));
        record.addColumn(new LongColumn(1));
        record.addColumn(new StringColumn("中文"));
        

        TestPluginCollector collector = new TestPluginCollector(Configuration.newDefault(), null, null);
        
        try {
            ParseRecord.parseNormalRecordToOTSLine("xx", OTSOpType.UPDATE_ROW, pkColumns, attrColumns, record, -1, collector);
            assertTrue(false);
        } catch (NullPointerException e) {
            assertTrue(true);
        }
    }
    
    // 多版本模式下，采用Put模式输入的Column未定义，期望报错退出，覆盖点：1.2 2.2
    @Test
    public void testCase5ForPut() throws Exception {
        // user configurtion
        List<OTSPKColumn> pkColumns = new ArrayList<OTSPKColumn>();
        pkColumns.add(new OTSPKColumn("pk_0", PrimaryKeyType.STRING));
        pkColumns.add(new OTSPKColumn("pk_1", PrimaryKeyType.INTEGER));
        
        List<OTSAttrColumn> attrColumns = new ArrayList<OTSAttrColumn>();
        attrColumns.add(new OTSAttrColumn("attr:0", "attr_0", ColumnType.STRING));
        attrColumns.add(new OTSAttrColumn("attr:1", "attr_1", ColumnType.INTEGER));
        
        // input data
        List<Record> records = new ArrayList<Record>();
        List<RecordAndMessage> expect = new ArrayList<RecordAndMessage>();
        {
            Record record = new DefaultRecord();
            record.addColumn(new StringColumn("hello"));  // pk 0
            record.addColumn(new LongColumn(1));          // pk 1
            record.addColumn(new StringColumn("attr:0")); // column name
            record.addColumn(new LongColumn(1000));       // ts
            record.addColumn(new StringColumn("中文"));    // value
            
            records.add(record);
        }
        
        {
            Record record = new DefaultRecord();
            record.addColumn(new StringColumn("hello"));  // pk 0
            record.addColumn(new LongColumn(1));          // pk 1
            record.addColumn(new StringColumn("attr:4")); // column name, undefine
            record.addColumn(new LongColumn(1001));       // ts
            record.addColumn(new LongColumn(-1));         // value
            
            records.add(record);
            
            expect.add(
                    new RecordAndMessage(
                            record, 
                            "The column name : 'attr:4' not define in column."
                            )
                    );
        }

        TestPluginCollector collector = new TestPluginCollector(Configuration.newDefault(), null, null);
        ParseRecord.parseMultiVersionRecordToOTSLine("xx", OTSOpType.UPDATE_ROW, pkColumns, attrColumns, records, collector);
        
        assertEquals(true, DataChecker.checkRecordWithMessage(collector.getContent(), expect)); 
    }
    
    // 多版本模式下，采用Update模式输入的Column未定义，期望报错退出，覆盖点：1.2 2.2
    @Test
    public void testCase5ForUpdate() throws Exception {
        // user configurtion
        List<OTSPKColumn> pkColumns = new ArrayList<OTSPKColumn>();
        pkColumns.add(new OTSPKColumn("pk_0", PrimaryKeyType.STRING));
        pkColumns.add(new OTSPKColumn("pk_1", PrimaryKeyType.INTEGER));
        
        List<OTSAttrColumn> attrColumns = new ArrayList<OTSAttrColumn>();
        attrColumns.add(new OTSAttrColumn("attr:0", "attr_0", ColumnType.STRING));
        attrColumns.add(new OTSAttrColumn("attr:1", "attr_1", ColumnType.INTEGER));
        
        // input data
        List<Record> records = new ArrayList<Record>();
        List<RecordAndMessage> expect = new ArrayList<RecordAndMessage>();
        {
            Record record = new DefaultRecord();
            record.addColumn(new StringColumn("hello"));  // pk 0
            record.addColumn(new LongColumn(1));          // pk 1
            record.addColumn(new StringColumn("attr:0")); // column name
            record.addColumn(new LongColumn(1000));       // ts
            record.addColumn(new StringColumn("中文"));    // value
            
            records.add(record);
        }
        
        {
            Record record = new DefaultRecord();
            record.addColumn(new StringColumn("hello"));  // pk 0
            record.addColumn(new LongColumn(1));          // pk 1
            record.addColumn(new StringColumn("attr:4")); // column name, undefine
            record.addColumn(new LongColumn(1001));       // ts
            record.addColumn(new LongColumn(-1));         // value
            
            records.add(record);
            
            expect.add(
                    new RecordAndMessage(
                            record, 
                            "The column name : 'attr:4' not define in column."
                            )
                    );
        }

        TestPluginCollector collector = new TestPluginCollector(Configuration.newDefault(), null, null);
        ParseRecord.parseMultiVersionRecordToOTSLine("xx", OTSOpType.UPDATE_ROW, pkColumns, attrColumns, records, collector);
        
        assertEquals(true, DataChecker.checkRecordWithMessage(collector.getContent(), expect)); 
    }
}
