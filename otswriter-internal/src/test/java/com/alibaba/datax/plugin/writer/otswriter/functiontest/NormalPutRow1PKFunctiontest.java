package com.alibaba.datax.plugin.writer.otswriter.functiontest;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.plugin.writer.otswriter.common.BaseTest;
import com.alibaba.datax.plugin.writer.otswriter.common.Conf;
import com.alibaba.datax.plugin.writer.otswriter.common.OTSHelper;
import com.alibaba.datax.plugin.writer.otswriter.common.OTSRowBuilder;
import com.alibaba.datax.plugin.writer.otswriter.common.Utils;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSMode;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSOpType;
import com.aliyun.openservices.ots.internal.OTS;
import com.aliyun.openservices.ots.internal.model.ColumnType;
import com.aliyun.openservices.ots.internal.model.ColumnValue;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyType;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.internal.model.Row;
import com.aliyun.openservices.ots.internal.model.TableMeta;

public class NormalPutRow1PKFunctiontest extends BaseTest{
    private static String tableName = "NormalPutRow1PKFunctiontest";
    private static OTS ots = Utils.getOTSClient();
    private static TableMeta tableMeta = null;
    
    @BeforeClass
    public static void setBeforeClass() {}
    
    @AfterClass
    public static void setAfterClass() {
        ots.shutdown();
    }
    
    @Before
    public void setup() throws Exception {
        tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("UID", PrimaryKeyType.STRING);
        
        OTSHelper.createTableSafe(ots, tableMeta);
    }
    
    @After
    public void teardown() {}
    
    /**
     * 测试目的：测试在PutRow模式下，数据是否能正常的导入OTS中。
     * 测试内容：创建一个拥有4个PK的表，并分别构造1不重复的行，导入OTS，期望数据符合预期
     * @throws Exception 
     */
    @Test
    public void testCase1() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        long ts = System.currentTimeMillis();
        // 构造数据
        {
            for (int c = 0; c < 1; c++) { // row
                String value = String.format("UID_value_%06d", c);
                OTSRowBuilder row = OTSRowBuilder.newInstance();
                row.addPrimaryKeyColumn("UID", PrimaryKeyValue.fromString(value));
                
                Record r = new DefaultRecord();
                // pk
                r.addColumn(new StringColumn(value));
                
                for (int i = 0; i < 5; i++) { // column
                    String columnName = getColumnName(i);
                    r.addColumn(new LongColumn(i));
                    row.addAttrColumn(columnName, ColumnValue.fromLong(i), ts);
                }
                input.add(r);
                expect.add(row.toRow());
            }
        }
        
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(5, ColumnType.INTEGER), 
                OTSOpType.PUT_ROW,
                OTSMode.NORMAL);
        conf.setTimestamp(ts);
        testWithTS(ots, conf, input, expect); 
    }
    
    /**
     * 测试在PutRow模式下，数据是否能正常的导入OTS中。
     * 测试内容：创建一个拥有4个PK的表，并分别构造10不重复的行，导入OTS，期望数据符合预期
     * @throws Exception
     */
    @Test
    public void testCase2() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        long ts = System.currentTimeMillis();
        // 构造数据
        {
            for (int c = 0; c < 10; c++) { // row
                String value = String.format("UID_value_%06d", c);
                OTSRowBuilder row = OTSRowBuilder.newInstance();
                row.addPrimaryKeyColumn("UID", PrimaryKeyValue.fromString(value));
                
                Record r = new DefaultRecord();
                // pk
                r.addColumn(new StringColumn(value));
                
                for (int i = 0; i < 5; i++) { // column
                    String columnName = getColumnName(i);
                    r.addColumn(new LongColumn(i));
                    row.addAttrColumn(columnName, ColumnValue.fromLong(i), ts);
                }
                input.add(r);
                expect.add(row.toRow());
            }
        }
        
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(5, ColumnType.INTEGER), 
                OTSOpType.PUT_ROW,
                OTSMode.NORMAL);
        conf.setTimestamp(ts);
        testWithTS(ots, conf, input, expect); 
    }
    
    /**
     * 测试在PutRow模式下，数据是否能正常的导入OTS中。
     * 测试内容：创建一个拥有4个PK的表，并分别构造50不重复的行，导入OTS，期望数据符合预期
     * @throws Exception
     */
    @Test
    public void testCase3() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        long ts = System.currentTimeMillis();
        // 构造数据
        {
            for (int c = 0; c < 50; c++) { // row
                String value = String.format("UID_value_%06d", c);
                OTSRowBuilder row = OTSRowBuilder.newInstance();
                row.addPrimaryKeyColumn("UID", PrimaryKeyValue.fromString(value));
                
                Record r = new DefaultRecord();
                // pk
                r.addColumn(new StringColumn(value));
                
                for (int i = 0; i < 5; i++) { // column
                    String columnName = getColumnName(i);
                    r.addColumn(new LongColumn(i));
                    row.addAttrColumn(columnName, ColumnValue.fromLong(i), ts);
                }
                input.add(r);
                expect.add(row.toRow());
            }
        }
        
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(5, ColumnType.INTEGER), 
                OTSOpType.PUT_ROW,
                OTSMode.NORMAL);
        conf.setTimestamp(ts);
        testWithTS(ots, conf, input, expect); 
    }
    
    /**
     * 测试在PutRow模式下，数据是否能正常的导入OTS中。
     * 测试内容：创建一个拥有4个PK的表，并分别构造100不重复的行，导入OTS，期望数据符合预期
     * @throws Exception
     */
    @Test
    public void testCase4() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        long ts = System.currentTimeMillis();
        // 构造数据
        {
            for (int c = 0; c < 100; c++) { // row
                String value = String.format("UID_value_%06d", c);
                OTSRowBuilder row = OTSRowBuilder.newInstance();
                row.addPrimaryKeyColumn("UID", PrimaryKeyValue.fromString(value));
                
                Record r = new DefaultRecord();
                // pk
                r.addColumn(new StringColumn(value));
                
                for (int i = 0; i < 5; i++) { // column
                    String columnName = getColumnName(i);
                    r.addColumn(new LongColumn(i));
                    row.addAttrColumn(columnName, ColumnValue.fromLong(i), ts);
                }
                input.add(r);
                expect.add(row.toRow());
            }
        }
        
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(5, ColumnType.INTEGER), 
                OTSOpType.PUT_ROW,
                OTSMode.NORMAL);
        conf.setTimestamp(ts);
        testWithTS(ots, conf, input, expect); 
    }
    
    /**
     * 测试在PutRow模式下，数据是否能正常的导入OTS中。
     * 测试内容：创建一个拥有4个PK的表，并分别构造500不重复的行，导入OTS，期望数据符合预期
     * @throws Exception
     */
    @Test
    public void testCase5() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        long ts = System.currentTimeMillis();
        // 构造数据
        {
            for (int c = 0; c < 500; c++) { // row
                String value = String.format("UID_value_%06d", c);
                OTSRowBuilder row = OTSRowBuilder.newInstance();
                row.addPrimaryKeyColumn("UID", PrimaryKeyValue.fromString(value));
                
                Record r = new DefaultRecord();
                // pk
                r.addColumn(new StringColumn(value));
                
                for (int i = 0; i < 5; i++) { // column
                    String columnName = getColumnName(i);
                    r.addColumn(new LongColumn(i));
                    row.addAttrColumn(columnName, ColumnValue.fromLong(i), ts);
                }
                input.add(r);
                expect.add(row.toRow());
            }
        }
        
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(5, ColumnType.INTEGER), 
                OTSOpType.PUT_ROW,
                OTSMode.NORMAL);
        conf.setTimestamp(ts);
        testWithTS(ots, conf, input, expect); 
    }
    
    /**
     * 测试在PutRow模式下，数据是否能正常的导入OTS中。
     * 测试内容：创建一个拥有4个PK的表，并分别构造10重复的行，导入OTS，期望数据符合预期
     */
    @Test
    public void testCase6() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        long ts = System.currentTimeMillis();
        // 构造数据
        {
            for (int c = 0; c < 10; c++) { // row
                String value = String.format("UID_value_%06d", 0);
                OTSRowBuilder row = OTSRowBuilder.newInstance();
                row.addPrimaryKeyColumn("UID", PrimaryKeyValue.fromString(value));
                
                Record r = new DefaultRecord();
                // pk
                r.addColumn(new StringColumn(value));
                
                for (int i = 0; i < 5; i++) { // column
                    String columnName = getColumnName(i);
                    r.addColumn(new LongColumn(i));
                    row.addAttrColumn(columnName, ColumnValue.fromLong(i), ts);
                }
                input.add(r);
                expect.add(row.toRow());
            }
        }
        
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(5, ColumnType.INTEGER), 
                OTSOpType.PUT_ROW,
                OTSMode.NORMAL);
        conf.setTimestamp(ts);
        testWithTS(ots, conf, input, expect.subList(expect.size() - 1, expect.size())); 
    }
    
    /**
     * 测试在PutRow模式下，数据是否能正常的导入OTS中。
     * 测试内容：创建一个拥有4个PK的表，并分别构造50重复的行，导入OTS，期望数据符合预期
     * @throws Exception
     */
    @Test
    public void testCase7() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        long ts = System.currentTimeMillis();
        // 构造数据
        {
            for (int c = 0; c < 50; c++) { // row
                String value = String.format("UID_value_%06d", 0);
                OTSRowBuilder row = OTSRowBuilder.newInstance();
                row.addPrimaryKeyColumn("UID", PrimaryKeyValue.fromString(value));
                
                Record r = new DefaultRecord();
                // pk
                r.addColumn(new StringColumn(value));
                
                for (int i = 0; i < 5; i++) { // column
                    String columnName = getColumnName(i);
                    r.addColumn(new LongColumn(i));
                    row.addAttrColumn(columnName, ColumnValue.fromLong(i), ts);
                }
                input.add(r);
                expect.add(row.toRow());
            }
        }
        
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(5, ColumnType.INTEGER), 
                OTSOpType.PUT_ROW,
                OTSMode.NORMAL);
        conf.setTimestamp(ts);
        testWithTS(ots, conf, input, expect.subList(expect.size() - 1, expect.size())); 
    }
    
    /**
     * 测试在PutRow模式下，数据是否能正常的导入OTS中。
     * 测试内容：创建一个拥有4个PK的表，并分别构造100重复的行，导入OTS，期望数据符合预期
     * @throws Exception
     */
    @Test
    public void testCase8() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        long ts = System.currentTimeMillis();
        // 构造数据
        {
            for (int c = 0; c < 100; c++) { // row
                String value = String.format("UID_value_%06d", 0);
                OTSRowBuilder row = OTSRowBuilder.newInstance();
                row.addPrimaryKeyColumn("UID", PrimaryKeyValue.fromString(value));
                
                Record r = new DefaultRecord();
                // pk
                r.addColumn(new StringColumn(value));
                
                for (int i = 0; i < 5; i++) { // column
                    String columnName = getColumnName(i);
                    r.addColumn(new LongColumn(i));
                    row.addAttrColumn(columnName, ColumnValue.fromLong(i), ts);
                }
                input.add(r);
                expect.add(row.toRow());
            }
        }
        
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(5, ColumnType.INTEGER), 
                OTSOpType.PUT_ROW,
                OTSMode.NORMAL);
        conf.setTimestamp(ts);
        testWithTS(ots, conf, input, expect.subList(expect.size() - 1, expect.size())); 
    }
    
    /**
     * 测试在PutRow模式下，数据是否能正常的导入OTS中。
     * 测试内容：创建一个拥有4个PK的表，并分别构造500重复的行，导入OTS，期望数据符合预期
     * @throws Exception
     */
    @Test
    public void testCase9() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        long ts = System.currentTimeMillis();
        // 构造数据
        {
            for (int c = 0; c < 500; c++) { // row
                String value = String.format("UID_value_%06d", 0);
                OTSRowBuilder row = OTSRowBuilder.newInstance();
                row.addPrimaryKeyColumn("UID", PrimaryKeyValue.fromString(value));
                
                Record r = new DefaultRecord();
                // pk
                r.addColumn(new StringColumn(value));
                
                for (int i = 0; i < 6; i++) { // column
                    String columnName = getColumnName(i);
                    r.addColumn(new LongColumn(i));
                    row.addAttrColumn(columnName, ColumnValue.fromLong(i), ts);
                }
                input.add(r);
                expect.add(row.toRow());
            }
        }
        
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(6, ColumnType.INTEGER), 
                OTSOpType.PUT_ROW,
                OTSMode.NORMAL);
        conf.setTimestamp(ts);
        testWithTS(ots, conf, input, expect.subList(expect.size() - 1, expect.size())); 
    }
}
