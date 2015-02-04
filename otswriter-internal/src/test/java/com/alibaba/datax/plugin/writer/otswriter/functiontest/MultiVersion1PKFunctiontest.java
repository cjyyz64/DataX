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

public class MultiVersion1PKFunctiontest extends BaseTest{
    private static String tableName = "MultiVersion1PKFunctiontest";
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
     * 测试目的：测试在UpdateRow模式下，数据是否能正常的导入OTS中。
     * 测试内容：创建一个拥有4个PK的表，构造1行数据，该行数据包含128列，每列5个版本，导入OTS，期望数据符合预期
     * @throws Exception 
     */
    @Test
    public void testCase1() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        // 构造数据
        
        {
            long ts = System.currentTimeMillis();
            
            OTSRowBuilder row = OTSRowBuilder.newInstance();
            row.addPrimaryKeyColumn("UID", PrimaryKeyValue.fromString("UID_value"));

            for (int i = 0; i < 128; i++) {
                String columnName = getColumnName(i);
                for (int j = 0; j < 5; j++) {
                    Record r = new DefaultRecord();
                    // pk
                    r.addColumn(new StringColumn("UID_value"));
                    // columnName
                    r.addColumn(new StringColumn(columnName));
                    // timestamp
                    r.addColumn(new LongColumn(ts + j));
                    // value
                    r.addColumn(new LongColumn(j));
                    input.add(r);
                    
                    row.addAttrColumn(columnName, ColumnValue.fromLong(j), ts + j);
                }
            }
            expect.add(row.toRow());
        }
        
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(128, ColumnType.INTEGER), 
                OTSOpType.UPDATE_ROW,
                OTSMode.MULTI_VERSION);
        testWithTS(ots, conf, input, expect);
    }
    
    /**
     * 测试目的：测试在UpdateRow模式下，数据是否能正常的导入OTS中。
     * 测试内容：创建一个拥有4个PK的表，构造10不重复行数据，该行数据包含12列，每列5个版本，导入OTS，期望数据符合预期
     * @throws Exception 
     */
    @Test
    public void testCase2() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        // 构造数据
        
        {
            long ts = System.currentTimeMillis();

            for (int c = 0; c < 10; c++) { // row
                String value = String.format("UID_value_%06d", c);
                OTSRowBuilder row = OTSRowBuilder.newInstance();
                row.addPrimaryKeyColumn("UID", PrimaryKeyValue.fromString(value));
                for (int i = 0; i < 12; i++) { // column
                    String columnName = getColumnName(i);
                    for (int j = 0; j < 5; j++) { // version
                        Record r = new DefaultRecord();
                        // pk
                        r.addColumn(new StringColumn(value));
                        // columnName
                        r.addColumn(new StringColumn(columnName));
                        // timestamp
                        r.addColumn(new LongColumn(ts + j));
                        // value
                        r.addColumn(new LongColumn(j));
                        input.add(r);

                        row.addAttrColumn(columnName, ColumnValue.fromLong(j), ts + j);
                    }
                }
                expect.add(row.toRow());
            }
        }
        
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(12, ColumnType.INTEGER), 
                OTSOpType.UPDATE_ROW,
                OTSMode.MULTI_VERSION);
        testWithTS(ots, conf, input, expect);
    }
    
    /**
     * 测试目的：测试在UpdateRow模式下，数据是否能正常的导入OTS中。
     * 测试内容：创建一个拥有4个PK的表，构造50不重复行数据，该行数据包含2列，每列5个版本，导入OTS，期望数据符合预期
     * @throws Exception
     */
    @Test
    public void testCase3() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        // 构造数据
        
        {
            long ts = System.currentTimeMillis();

            for (int c = 0; c < 50; c++) { // row
                String value = String.format("UID_value_%06d", c);
                OTSRowBuilder row = OTSRowBuilder.newInstance();
                row.addPrimaryKeyColumn("UID", PrimaryKeyValue.fromString(value));
                for (int i = 0; i < 2; i++) { // column
                    String columnName = getColumnName(i);
                    for (int j = 0; j < 5; j++) { // version
                        Record r = new DefaultRecord();
                        // pk
                        r.addColumn(new StringColumn(value));
                        // columnName
                        r.addColumn(new StringColumn(columnName));
                        // timestamp
                        r.addColumn(new LongColumn(ts + j));
                        // value
                        r.addColumn(new LongColumn(j));
                        input.add(r);

                        row.addAttrColumn(columnName, ColumnValue.fromLong(j), ts + j);
                    }
                }
                expect.add(row.toRow());
            }
        }
        
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(2, ColumnType.INTEGER), 
                OTSOpType.UPDATE_ROW,
                OTSMode.MULTI_VERSION);
        testWithTS(ots, conf, input, expect);
    }
    
    /**
     * 测试目的：测试在UpdateRow模式下，数据是否能正常的导入OTS中。
     * 测试内容：创建一个拥有4个PK的表，构造100不重复行数据，该行数据包含2列，每列2个版本，导入OTS，期望数据符合预期
     * @throws Exception
     */
    @Test
    public void testCase4() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        // 构造数据
        
        {
            long ts = System.currentTimeMillis();

            for (int c = 0; c < 100; c++) { // row
                String value = String.format("UID_value_%06d", c);
                OTSRowBuilder row = OTSRowBuilder.newInstance();
                row.addPrimaryKeyColumn("UID", PrimaryKeyValue.fromString(value));
                for (int i = 0; i < 2; i++) { // column
                    String columnName = getColumnName(i);
                    for (int j = 0; j < 2; j++) { // version
                        Record r = new DefaultRecord();
                        // pk
                        r.addColumn(new StringColumn(value));
                        // columnName
                        r.addColumn(new StringColumn(columnName));
                        // timestamp
                        r.addColumn(new LongColumn(ts + j));
                        // value
                        r.addColumn(new LongColumn(j));
                        input.add(r);

                        row.addAttrColumn(columnName, ColumnValue.fromLong(j), ts + j);
                    }
                }
                expect.add(row.toRow());
            }
        }
        
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(2, ColumnType.INTEGER), 
                OTSOpType.UPDATE_ROW,
                OTSMode.MULTI_VERSION);
        testWithTS(ots, conf, input, expect);
    }
    
    /**
     * 测试目的：测试在UpdateRow模式下，数据是否能正常的导入OTS中。
     * 测试内容：创建一个拥有4个PK的表，构造500不重复行数据，该行数据包含2列，每列2个版本，导入OTS，期望数据符合预期
     * @throws Exception
     */
    @Test
    public void testCase5() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        // 构造数据
        
        {
            long ts = System.currentTimeMillis();

            for (int c = 0; c < 500; c++) { // row
                String value = String.format("UID_value_%06d", c);
                OTSRowBuilder row = OTSRowBuilder.newInstance();
                row.addPrimaryKeyColumn("UID", PrimaryKeyValue.fromString(value));
                for (int i = 0; i < 2; i++) { // column
                    String columnName = getColumnName(i);
                    for (int j = 0; j < 2; j++) { // version
                        Record r = new DefaultRecord();
                        // pk
                        r.addColumn(new StringColumn(value));
                        // columnName
                        r.addColumn(new StringColumn(columnName));
                        // timestamp
                        r.addColumn(new LongColumn(ts + j));
                        // value
                        r.addColumn(new LongColumn(j));
                        input.add(r);

                        row.addAttrColumn(columnName, ColumnValue.fromLong(j), ts + j);
                    }
                }
                expect.add(row.toRow());
            }
        }
        
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(2, ColumnType.INTEGER), 
                OTSOpType.UPDATE_ROW,
                OTSMode.MULTI_VERSION);
        testWithTS(ots, conf, input, expect);
    }
    
    /**
     * 测试目的：测试在UpdateRow模式下，数据是否能正常的导入OTS中。
     * 测试内容：创建一个拥有4个PK的表，构造10重复行数据，该行数据包含12列，每列5个版本，导入OTS，期望数据符合预期
     * @throws Exception
     */
    @Test
    public void testCase6() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        // 构造数据
        
        {
            long ts = System.currentTimeMillis();

            for (int c = 0; c < 10; c++) { // row
                String value = String.format("UID_value_%06d", 1);
                OTSRowBuilder row = OTSRowBuilder.newInstance();
                row.addPrimaryKeyColumn("UID", PrimaryKeyValue.fromString(value));
                for (int i = 0; i < 12; i++) { // column
                    String columnName = getColumnName(i);
                    for (int j = 0; j < 5; j++) { // version
                        Record r = new DefaultRecord();
                        // pk
                        r.addColumn(new StringColumn(value));
                        // columnName
                        r.addColumn(new StringColumn(columnName));
                        // timestamp
                        r.addColumn(new LongColumn(ts + j));
                        // value
                        r.addColumn(new LongColumn(c * 10000 + j));
                        input.add(r);

                        row.addAttrColumn(columnName, ColumnValue.fromLong(c * 10000 + j), ts + j);
                    }
                }
                expect.add(row.toRow());
            }
        }
        
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(12, ColumnType.INTEGER), 
                OTSOpType.UPDATE_ROW,
                OTSMode.MULTI_VERSION);
        testWithTS(ots, conf, input, expect.subList(expect.size() - 1, expect.size()));
    }
    
    /**
     * 测试目的：测试在UpdateRow模式下，数据是否能正常的导入OTS中。
     * 测试内容：创建一个拥有4个PK的表，构造50重复行数据，该行数据包含2列，每列5个版本，导入OTS，期望数据符合预期
     * @throws Exception
     */
    @Test
    public void testCase7() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        // 构造数据
        
        {
            long ts = System.currentTimeMillis();

            for (int c = 0; c < 50; c++) { // row
                String value = String.format("UID_value_%06d", 1);
                OTSRowBuilder row = OTSRowBuilder.newInstance();
                row.addPrimaryKeyColumn("UID", PrimaryKeyValue.fromString(value));
                for (int i = 0; i < 2; i++) { // column
                    String columnName = getColumnName(i);
                    for (int j = 0; j < 5; j++) { // version
                        Record r = new DefaultRecord();
                        // pk
                        r.addColumn(new StringColumn(value));
                        // columnName
                        r.addColumn(new StringColumn(columnName));
                        // timestamp
                        r.addColumn(new LongColumn(ts + j));
                        // value
                        r.addColumn(new LongColumn(c * 10000 + j));
                        input.add(r);

                        row.addAttrColumn(columnName, ColumnValue.fromLong(c * 10000 + j), ts + j);
                    }
                }
                expect.add(row.toRow());
            }
        }
        
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(2, ColumnType.INTEGER), 
                OTSOpType.UPDATE_ROW,
                OTSMode.MULTI_VERSION);
        testWithTS(ots, conf, input, expect.subList(expect.size() - 1, expect.size()));
    }
    
    /**
     *  测试目的：测试在UpdateRow模式下，数据是否能正常的导入OTS中。
     *  测试内容：创建一个拥有4个PK的表，构造100重复行数据，该行数据包含2列，每列2个版本，导入OTS，期望数据符合预期
     * @throws Exception
     */
    @Test
    public void testCase8() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        // 构造数据
        
        {
            long ts = System.currentTimeMillis();

            for (int c = 0; c < 100; c++) { // row
                String value = String.format("UID_value_%06d", 1);
                OTSRowBuilder row = OTSRowBuilder.newInstance();
                row.addPrimaryKeyColumn("UID", PrimaryKeyValue.fromString(value));
                for (int i = 0; i < 2; i++) { // column
                    String columnName = getColumnName(i);
                    for (int j = 0; j < 2; j++) { // version
                        Record r = new DefaultRecord();
                        // pk
                        r.addColumn(new StringColumn(value));
                        // columnName
                        r.addColumn(new StringColumn(columnName));
                        // timestamp
                        r.addColumn(new LongColumn(ts + j));
                        // value
                        r.addColumn(new LongColumn(c * 10000 + j));
                        input.add(r);

                        row.addAttrColumn(columnName, ColumnValue.fromLong(c * 10000 + j), ts + j);
                    }
                }
                expect.add(row.toRow());
            }
        }
        
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(2, ColumnType.INTEGER), 
                OTSOpType.UPDATE_ROW,
                OTSMode.MULTI_VERSION);
        testWithTS(ots, conf, input, expect.subList(expect.size() - 1, expect.size()));
    }
    
    /**
     *  测试目的：测试在UpdateRow模式下，数据是否能正常的导入OTS中。
     *  测试内容：创建一个拥有4个PK的表，构造100重复行数据，该行数据包含2列，每列2个版本，导入OTS，期望数据符合预期
     * @throws Exception
     */
    @Test
    public void testCase9() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        // 构造数据
        
        {
            long ts = System.currentTimeMillis();

            for (int c = 0; c < 100; c++) { // row
                String value = String.format("UID_value_%06d", 1);
                OTSRowBuilder row = OTSRowBuilder.newInstance();
                row.addPrimaryKeyColumn("UID", PrimaryKeyValue.fromString(value));
                for (int i = 0; i < 2; i++) { // column
                    String columnName = getColumnName(i);
                    for (int j = 0; j < 2; j++) { // version
                        Record r = new DefaultRecord();
                        // pk
                        r.addColumn(new StringColumn(value));
                        // columnName
                        r.addColumn(new StringColumn(columnName));
                        // timestamp
                        r.addColumn(new LongColumn(ts + j));
                        // value
                        r.addColumn(new LongColumn(c * 10000 + j));
                        input.add(r);

                        row.addAttrColumn(columnName, ColumnValue.fromLong(c * 10000 + j), ts + j);
                    }
                }
                expect.add(row.toRow());
            }
        }
        
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(2, ColumnType.INTEGER), 
                OTSOpType.UPDATE_ROW,
                OTSMode.MULTI_VERSION);
        testWithTS(ots, conf, input, expect.subList(expect.size() - 1, expect.size()));
    }
    
    private void testMultiCellWithDifTS(int rowCount, int columnCount, int cellCount) throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        {
            long ts = System.currentTimeMillis();
            for (int c = 0; c < rowCount; c++) { // row
                String value = String.format("UID_value_%06d", c);
                OTSRowBuilder row = OTSRowBuilder.newInstance();
                row.addPrimaryKeyColumn("UID", PrimaryKeyValue.fromString(value));
                for (int i = 0; i < columnCount; i++) { // column
                    String columnName = getColumnName(i);
                    for (int j = 0; j < cellCount; j++) { // version
                        Record r = new DefaultRecord();
                        // pk
                        r.addColumn(new StringColumn(value));
                        // columnName
                        r.addColumn(new StringColumn(columnName));
                        // timestamp
                        r.addColumn(new LongColumn(ts));
                        // value
                        r.addColumn(new LongColumn(c * 10000 + j));
                        input.add(r);

                        row.addAttrColumn(columnName, ColumnValue.fromLong(c * 10000 + j), ts);
                        ts++;
                    }
                }
                expect.add(row.toRow());
            }
        }
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(columnCount, ColumnType.INTEGER), 
                OTSOpType.UPDATE_ROW,
                OTSMode.MULTI_VERSION);
        testWithTS(ots, conf, input, expect);
    }
    
    /**
     * 测试目的：测试在UpdateRow模式下，数据是否能正常的导入OTS中。
     * 测试内容：创建一个拥有1个PK的表，一行数据，该行依次包括构造1、129、128 * 100、128 * 100 + 1个Cell，导入OTS，期望数据符合预期
     * @throws Exception 
     */
    @Test
    public void testCase10() throws Exception {
        this.setup();
        testMultiCellWithDifTS(1, 1, 1);
        this.setup();
        testMultiCellWithDifTS(1, 1, 129);
        this.setup();
        testMultiCellWithDifTS(1, 1, 128*100);
        this.setup();
        testMultiCellWithDifTS(1, 1, 128*100 + 1);
        this.setup();
        testMultiCellWithDifTS(101, 1, 129);
        this.setup();
        testMultiCellWithDifTS(101, 2, 129);
        this.setup();
        testMultiCellWithDifTS(10, 128, 10);
    }
}
