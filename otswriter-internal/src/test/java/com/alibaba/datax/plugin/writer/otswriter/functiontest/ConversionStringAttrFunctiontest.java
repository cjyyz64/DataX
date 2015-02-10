package com.alibaba.datax.plugin.writer.otswriter.functiontest;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.DoubleColumn;
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

/**
 * 主要是测试各种类型转换为String的行为
 */
public class ConversionStringAttrFunctiontest extends BaseTest{
    
    public static String tableName = "ots_writer_conversion_string_attr_ft";
    
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
        tableMeta.addPrimaryKeyColumn("pk_0", PrimaryKeyType.STRING);

        OTSHelper.createTableSafe(ots, tableMeta);
    }
    
    @After
    public void teardown() {}
    
    /**
     * 传入 : 值是String，用户指定的是String 
     * 期待 : 转换正常，且值符合预期
     * @throws Exception
     */
    @Test
    public void testStringToString() throws Exception {
        
        List<Record> rs = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        
        // 传入值是String
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn(""));
            r.addColumn(new StringColumn(""));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString(""))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromString(""), 1)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("-100"));
            r.addColumn(new StringColumn("-100"));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("-100"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromString("-100"), 1)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("100L"));
            r.addColumn(new StringColumn("100L"));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("100L"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromString("100L"), 1)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("0x5f"));
            r.addColumn(new StringColumn("0x5f"));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("0x5f"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromString("0x5f"), 1)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("0"));
            r.addColumn(new StringColumn("0"));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("0"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromString("0"), 1)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("(*^__^*) 嘻嘻……"));
            r.addColumn(new StringColumn("(*^__^*) 嘻嘻……"));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("(*^__^*) 嘻嘻……"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromString("(*^__^*) 嘻嘻……"), 1)
                    .toRow();
            expect.add(row);
        }
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(1, ColumnType.STRING), 
                OTSOpType.UPDATE_ROW,
                OTSMode.NORMAL);
        testWithNoTS(ots,conf, rs, expect);
    }
    
    
    // 传入值是Int，用户指定的是String, 期待转换正常，且值符合预期
    @Test
    public void testIntToString() throws Exception {
        
        List<Record> rs = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        
        // 传入值是Int
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(Long.MIN_VALUE));
            r.addColumn(new LongColumn(Long.MIN_VALUE));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString(String.valueOf(Long.MIN_VALUE)))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromString(String.valueOf(Long.MIN_VALUE)), 1)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(Long.MAX_VALUE));
            r.addColumn(new LongColumn(Long.MAX_VALUE));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString(String.valueOf(Long.MAX_VALUE)))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromString(String.valueOf(Long.MAX_VALUE)), 1)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(0));
            r.addColumn(new LongColumn(0));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("0"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromString("0"), 1)
                    .toRow();
            expect.add(row);
        }
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(1, ColumnType.STRING), 
                OTSOpType.UPDATE_ROW,
                OTSMode.NORMAL);
        testWithNoTS(ots,conf, rs, expect);
    }
    
    // 传入值是Double，用户指定的是String, 期待转换正常，且值符合预期
    @Test
    public void testDoubleToString() throws Exception {
        
        List<Record> rs = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        
        // 传入值是Double
        {
            Record r = new DefaultRecord();
            r.addColumn(new DoubleColumn(-9012.023));
            r.addColumn(new DoubleColumn(-9012.023));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("-9012.023"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromString("-9012.023"), 1)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new DoubleColumn(0));
            r.addColumn(new DoubleColumn(0));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("0"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromString("0"), 1)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new DoubleColumn(1211.12));
            r.addColumn(new DoubleColumn(1211.12));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("1211.12"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromString("1211.12"), 1)
                    .toRow();
            expect.add(row);
        }
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(1, ColumnType.STRING), 
                OTSOpType.UPDATE_ROW,
                OTSMode.NORMAL);
        testWithNoTS(ots,conf, rs, expect);
    }
    // 传入值是Bool，用户指定的是String, 期待转换正常，且值符合预期
    @Test
    public void testBoolToString() throws Exception {
        
        List<Record> rs = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        
        {
            Record r = new DefaultRecord();
            r.addColumn(new BoolColumn(true));
            r.addColumn(new BoolColumn(true));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("true"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromString("true"), 1)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new BoolColumn(false));
            r.addColumn(new BoolColumn(false));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("false"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromString("false"), 1)
                    .toRow();
            expect.add(row);
        }
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(1, ColumnType.STRING), 
                OTSOpType.UPDATE_ROW,
                OTSMode.NORMAL);
        testWithNoTS(ots,conf, rs, expect);
    }
    // 传入值是Binary，用户指定的是String, 期待转换正常，且值符合预期
    @Test
    public void testBinaryToString() throws Exception {
        
        List<Record> rs = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        
        {
            Record r = new DefaultRecord();
            r.addColumn(new BytesColumn("快乐中国".getBytes("UTF-8")));
            r.addColumn(new BytesColumn("快乐中国".getBytes("UTF-8")));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("快乐中国"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromString("快乐中国"), 1)
                    .toRow();
            expect.add(row);
        }
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(1, ColumnType.STRING), 
                OTSOpType.UPDATE_ROW,
                OTSMode.NORMAL);
        testWithNoTS(ots,conf, rs, expect);
    }
}
