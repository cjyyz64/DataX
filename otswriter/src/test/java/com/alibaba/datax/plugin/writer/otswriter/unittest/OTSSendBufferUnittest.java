package com.alibaba.datax.plugin.writer.otswriter.unittest;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.plugin.writer.otswriter.common.Conf;
import com.alibaba.datax.plugin.writer.otswriter.common.TestPluginCollector;
import com.alibaba.datax.plugin.writer.otswriter.common.Utils;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSLine;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSOpType;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSRowPrimaryKey;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSSendBuffer;
import com.aliyun.openservices.ots.model.ColumnType;
import com.aliyun.openservices.ots.model.ColumnValue;
import com.aliyun.openservices.ots.model.MockOTSClient;
import com.aliyun.openservices.ots.model.OTSRow;
import com.aliyun.openservices.ots.model.PrimaryKeyType;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.model.Row;


/**
 * 测试目的：主要是验证SendBuffer在各种Input组合下对Size的计算是否正确，场景如下：
 * 
 * 1.Input的行数
 *      1.1 1行
 *      1.2 100行
 *      1.3 150行
 * 2.是否有重复行
 *      2.1 否
 *      2.2 是
 * 3.操作类型
 *      3.1 PutRow
 *      3.2 UpdateRow
 */
public class OTSSendBufferUnittest {
    private static String tableName = "OTSSendBufferUnittest";
    public static List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
    public static List<ColumnType> attr = new ArrayList<ColumnType>();
    
    @BeforeClass
    public static void init() {
        pk.add(PrimaryKeyType.STRING);
        attr.add(ColumnType.STRING);
    }
    
    @AfterClass
    public static void close() {
        
    }
    
    public static void test(Exception exception, Map<OTSRowPrimaryKey, Row> prepare, OTSOpType type, List<Record> input, List<OTSRow> expect, List<Integer> rows) throws Exception {
        OTSConf conf = Conf.getConf(tableName, pk, attr, type);
        conf.setRetry(5);
        Configuration configuration = Configuration.newDefault();
        TestPluginCollector collector = new TestPluginCollector(configuration, null, null);
        MockOTSClient ots = new MockOTSClient(5000, exception, prepare);
        OTSSendBuffer buffer = new OTSSendBuffer(ots, collector, conf);
        
        for (Record r :  input) {
            OTSLine line = new OTSLine(tableName, type, r, conf.getPrimaryKeyColumn(), conf.getAttributeColumn());
            buffer.write(line);
        }
        buffer.close();
        
        assertEquals(0, collector.getContent().size());
        assertEquals(true, Utils.checkInput(ots, expect)); 
        assertEquals(true, Utils.checkRows(rows, ots.getRows())); 
    }
    
    public static void testIllegal(Exception exception, Map<OTSRowPrimaryKey, Row> prepare, OTSOpType type, List<Record> input, List<Record> expect, List<Integer> rows) throws Exception {
        OTSConf conf = Conf.getConf(tableName, pk, attr, type);
        conf.setRetry(5);
        Configuration configuration = Configuration.newDefault();
        TestPluginCollector collector = new TestPluginCollector(configuration, null, null);
        MockOTSClient ots = new MockOTSClient(5000, exception, prepare);
        OTSSendBuffer buffer = new OTSSendBuffer(ots, collector, conf);
        
        for (Record r :  input) {
            OTSLine line = new OTSLine(tableName, type, r, conf.getPrimaryKeyColumn(), conf.getAttributeColumn());
            buffer.write(line);
        }
        buffer.close();
        
        assertEquals(expect.size(), collector.getContent().size());
        assertEquals(true, Utils.checkInput(expect, collector.getRecord())); 
        assertEquals(true, Utils.checkRows(rows, ots.getRows())); 
    }

    // 1行
    // 输入：1行数据，采用PutRow的方式，期望：该行数据被成功写入，且只调用1次BatchWriteRow接口，每次调用API写入的行数为1 （覆盖场景：1.1、3.1， 测试正常逻辑是否符合预期）
    @Test
    public void testCase1() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<OTSRow> expect = new ArrayList<OTSRow>();
        List<Integer> rowsExpect = new ArrayList<Integer>(); // 每轮切分的行数
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello"));
            r.addColumn(new StringColumn());
            input.add(r);
            
            OTSRow row = new OTSRow();
            row.addPK("pk_0", PrimaryKeyValue.fromString("hello"));
            expect.add(row);
        }
        rowsExpect.add(1);
        test(null, null, OTSOpType.PUT_ROW, input, expect, rowsExpect);
    }
    
    // 输入：1行数据，采用UpdateRow的方式，期望：该行数据被成功写入，且只调用1次BatchWriteRow接口，每次调用API写入的行数为1（覆盖场景：1.1、3.2，测试正常逻辑是否符合预期）
    @Test
    public void testCase2() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<OTSRow> expect = new ArrayList<OTSRow>();
        List<Integer> rowsExpect = new ArrayList<Integer>(); // 每轮切分的行数
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello"));
            r.addColumn(new StringColumn());
            input.add(r);
            
            OTSRow row = new OTSRow();
            row.addPK("pk_0", PrimaryKeyValue.fromString("hello"));
            expect.add(row);
        }
        rowsExpect.add(1);
        test(null, null, OTSOpType.UPDATE_ROW, input, expect, rowsExpect);
    }
    
    // 输入：1行数据，该行的STRING PK的长度为1025B，期望：该行数据写入失败，且只调用1次BatchWriteRow接口，每次调用API写入的行数为1（覆盖场景：1.1，测试单行参数错误是否符合预期）
    @Test
    public void testCase3() throws Exception {
        
        StringBuilder sb = new StringBuilder();
        for (int i = 0 ; i < 1025; i++) {
            sb.append("a");
        }
        
        List<Record> input = new ArrayList<Record>();
        List<Integer> rowsExpect = new ArrayList<Integer>(); // 每轮切分的行数
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn(sb.toString()));
            r.addColumn(new StringColumn("world"));
            input.add(r);
        }
        rowsExpect.add(1);
        testIllegal(null, null, OTSOpType.PUT_ROW, input, new ArrayList<Record>(input), rowsExpect);
    }
    
    
    // 输入：1行数据，该行的STRING ATTR的长度为64KB + 1B，期望：该行数据写入失败，且只调用1次BatchWriteRow接口，每次调用API写入的行数为1（覆盖场景：1.1，测试单行参数错误是否符合预期）
    @Test
    public void testCase4() throws Exception {
        
        StringBuilder sb = new StringBuilder();
        for (int i = 0 ; i < (64*1024 + 1); i++) {
            sb.append("a");
        }
        
        List<Record> input = new ArrayList<Record>();
        List<Integer> rowsExpect = new ArrayList<Integer>(); // 每轮切分的行数
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("world"));
            r.addColumn(new StringColumn(sb.toString()));
            input.add(r);
        }
        rowsExpect.add(1);
        testIllegal(null, null, OTSOpType.PUT_ROW, input, new ArrayList<Record>(input), rowsExpect);
    }
    
    // 100行数据
    // 输入：100行数据，采用PutRow的方式，无重复数据，期望：该行数据被成功写入，且只调用1次BatchWriteRow接口，每次调用API写入的行数为100（覆盖场景：1.2、3.1、2.1，测试多行数据正常逻辑是否符合预期）
    @Test
    public void testCase5() throws Exception {
        
        List<Record> input = new ArrayList<Record>();
        List<OTSRow> expect = new ArrayList<OTSRow>();
        List<Integer> rowsExpect = new ArrayList<Integer>(); // 每轮切分的行数
        
        for (int i = 0; i < 100; i++) {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello_" + i));
            r.addColumn(new StringColumn("world_" + i));
            input.add(r);
            
            OTSRow row = new OTSRow();
            row.addPK("pk_0", PrimaryKeyValue.fromString("hello_" + i));
            row.addColumn("attr_0", ColumnValue.fromString("world_" + i));
            expect.add(row);
        }
        rowsExpect.add(100);
        test(null, null, OTSOpType.PUT_ROW, input, expect, rowsExpect);
    }
    
    // 输入：100 + 1行数据，采用PutRow的方式，有一行重复数据，期望：该行数据被成功写入，且只调用1次BatchWriteRow接口，每次调用API写入的行数为100（覆盖场景：1.2、3.1、2.2，测试多行数据中又重复的行的计算逻辑是否符合预期）
    @Test
    public void testCase6() throws Exception {
        
        List<Record> input = new ArrayList<Record>();
        List<OTSRow> expect = new ArrayList<OTSRow>();
        List<Integer> rowsExpect = new ArrayList<Integer>(); // 每轮切分的行数
        
        // 重复行
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello_" + 0));
            r.addColumn(new StringColumn("world_" + 0));
            input.add(r);
        }
        
        for (int i = 0; i < 100; i++) {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello_" + i));
            r.addColumn(new StringColumn("world_" + i));
            input.add(r);
            
            OTSRow row = new OTSRow();
            row.addPK("pk_0", PrimaryKeyValue.fromString("hello_" + i));
            row.addColumn("attr_0", ColumnValue.fromString("world_" + i));
            expect.add(row);
        }
        rowsExpect.add(100);
        test(null, null, OTSOpType.PUT_ROW, input, expect, rowsExpect);
    }
    // 输入：100行数据，采用UpdateRow的方式，无重复数据，期望：该行数据被成功写入，且只调用1次BatchWriteRow接口，每次调用API写入的行数为100（覆盖场景：1.2、3.2、2.1， 测试多行数据正常逻辑是否符合预期）
    @Test
    public void testCase7() throws Exception {
        
        List<Record> input = new ArrayList<Record>();
        List<OTSRow> expect = new ArrayList<OTSRow>();
        List<Integer> rowsExpect = new ArrayList<Integer>(); // 每轮切分的行数
        
        for (int i = 0; i < 100; i++) {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello_" + i));
            r.addColumn(new StringColumn("world_" + i));
            input.add(r);
            
            OTSRow row = new OTSRow();
            row.addPK("pk_0", PrimaryKeyValue.fromString("hello_" + i));
            row.addColumn("attr_0", ColumnValue.fromString("world_" + i));
            expect.add(row);
        }
        rowsExpect.add(100);
        test(null, null, OTSOpType.UPDATE_ROW, input, expect, rowsExpect);
    }
    // 输入：100 + 1行数据，采用UpdateRow的方式，有一行重复数据，期望：该行数据被成功写入，且只调用1次BatchWriteRow接口，每次调用API写入的行数为100（覅该场景：1.2、3.2、2.2，测试多行数据中又重复的行的计算逻辑是否符合预期）
    @Test
    public void testCase8() throws Exception {
        
        List<Record> input = new ArrayList<Record>();
        List<OTSRow> expect = new ArrayList<OTSRow>();
        List<Integer> rowsExpect = new ArrayList<Integer>(); // 每轮切分的行数
        
        // 重复行
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello_" + 0));
            r.addColumn(new StringColumn("world_" + 0));
            input.add(r);
        }
        
        for (int i = 0; i < 100; i++) {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello_" + i));
            r.addColumn(new StringColumn("world_" + i));
            input.add(r);
            
            OTSRow row = new OTSRow();
            row.addPK("pk_0", PrimaryKeyValue.fromString("hello_" + i));
            row.addColumn("attr_0", ColumnValue.fromString("world_" + i));
            expect.add(row);
        }
        rowsExpect.add(100);
        test(null, null, OTSOpType.UPDATE_ROW, input, expect, rowsExpect);
    }
    // 150行
    // 输入：150行数据，采用PutRow的方式，无重复数据，期望：该行数据被成功写入，且只调用2次BatchWriteRow接口，每次调用API写入的行数为100、50 （覆盖场景：1.3，3.1、2.1，测试多行数据正常逻辑是否符合预期）
    @Test
    public void testCase9() throws Exception {
        
        List<Record> input = new ArrayList<Record>();
        List<OTSRow> expect = new ArrayList<OTSRow>();
        List<Integer> rowsExpect = new ArrayList<Integer>(); // 每轮切分的行数
        
        for (int i = 0; i < 150; i++) {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello_" + i));
            r.addColumn(new StringColumn("world_" + i));
            input.add(r);
            
            OTSRow row = new OTSRow();
            row.addPK("pk_0", PrimaryKeyValue.fromString("hello_" + i));
            row.addColumn("attr_0", ColumnValue.fromString("world_" + i));
            expect.add(row);
        }
        rowsExpect.add(100);
        rowsExpect.add(50);
        test(null, null, OTSOpType.PUT_ROW, input, expect, rowsExpect);
    }
    // 输入：150行数据，采用PutRow的方式，51行重复数据，期望：该行数据被成功写入，且只调用2次BatchWriteRow接口，每次调用API写入的行数为100、50 （覆盖场景：1.3，3.1、2.2，测试多行数据中又重复的行的计算逻辑是否符合预期）
    @Test
    public void testCase10() throws Exception {
        
        List<Record> input = new ArrayList<Record>();
        List<OTSRow> expect = new ArrayList<OTSRow>();
        List<Integer> rowsExpect = new ArrayList<Integer>(); // 每轮切分的行数
        
        // 重复行
        for (int i = 0; i < 51; i++) {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello_" + i));
            r.addColumn(new StringColumn("world_" + i));
            input.add(r);
        }
        
        for (int i = 0; i < 150; i++) {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello_" + i));
            r.addColumn(new StringColumn("world_" + i));
            input.add(r);
            
            OTSRow row = new OTSRow();
            row.addPK("pk_0", PrimaryKeyValue.fromString("hello_" + i));
            row.addColumn("attr_0", ColumnValue.fromString("world_" + i));
            expect.add(row);
        }
        rowsExpect.add(100);
        rowsExpect.add(50);
        test(null, null, OTSOpType.PUT_ROW, input, expect, rowsExpect);
    }
    
    // 输入：150行数据，采用UpdateRow的方式，无重复数据，期望：该行数据被成功写入，且只调用2次BatchWriteRow接口，每次调用API写入的行数为100、50 （覆盖场景：1.3，3.2、2.1，测试多行数据正常逻辑是否符合预期）
    @Test
    public void testCase11() throws Exception {
        
        List<Record> input = new ArrayList<Record>();
        List<OTSRow> expect = new ArrayList<OTSRow>();
        List<Integer> rowsExpect = new ArrayList<Integer>(); // 每轮切分的行数
        
        for (int i = 0; i < 150; i++) {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello_" + i));
            r.addColumn(new StringColumn("world_" + i));
            input.add(r);
            
            OTSRow row = new OTSRow();
            row.addPK("pk_0", PrimaryKeyValue.fromString("hello_" + i));
            row.addColumn("attr_0", ColumnValue.fromString("world_" + i));
            expect.add(row);
        }
        rowsExpect.add(100);
        rowsExpect.add(50);
        test(null, null, OTSOpType.UPDATE_ROW, input, expect, rowsExpect);
    }
    
    // 输入：150行数据，采用UpdateRow的方式，51行重复数据，期望：该行数据被成功写入，且只调用2次BatchWriteRow接口，每次调用API写入的行数为100、50 （覆盖场景：1.3，3.2、2.2，测试多行数据中又重复的行的计算逻辑是否符合预期）
    @Test
    public void testCase12() throws Exception {
        
        List<Record> input = new ArrayList<Record>();
        List<OTSRow> expect = new ArrayList<OTSRow>();
        List<Integer> rowsExpect = new ArrayList<Integer>(); // 每轮切分的行数
        
        // 重复行
        for (int i = 0; i < 50; i++) {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello_" + i));
            r.addColumn(new StringColumn("world_" + i));
            input.add(r);
        }
        
        for (int i = 0; i < 150; i++) {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello_" + i));
            r.addColumn(new StringColumn("world_" + i));
            input.add(r);
            
            OTSRow row = new OTSRow();
            row.addPK("pk_0", PrimaryKeyValue.fromString("hello_" + i));
            row.addColumn("attr_0", ColumnValue.fromString("world_" + i));
            expect.add(row);
        }
        rowsExpect.add(100);
        rowsExpect.add(50);
        test(null, null, OTSOpType.UPDATE_ROW, input, expect, rowsExpect);
    }
}
