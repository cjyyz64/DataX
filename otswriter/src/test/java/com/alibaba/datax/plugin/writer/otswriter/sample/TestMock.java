package com.alibaba.datax.plugin.writer.otswriter.sample;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alibaba.datax.common.element.NullColumn;
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
import com.aliyun.openservices.ots.OTSErrorCode;
import com.aliyun.openservices.ots.OTSException;
import com.aliyun.openservices.ots.model.ColumnType;
import com.aliyun.openservices.ots.model.ColumnValue;
import com.aliyun.openservices.ots.model.MockOTSClient;
import com.aliyun.openservices.ots.model.OTSRow;
import com.aliyun.openservices.ots.model.PrimaryKeyType;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.model.Row;

public class TestMock {

    private static String tableName = "test";
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
    
    public static void test(Exception exception, Map<OTSRowPrimaryKey, Row> prepare, OTSOpType type, List<Record> input, List<OTSRow> expect) throws Exception {
        OTSConf conf = Conf.getConf(tableName, pk, attr, type);
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
    }
    
    @Test
    public void testForPut() throws Exception {
        Map<OTSRowPrimaryKey, Row> prepare = new LinkedHashMap<OTSRowPrimaryKey, Row>();
        {
            OTSRow row = new OTSRow();
            row.addPK("pk_0", PrimaryKeyValue.fromString("hello"));
            row.addColumn("attr_0", ColumnValue.fromString("world"));
            row.addColumn("attr_1", ColumnValue.fromString("ots"));
            row.addColumn("attr_2", ColumnValue.fromString("writer"));
            prepare.put(row.getPK(), row.getRow());
        }
        
        List<Record> input = new ArrayList<Record>();
        List<OTSRow> expect = new ArrayList<OTSRow>();
        
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello"));
            r.addColumn(new NullColumn());
            input.add(r);
            
            OTSRow row = new OTSRow();
            row.addPK("pk_0", PrimaryKeyValue.fromString("hello"));
            expect.add(row);
        }
        test(null, prepare, OTSOpType.PUT_ROW, input, expect);
    }
    
    @Test
    public void testForUpdate() throws Exception {
        Map<OTSRowPrimaryKey, Row> prepare = new LinkedHashMap<OTSRowPrimaryKey, Row>();
        {
            OTSRow row = new OTSRow();
            row.addPK("pk_0", PrimaryKeyValue.fromString("hello"));
            row.addColumn("attr_0", ColumnValue.fromString("world"));
            row.addColumn("attr_1", ColumnValue.fromString("ots"));
            row.addColumn("attr_2", ColumnValue.fromString("writer"));
            prepare.put(row.getPK(), row.getRow());
        }
        
        List<Record> input = new ArrayList<Record>();
        List<OTSRow> expect = new ArrayList<OTSRow>();
        
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello"));
            r.addColumn(new NullColumn());
            input.add(r);
            
            OTSRow row = new OTSRow();
            row.addPK("pk_0", PrimaryKeyValue.fromString("hello"));
            row.addColumn("attr_1", ColumnValue.fromString("ots"));
            row.addColumn("attr_2", ColumnValue.fromString("writer"));
            expect.add(row);
        }
        test(null, prepare, OTSOpType.UPDATE_ROW, input, expect);
    }
    
    @Test
    public void testForException() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<OTSRow> expect = new ArrayList<OTSRow>();
        
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello"));
            r.addColumn(new NullColumn());
            input.add(r);
            
            OTSRow row = new OTSRow();
            row.addPK("pk_0", PrimaryKeyValue.fromString("hello"));
            expect.add(row);
        }
        long begin = new Date().getTime();
        test(new OTSException("", null, OTSErrorCode.STORAGE_TIMEOUT, "", 500), null, OTSOpType.UPDATE_ROW, input, expect);
        long end = new Date().getTime();
        System.out.println(end - begin);
    }
}
