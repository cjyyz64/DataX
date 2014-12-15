package com.alibaba.datax.plugin.writer.otswriter.unittest;

import static org.junit.Assert.*;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.plugin.writer.otswriter.common.Conf;
import com.alibaba.datax.plugin.writer.otswriter.common.TestPluginCollector;
import com.alibaba.datax.plugin.writer.otswriter.common.TestPluginCollector.RecordAndMessage;
import com.alibaba.datax.plugin.writer.otswriter.common.Utils;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSAttrColumn;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSBatchWriterRowTask;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSLine;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSOpType;
import com.aliyun.openservices.ots.OTSErrorCode;
import com.aliyun.openservices.ots.OTSException;
import com.aliyun.openservices.ots.model.ColumnType;
import com.aliyun.openservices.ots.model.ColumnValue;
import com.aliyun.openservices.ots.model.MockOTSClient;
import com.aliyun.openservices.ots.model.OTSRow;
import com.aliyun.openservices.ots.model.PrimaryKeyType;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;

/**
 * 1.批量导入行数
 *      1.1 1行
 *      1.2 50行
 *      1.3 100行
 *      1.4 150行
 * 2.重试的场景
 *      2.0 成功
 *      2.1 异常整体失败
 *          2.1.1 单行重试
 *              2.1.1.1 全部成功
 *              2.1.1.2 部分成功
 *              2.1.1.3 全部失败
 *          2.1.2 整体重试
 *              2.1.2.1 全部成功
 *              2.1.2.2 部分成功
 *              2.1.2.3 全部失败
 *      2.2 返回状态失败
 *          2.2.1 部分失败
 *          2.2.2 全部成功
 *          2.2.3 全部失败
 * 3.操作类型
 *      3.1 PutRow
 *      3.2 UpdateRow
 *
 */
public class OTSBatchWriteRowTaskUnittest {

    private static String tableName = "OTSBatchWriteRowTaskUnittest";
    public static List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
    public static List<ColumnType> attr = new ArrayList<ColumnType>();
    
    private static final Logger LOG = LoggerFactory.getLogger(OTSBatchWriteRowTaskUnittest.class);

    @BeforeClass
    public static void init() {
        pk.add(PrimaryKeyType.STRING);
        attr.add(ColumnType.STRING);
        attr.add(ColumnType.BINARY);
    }

    public void test(OTSOpType type, List<Record> input, List<OTSRow> expect, int invokeTime, List<OTSAttrColumn> attributeColumn) {
        OTSConf conf = Conf.getConf(tableName, pk, attr, type);
        if (attributeColumn != null) {
            conf.setAttributeColumn(attributeColumn);
        }
        Configuration configuration = Configuration.newDefault();
        TestPluginCollector collector = new TestPluginCollector(configuration, null, null);
        MockOTSClient ots = new MockOTSClient();

        List<OTSLine> lines = new ArrayList<OTSLine>();
        for (Record r : input) {
            OTSLine line = new OTSLine(tableName, type, r, conf.getPrimaryKeyColumn(), conf.getAttributeColumn());
            lines.add(line);
        }

        OTSBatchWriterRowTask task = new OTSBatchWriterRowTask(collector, ots, conf, lines);
        task.run();

        assertEquals(0, collector.getContent().size());
        assertEquals(true, Utils.checkInput(ots, expect)); 
        assertEquals(invokeTime, ots.getInvokeTimes());
    }
    
    public void testIllegal(OTSOpType type, List<Record> input, List<RecordAndMessage> expect, int invokeTimes, List<OTSAttrColumn> attributeColumn) {
        testIllegal(null, type, input, expect, invokeTimes, attributeColumn);
    }

    public void testIllegal(Exception exception, OTSOpType type, List<Record> input, List<RecordAndMessage> expect, int invokeTimes, List<OTSAttrColumn> attributeColumn) {
        OTSConf conf = Conf.getConf(tableName, pk, attr, type);
        if (attributeColumn != null) {
            conf.setAttributeColumn(attributeColumn);
        }

        Configuration configuration = Configuration.newDefault();
        TestPluginCollector collector = new TestPluginCollector(configuration, null, null);
        MockOTSClient ots = new MockOTSClient(5000, exception, null);

        List<OTSLine> lines = new ArrayList<OTSLine>();
        for (Record r : input) {
            OTSLine line = new OTSLine(tableName, type, r, conf.getPrimaryKeyColumn(), conf.getAttributeColumn());
            lines.add(line);
        }

        OTSBatchWriterRowTask task = new OTSBatchWriterRowTask(collector, ots, conf, lines);
        task.run();

        assertEquals(expect.size(), collector.getContent().size());
        assertEquals(true, Utils.checkInputWithMessage(expect, collector.getContent())); 
        assertEquals(invokeTimes, ots.getInvokeTimes());
    }

    public void testIllegal(OTSOpType type, List<Record> input, List<OTSRow> expect, List<RecordAndMessage> errorExpect, int invokeTime, List<OTSAttrColumn> attributeColumn) {
        OTSConf conf = Conf.getConf(tableName, pk, attr, type);
        if (attributeColumn != null) {
            conf.setAttributeColumn(attributeColumn);
        }
        Configuration configuration = Configuration.newDefault();
        TestPluginCollector collector = new TestPluginCollector(configuration, null, null);
        MockOTSClient ots = new MockOTSClient();

        List<OTSLine> lines = new ArrayList<OTSLine>();
        for (Record r : input) {
            OTSLine line = new OTSLine(tableName, type, r, conf.getPrimaryKeyColumn(), conf.getAttributeColumn());
            lines.add(line);
        }

        OTSBatchWriterRowTask task = new OTSBatchWriterRowTask(collector, ots, conf, lines);
        task.run();

        assertEquals(errorExpect.size(), collector.getContent().size());
        assertEquals(true, Utils.checkInput(ots, expect)); 
        assertEquals(true, Utils.checkInputWithMessage(errorExpect, collector.getContent())); 
        assertEquals(invokeTime, ots.getInvokeTimes());
    }

    // PUT/UPDATE ROW 的方式

    // 输入：构造1行正常数据，期望：这1行数据能被正确的写入,SDK被调用一次（覆盖场景：1.1、3.1、2.0，主要测试单行成功的场景）
    // 输入：构造50行正常数据，期望：这50行数据能被正确的写入，SDK被调用一次。（覆盖场景：1.2、3.1、2.0，主要测试多行成功的场景）
    // 输入：构造100行正常数据，期望：这100行数据能被正确的写入，SDK被调用一次。（覆盖场景：1.2、3.1、2.0，主要测试多行成功的场景）
    // 输入：构造150行正常数据，期望：这150行数据能被正确的写入，SDK被调用151次。（覆盖场景：1.2、3.1、2.1.1.1，主要测试多行整体失败单行重试全部成功的场景）
    @Test
    public void testCase1() throws UnsupportedEncodingException {
        // key：数据量
        // value：SDK调用次数 
        Map<Integer, Integer> conditions = new LinkedHashMap<Integer, Integer>();
        conditions.put(1, 1);
        conditions.put(50, 1);
        conditions.put(100, 1);
        conditions.put(150, 151);

        for (Entry<Integer, Integer> en : conditions.entrySet()) {
            List<Record> input = new ArrayList<Record>();
            List<OTSRow> expect = new ArrayList<OTSRow>();
            for (int i = 0; i < en.getKey(); i++) {
                Record r = new DefaultRecord();
                r.addColumn(new StringColumn("hello_" + i));
                r.addColumn(new StringColumn("hello_" + i));
                r.addColumn(new BytesColumn(("hello_" + i).getBytes("UTF-8")));
                input.add(r);

                OTSRow row = new OTSRow();
                row.addPK("pk_0", PrimaryKeyValue.fromString("hello_" + i));
                row.addColumn("attr_0", ColumnValue.fromString("hello_" + i));
                row.addColumn("attr_1", ColumnValue.fromBinary(("hello_" + i).getBytes("UTF-8")));
                expect.add(row);
            }
            LOG.info("PUT_ROW, Row Count:{}, SDK invoke times:{}", en.getKey(), en.getValue());
            test(OTSOpType.PUT_ROW, input, expect, en.getValue(), null);
            LOG.info("UPDATE_ROW, Row Count:{}, SDK invoke times:{}", en.getKey(), en.getValue());
            test(OTSOpType.UPDATE_ROW, input, expect, en.getValue(), null);
        }
    }

    // PUT / UPDATE ROW
    // 输入：构造1行数据，其中1列数据的Attr Column非法，期望：1行数据进入垃圾回收器,SDK被调用一次。（覆盖场景：1.1、3.1、2.1.2.3， 主要测试单行整体失败的场景）
    // 输入：构造50行数据，其中1列数据的Attr Column非法，期望：50行数据进入垃圾回收器，SDK被调用51次。（覆盖场景：1.2、3.1、2.1.1，主要测试多行整体失败的场景）
    // 输入：构造100行数据，其中1列数据的Attr Column非法，期望：100行数据进入垃圾回收器，SDK被调用101次。（覆盖场景：1.2、3.1、2.1.1，主要测试多行整体失败的场景）
    @Test
    public void testCase2() throws UnsupportedEncodingException {
        // key：数据量
        // value：SDK调用次数 
        Map<Integer, Integer> conditions = new LinkedHashMap<Integer, Integer>();
        conditions.put(1, 1);
        conditions.put(50, 51);
        conditions.put(100, 101);
        for (Entry<Integer, Integer> en : conditions.entrySet()) {

            List<OTSAttrColumn> attributeColumn = new ArrayList<OTSAttrColumn>();
            attributeColumn.add(new OTSAttrColumn("0_attr", ColumnType.STRING));
            attributeColumn.add(new OTSAttrColumn("attr_1", ColumnType.BINARY));

            List<Record> input = new ArrayList<Record>();
            List<RecordAndMessage> expect = new ArrayList<RecordAndMessage>();
            for (int i = 0; i < en.getKey(); i++) {
                Record r = new DefaultRecord();
                r.addColumn(new StringColumn("hello_" + i));
                r.addColumn(new StringColumn("hello_" + i));
                r.addColumn(new BytesColumn(("hello_" + i).getBytes("UTF-8")));

                input.add(r);

                expect.add((new TestPluginCollector(Configuration.newDefault(), null, null)).new RecordAndMessage(r, "Column name invalid"));
            }
            LOG.info("PUT_ROW, Row Count:{}, SDK invoke times:{}", en.getKey(), en.getValue());
            testIllegal(OTSOpType.PUT_ROW, input, expect, en.getValue(), attributeColumn);
            LOG.info("UPDATE_ROW, Row Count:{}, SDK invoke times:{}", en.getKey(), en.getValue());
            testIllegal(OTSOpType.UPDATE_ROW, input, expect, en.getValue(), attributeColumn);
        }
    }

    // PUT / UPDATE ROW
    // 输入：构造1行数据，其中1列数据的大于OTS的Cell限制（STRING PK, STRING ATTR, BINARY），期望:1行数据进入垃圾回收器,SDK被调用一次。（覆盖场景：1.1、3.1、2.1.1.3， 主要测试单行失败的场景）
    // 输入：构造50行数据，其中1行的1列数据的大于OTS的Cell限制（STRING PK, STRING ATTR, BINARY），期望：1行数据进入垃圾回收器，SDK被调用1次。（覆盖场景：1.2、3.1、2.2.1，主要是测试多行部分失败的场景）
    // 输入：构造100行数据，其中1行的1列数据的大于OTS的Cell限制（STRING PK, STRING ATTR, BINARY），期望：1行数据进入垃圾回收器，SDK被调用1次。（覆盖场景：1.2、3.1、2.2.1，主要是测试多行部分失败的场景）

    // STRING PK
    @Test
    public void testCase3() throws UnsupportedEncodingException {
        // key：数据量
        // value：SDK调用次数 
        Map<Integer, Integer> conditions = new LinkedHashMap<Integer, Integer>();
        conditions.put(1, 1);
        conditions.put(50, 1);
        conditions.put(100, 1);

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1025; i++) {
            sb.append("x");
        }

        for (Entry<Integer, Integer> en : conditions.entrySet()) {
            List<Record> input = new ArrayList<Record>();
            List<RecordAndMessage> expect = new ArrayList<RecordAndMessage>();
            for (int i = 0; i < en.getKey(); i++) {
                Record r = new DefaultRecord();
                r.addColumn(new StringColumn(sb.toString()));
                r.addColumn(new StringColumn("hello_" + i));
                r.addColumn(new BytesColumn(("hello_" + i).getBytes("UTF-8")));
                input.add(r);
                expect.add((new TestPluginCollector(Configuration.newDefault(), null, null)).new RecordAndMessage(r, "STRING PK SIZE > 1KB"));
            }
            LOG.info("PUT_ROW, Row Count:{}, SDK invoke times:{}", en.getKey(), en.getValue());
            testIllegal(OTSOpType.PUT_ROW, input, expect, en.getValue(), null);
            LOG.info("UPDATE_ROW, Row Count:{}, SDK invoke times:{}", en.getKey(), en.getValue());
            testIllegal(OTSOpType.PUT_ROW, input, expect, en.getValue(), null);
        }
    }

    // STRING ATTR
    @Test
    public void testCase4() throws UnsupportedEncodingException {
        // key：数据量
        // value：SDK调用次数 
        Map<Integer, Integer> conditions = new LinkedHashMap<Integer, Integer>();
        conditions.put(1, 1);
        conditions.put(50, 51);
        conditions.put(100, 101);

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < (64 *1024 + 1); i++) {
            sb.append("x");
        }

        for (Entry<Integer, Integer> en : conditions.entrySet()) {

            List<Record> input = new ArrayList<Record>();
            List<RecordAndMessage> expect = new ArrayList<RecordAndMessage>();
            for (int i = 0; i < en.getKey(); i++) {
                Record r = new DefaultRecord();
                r.addColumn(new StringColumn("hello_" + i));
                r.addColumn(new StringColumn(sb.toString()));
                r.addColumn(new BytesColumn(("hello_" + i).getBytes("UTF-8")));
                input.add(r);
                expect.add((new TestPluginCollector(Configuration.newDefault(), null, null)).new RecordAndMessage(r, "STRING ATTR SIZE > 64KB"));
            }
            LOG.info("PUT_ROW, Row Count:{}, SDK invoke times:{}", en.getKey(), en.getValue());
            testIllegal(OTSOpType.PUT_ROW, input, expect, en.getValue(), null);
            LOG.info("UPDATE_ROW, Row Count:{}, SDK invoke times:{}", en.getKey(), en.getValue());
            testIllegal(OTSOpType.UPDATE_ROW, input, expect, en.getValue(), null);
        }
    }

    // BINARY ATTR
    @Test
    public void testCase5() throws UnsupportedEncodingException {

        // key：数据量
        // value：SDK调用次数 
        Map<Integer, Integer> conditions = new LinkedHashMap<Integer, Integer>();
        conditions.put(1, 1);
        conditions.put(50, 51);
        conditions.put(100, 101);

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < (64 *1024 + 1); i++) {
            sb.append("x");
        }

        for (Entry<Integer, Integer> en : conditions.entrySet()) {

            List<Record> input = new ArrayList<Record>();
            List<RecordAndMessage> expect = new ArrayList<RecordAndMessage>();
            for (int i = 0; i < en.getKey(); i++) {
                Record r = new DefaultRecord();
                r.addColumn(new StringColumn("hello_" + i));
                r.addColumn(new StringColumn("hello_" + i));
                r.addColumn(new BytesColumn((sb.toString()).getBytes("UTF-8")));
                input.add(r);
                expect.add((new TestPluginCollector(Configuration.newDefault(), null, null)).new RecordAndMessage(r, "BINARY ATTR SIZE > 64KB"));
            }
            LOG.info("PUT_ROW, Row Count:{}, SDK invoke times:{}", en.getKey(), en.getValue());
            testIllegal(OTSOpType.PUT_ROW, input, expect, en.getValue(), null);
            LOG.info("UPDATE_ROW, Row Count:{}, SDK invoke times:{}", en.getKey(), en.getValue());
            testIllegal(OTSOpType.UPDATE_ROW, input, expect, en.getValue(), null);
        }
    }

    // 输入：构造1行数据，该行数据大于1MB，期望:1行数据进入垃圾回收器，SDK被调用一次。（覆盖场景：1.1、3.1、2.1.2.3，主要测试单行失败的场景）
    @Test
    public void testCase6() throws UnsupportedEncodingException {

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < (1024 *1024); i++) {
            sb.append("x");
        }

        List<Record> input = new ArrayList<Record>();
        List<RecordAndMessage> expect = new ArrayList<RecordAndMessage>();
        for (int i = 0; i < 1; i++) {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello_" + i));
            r.addColumn(new StringColumn("hello_" + i));
            r.addColumn(new BytesColumn((sb.toString()).getBytes("UTF-8")));
            input.add(r);
            expect.add((new TestPluginCollector(Configuration.newDefault(), null, null)).new RecordAndMessage(r, "Total Size > 1MB"));
        }
        testIllegal(OTSOpType.PUT_ROW, input, expect, 1, null);
        testIllegal(OTSOpType.UPDATE_ROW, input, expect, 1, null);
    }

    // 输入：PutRow, 构造1行数据，该行的数据等于1MB + 空列，期望:这1行数据能被正确的写入OTS，SDK被调用一次。（覆盖场景：1.1、3.1、2.0， 主要测试单行正确的场景）
    // 输入：UpdateRow, 构造1行数据，该行的数据等于1MB + 空列，期望：1行数据进入垃圾回收器，SDK被调用1次。（覆盖场景：1.1、3.1、2.0，主要测试单行失败的场景）
    @Test
    public void testCase7() throws UnsupportedEncodingException {

        List<OTSAttrColumn> attributeColumn = new ArrayList<OTSAttrColumn>();
        for (int i = 0; i < 17; i++) {
            attributeColumn.add(new OTSAttrColumn(String.format("attr_%03d", i), ColumnType.STRING));
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < (64*1024); i++) {
            sb.append("x");
        }

        StringBuilder tmpSb = new StringBuilder();
        for (int i = 0; i < 65397; i++) {
            tmpSb.append("z");
        }

        List<Record> input = new ArrayList<Record>();
        List<OTSRow> expect = new ArrayList<OTSRow>();
        List<RecordAndMessage> errorExpect = new ArrayList<RecordAndMessage>();
        for (int i = 0; i < 1; i++) {
            Record r = new DefaultRecord();
            OTSRow row = new OTSRow();

            r.addColumn(new StringColumn("hello_" + i));// 4 + 7 Bytes
            row.addPK("pk_0", PrimaryKeyValue.fromString("hello_" + i));

            for (int j = 0; j < 15; j++) {
                r.addColumn(new StringColumn(sb.toString()));
                row.addColumn(String.format("attr_%03d", j), ColumnValue.fromString(sb.toString()));
            }

            r.addColumn(new StringColumn(tmpSb.toString()));
            row.addColumn("attr_015", ColumnValue.fromString(tmpSb.toString()));

            // Null Column
            r.addColumn(new StringColumn());

            input.add(r);
            expect.add(row);
            errorExpect.add((new TestPluginCollector(Configuration.newDefault(), null, null)).new RecordAndMessage(r, "Total Size > 1MB"));
        }
        test(OTSOpType.PUT_ROW, input, expect, 1, attributeColumn);
        testIllegal(OTSOpType.UPDATE_ROW, input, errorExpect, 1, attributeColumn);
    }

    // PUT / UPDATE
    // 输入：构造1行数据，该行的数据等于1MB，期望:这1行数据能被正确的写入，SDK被调用1次。（覆盖场景：1.1、3.1、2.0，主要是测试单行正确的场景）
    @Test
    public void testCase8() throws UnsupportedEncodingException {

        List<OTSAttrColumn> attributeColumn = new ArrayList<OTSAttrColumn>();
        for (int i = 0; i < 16; i++) {
            attributeColumn.add(new OTSAttrColumn(String.format("attr_%03d", i), ColumnType.STRING));
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < (64*1024); i++) {
            sb.append("x");
        }

        StringBuilder tmpSb = new StringBuilder();
        for (int i = 0; i < 65397; i++) {
            tmpSb.append("z");
        }

        List<Record> input = new ArrayList<Record>();
        List<OTSRow> expect = new ArrayList<OTSRow>();
        for (int i = 0; i < 1; i++) {
            Record r = new DefaultRecord();
            OTSRow row = new OTSRow();

            r.addColumn(new StringColumn("hello_" + i));// 4 + 7 Bytes
            row.addPK("pk_0", PrimaryKeyValue.fromString("hello_" + i));

            for (int j = 0; j < 15; j++) {
                r.addColumn(new StringColumn(sb.toString()));
                row.addColumn(String.format("attr_%03d", j), ColumnValue.fromString(sb.toString()));
            }

            r.addColumn(new StringColumn(tmpSb.toString()));
            row.addColumn("attr_015", ColumnValue.fromString(tmpSb.toString()));

            input.add(r);
            expect.add(row);
        }
        test(OTSOpType.PUT_ROW, input, expect, 1, attributeColumn);
        test(OTSOpType.UPDATE_ROW, input, expect, 1, attributeColumn);
    }

    // PUT / UPDATE
    // 输入：构造50行数据，其中有一行数据大于1MB，期望：49行被写入，1行被写入垃圾回收器，SDK被调用51次（覆盖场景：1.2、3.1、2.1.1.2，主要是测试整体失败且单行重试部分成功的场景）
    @Test
    public void testCase14() throws UnsupportedEncodingException {

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < (1024 *1024); i++) {
            sb.append("x");
        }

        List<Record> input = new ArrayList<Record>();
        List<OTSRow> expect = new ArrayList<OTSRow>();
        List<RecordAndMessage> errorExpect = new ArrayList<RecordAndMessage>();

        for (int i = 0; i < 1; i++) {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello_" + i));
            r.addColumn(new StringColumn("hello_" + i));
            r.addColumn(new BytesColumn((sb.toString()).getBytes("UTF-8")));
            input.add(r);
            errorExpect.add((new TestPluginCollector(Configuration.newDefault(), null, null)).new RecordAndMessage(r, "BINARY ATTR SIZE > 64KB"));
        }

        for (int i = 1; i < 50; i++) {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello_" + i));
            r.addColumn(new StringColumn("hello_" + i));
            r.addColumn(new BytesColumn(("hello_" + i).getBytes("UTF-8")));
            input.add(r);
            OTSRow row = new OTSRow();
            row.addPK("pk_0", PrimaryKeyValue.fromString("hello_" + i));
            row.addColumn("attr_0", ColumnValue.fromString("hello_" + i));
            row.addColumn("attr_1", ColumnValue.fromBinary(("hello_" + i).getBytes("UTF-8")));
            expect.add(row);
        }
        testIllegal(OTSOpType.PUT_ROW, input, expect, errorExpect, 51, null);
        testIllegal(OTSOpType.UPDATE_ROW, input, expect, errorExpect, 51, null);
    }
    // PUT / UPDATE
    // 输入：构造50行数据，数据量为1MB + 1B，期望：这50行数据成功的写入，SDK被调用51次（覆盖场景：1.2、3.1、2.1.1.1，主要是测试整体失败且单行重试全部成功的场景）
    @Test
    public void testCase15() throws UnsupportedEncodingException {
        List<Record> input = new ArrayList<Record>();
        List<OTSRow> expect = new ArrayList<OTSRow>();

        StringBuilder pkSb = new StringBuilder();
        for (int i = 0; i < 967; i++) {
            pkSb.append("p"); // 964 + 4(pk_0) + 3(000) = 971
        }

        StringBuilder attrSb = new StringBuilder();
        for (int i = 0; i < 9994; i++) {
            attrSb.append("k");// 9991 + 6(attr_*) + 3(000) =  10000
        }

        for (int i = 0; i < 49; i++) {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn(String.format("%s%03d", pkSb.toString(), i)));
            r.addColumn(new StringColumn(String.format("%s%03d", attrSb.toString(), i)));
            r.addColumn(new BytesColumn((String.format("%s%03d", attrSb.toString(),i)).getBytes("UTF-8")));
            input.add(r);

            OTSRow row = new OTSRow();
            row.addPK("pk_0", PrimaryKeyValue.fromString(String.format("%s%03d", pkSb.toString(), i)));
            row.addColumn("attr_0", ColumnValue.fromString(String.format("%s%03d", attrSb.toString(), i)));
            row.addColumn("attr_1", ColumnValue.fromBinary((String.format("%s%03d", attrSb.toString(), i)).getBytes("UTF-8")));
            expect.add(row);
        }

        StringBuilder newPkSb = new StringBuilder();
        for (int i = 0; i < 27; i++) {
            newPkSb.append("p");
        }

        for (int i = 49; i < 50; i++) {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn(String.format("%s%03d%s", pkSb.toString(), i, newPkSb.toString())));
            r.addColumn(new StringColumn(String.format("%s%03d", attrSb.toString(), i)));
            r.addColumn(new BytesColumn((String.format("%s%03d", attrSb.toString(),i)).getBytes("UTF-8")));
            input.add(r);

            OTSRow row = new OTSRow();
            row.addPK("pk_0", PrimaryKeyValue.fromString(String.format("%s%03d%s", pkSb.toString(), i, newPkSb.toString())));
            row.addColumn("attr_0", ColumnValue.fromString(String.format("%s%03d", attrSb.toString(), i)));
            row.addColumn("attr_1", ColumnValue.fromBinary((String.format("%s%03d", attrSb.toString(), i)).getBytes("UTF-8")));
            expect.add(row);
        }
        test(OTSOpType.PUT_ROW, input, expect, 51, null);
        test(OTSOpType.UPDATE_ROW, input, expect, 51, null);
    }

    // 100行
    // PUT / UPDATE
    // 输入：构造100行数据，其中有一行数据大于1MB，期望：99行被写入，1行被写入垃圾回收器，SDK被调用101次（覆盖场景：1.2、3.1、2.1.1.2，主要是测试整体失败且单行重试部分成功的场景）
    @Test
    public void testCase16() throws UnsupportedEncodingException {

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < (1024 *1024); i++) {
            sb.append("x");
        }

        List<Record> input = new ArrayList<Record>();
        List<OTSRow> expect = new ArrayList<OTSRow>();
        List<RecordAndMessage> errorExpect = new ArrayList<RecordAndMessage>();

        for (int i = 0; i < 1; i++) {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello_" + i));
            r.addColumn(new StringColumn("hello_" + i));
            r.addColumn(new BytesColumn((sb.toString()).getBytes("UTF-8")));
            input.add(r);
            errorExpect.add((new TestPluginCollector(Configuration.newDefault(), null, null)).new RecordAndMessage(r, "BINARY ATTR SIZE > 64KB"));
        }

        for (int i = 1; i < 100; i++) {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello_" + i));
            r.addColumn(new StringColumn("hello_" + i));
            r.addColumn(new BytesColumn(("hello_" + i).getBytes("UTF-8")));
            input.add(r);
            OTSRow row = new OTSRow();
            row.addPK("pk_0", PrimaryKeyValue.fromString("hello_" + i));
            row.addColumn("attr_0", ColumnValue.fromString("hello_" + i));
            row.addColumn("attr_1", ColumnValue.fromBinary(("hello_" + i).getBytes("UTF-8")));
            expect.add(row);
        }
        testIllegal(OTSOpType.PUT_ROW, input, expect, errorExpect, 101, null);
        testIllegal(OTSOpType.UPDATE_ROW, input, expect, errorExpect, 101, null);
    }
    
    // PUT / UPDATE
    // 输入：构造100行数据，数据量为1MB + 1B，期望：这50行数据成功的写入，SDK被调用101次（覆盖场景：1.2、3.1、2.1.1.1，主要是测试整体失败且单行重试全部成功的场景）
    @Test
    public void testCase17() throws UnsupportedEncodingException {
        List<Record> input = new ArrayList<Record>();
        List<OTSRow> expect = new ArrayList<OTSRow>();

        StringBuilder pkSb = new StringBuilder();
        for (int i = 0; i < 478; i++) {
            pkSb.append("p"); // 478 + 4(pk_0) + 3(000) = 485
        }

        StringBuilder attrSb = new StringBuilder();
        for (int i = 0; i < 4991; i++) {
            attrSb.append("k");// 4991 + 6(attr_*) + 3(000) =  5000
        }

        for (int i = 0; i < 99; i++) {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn(String.format("%s%03d", pkSb.toString(), i)));
            r.addColumn(new StringColumn(String.format("%s%03d", attrSb.toString(), i)));
            r.addColumn(new BytesColumn((String.format("%s%03d", attrSb.toString(),i)).getBytes("UTF-8")));
            input.add(r);

            OTSRow row = new OTSRow();
            row.addPK("pk_0", PrimaryKeyValue.fromString(String.format("%s%03d", pkSb.toString(), i)));
            row.addColumn("attr_0", ColumnValue.fromString(String.format("%s%03d", attrSb.toString(), i)));
            row.addColumn("attr_1", ColumnValue.fromBinary((String.format("%s%03d", attrSb.toString(), i)).getBytes("UTF-8")));
            expect.add(row);
        }

        StringBuilder newSb = new StringBuilder();
        for (int i = 0; i < 10562; i++) {
            newSb.append("p");
        }

        for (int i = 99; i < 100; i++) {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn(String.format("%s%03d", pkSb.toString(), i )));
            r.addColumn(new StringColumn(String.format("%s%03d%s", attrSb.toString(), i, newSb.toString())));
            r.addColumn(new BytesColumn((String.format("%s%03d", attrSb.toString(),i)).getBytes("UTF-8")));
            input.add(r);

            OTSRow row = new OTSRow();
            row.addPK("pk_0", PrimaryKeyValue.fromString(String.format("%s%03d", pkSb.toString(), i )));
            row.addColumn("attr_0", ColumnValue.fromString(String.format("%s%03d%s", attrSb.toString(), i, newSb.toString())));
            row.addColumn("attr_1", ColumnValue.fromBinary((String.format("%s%03d", attrSb.toString(), i)).getBytes("UTF-8")));
            expect.add(row);
        }
        test(OTSOpType.PUT_ROW, input, expect, 101, null);
        test(OTSOpType.UPDATE_ROW, input, expect, 101, null);
    }


    // 其他
    // PUT / UPDATE
    // 输入：构造5行数据，每行数据的数据量为1MB+1B，期望：5行的数据写入失败，SDK被调用了6次（覆盖场景：1.2、3.1、2.1.1.3，主要是测试整体失败之后单行重试也失败）
    @Test
    public void testCase18() throws UnsupportedEncodingException {

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < (1024 *1024); i++) {
            sb.append("x");
        }

        List<Record> input = new ArrayList<Record>();
        List<RecordAndMessage> expect = new ArrayList<RecordAndMessage>();
        for (int i = 0; i < 5; i++) {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello_" + i));
            r.addColumn(new StringColumn("hello_" + i));
            r.addColumn(new BytesColumn((sb.toString()).getBytes("UTF-8")));
            input.add(r);
            expect.add((new TestPluginCollector(Configuration.newDefault(), null, null)).new RecordAndMessage(r, "BINARY ATTR SIZE > 64KB"));
        }
        testIllegal(OTSOpType.PUT_ROW, input, expect, 6, null);
        testIllegal(OTSOpType.UPDATE_ROW, input, expect, 6, null);
    }
    
    // 输入：构造10行数据，是OTS返回500错误，期望Task退出时的耗时在5分钟到5分30秒之间，且调用次数为19次
    @Test
    public void testCase19ForTimeout() throws UnsupportedEncodingException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < (64 *1024); i++) {
            sb.append("x");
        }

        List<Record> input = new ArrayList<Record>();
        List<RecordAndMessage> errorExpect = new ArrayList<RecordAndMessage>();
        for (int i = 0; i < 10; i++) {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello_" + i));
            r.addColumn(new StringColumn("hello_" + i));
            r.addColumn(new BytesColumn((sb.toString()).getBytes("UTF-8")));
            input.add(r);
            errorExpect.add((new TestPluginCollector(Configuration.newDefault(), null, null)).new RecordAndMessage(r, "Timeout"));
        }
        {
            long begin = (new Date()).getTime();
            testIllegal(new OTSException("Timeout", null, OTSErrorCode.REQUEST_TIMEOUT, "", 500), OTSOpType.PUT_ROW, input, errorExpect, 19, null);
            long end = (new Date()).getTime();
            
            long interval = (end - begin) / 1000;
            if (interval > 300 && interval <= 330) {
                assertTrue(true);
            } else {
                assertTrue(false);
            }
        }
        {
            long begin = (new Date()).getTime();
            testIllegal(new OTSException("Timeout", null, OTSErrorCode.REQUEST_TIMEOUT, "", 500), OTSOpType.UPDATE_ROW, input, errorExpect, 19, null);
            long end = (new Date()).getTime();
            
            long interval = (end - begin) / 1000;
            if (interval > 300 && interval <= 330) {
                assertTrue(true);
            } else {
                assertTrue(false);
            }
        }
    }
}
