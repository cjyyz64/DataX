package com.alibaba.datax.plugin.reader.otsreader.functiontest;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Test;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.OtsReaderSlaveProxy;
import com.alibaba.datax.plugin.reader.otsreader.common.BaseTest;
import com.alibaba.datax.plugin.reader.otsreader.common.Table;
import com.alibaba.datax.plugin.reader.otsreader.common.Utils;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConst;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSRange;
import com.alibaba.datax.plugin.reader.otsreader.utils.GsonParser;
import com.alibaba.datax.test.simulator.util.RecordSenderForTest;
import com.aliyun.openservices.ots.model.ColumnType;
import com.aliyun.openservices.ots.model.Direction;
import com.aliyun.openservices.ots.model.PrimaryKeyType;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.model.RowPrimaryKey;

/**
 * 主要是测试实际插入的数据和指定取出数据的范围的正确性
 * @author wanhong.chenwh@alibaba-inc.com
 *
 */
public class RangeFunctiontest{
    
    private static String tableName = "ots_reader_range_test";

    private static BaseTest base = new BaseTest("ots_reader_range_test");
    
    @AfterClass
    public static void close() {
        base.close();
    }
    
    private OTSConf getConf() {
        OTSConf conf = new OTSConf();
        conf.setEndpoint(base.getP().getString("endpoint"));
        conf.setAccessId(base.getP().getString("accessid"));
        conf.setAccesskey(base.getP().getString("accesskey"));
        conf.setInstanceName(base.getP().getString("instance-name"));
        conf.setTableName(tableName);
        
        List<OTSColumn> columns = new ArrayList<OTSColumn>();
        columns.add(OTSColumn.fromNormalColumn("pk_0"));

        conf.setColumns(columns);
        
        conf.setRetry(17);
        conf.setSleepInMilliSecond(100);
        return conf;
    }

    private long test(List<PrimaryKeyType> pkTypes, long begin, long rowCount, OTSConf conf, OTSRange range, Direction direction) throws Exception {
        // create table and prepare data
        {
            List<ColumnType> attriTypes = new ArrayList<ColumnType>();
            attriTypes.add(ColumnType.STRING);
            Table t = new Table(base.getOts(), tableName, pkTypes, attriTypes, 0.5);
            t.create();
            t.insertData(begin, rowCount);
        }
        
        List<Record> noteRecordForTest = new LinkedList<Record>();
        RecordSender sender = new RecordSenderForTest(null, noteRecordForTest);

        OtsReaderSlaveProxy slave = new OtsReaderSlaveProxy();
        
        Configuration configuration = Configuration.newDefault();
        configuration.set(OTSConst.OTS_CONF, GsonParser.confToJson(conf));
        configuration.set(OTSConst.OTS_RANGE, GsonParser.rangeToJson(range));
        configuration.set(OTSConst.OTS_DIRECTION, GsonParser.directionToJson(direction));
        
        slave.read(sender, configuration);
        
        assertEquals(true, Utils.checkOutput(base.getOts(), conf, noteRecordForTest));
        
        return noteRecordForTest.size();
    }
    
    // integer 相关case
    
    /**
     * Partition Key为Int时，第一个PK逆序
     * 输入：
     *      begin = [INF_MAX, INF_MAX, INF_MAX, INF_MAX]
     *      end   = [INF_MIN, INF_MIN, INF_MIN, INF_MIN]
     * 期望：导出指定的数据，且数据正确
     * @throws Exception 
     */
    @Test
    public void testReverseRangeByIntegerPartitionKey1() throws Exception {
        List<PrimaryKeyType> pkTypes = new ArrayList<PrimaryKeyType>();
        pkTypes.add(PrimaryKeyType.INTEGER);
        pkTypes.add(PrimaryKeyType.STRING);
        pkTypes.add(PrimaryKeyType.STRING);
        pkTypes.add(PrimaryKeyType.STRING);
        
        RowPrimaryKey begin = new RowPrimaryKey();
        begin.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MAX);
        begin.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.INF_MAX);
        begin.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.INF_MAX);
        begin.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.INF_MAX);
        
        RowPrimaryKey end = new RowPrimaryKey();
        end.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MIN);
        end.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.INF_MIN);
        end.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.INF_MIN);
        end.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.INF_MIN);
        
        OTSConf conf = this.getConf();
        
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        rangeBegin.add(PrimaryKeyValue.INF_MAX);
        rangeBegin.add(PrimaryKeyValue.INF_MAX);
        rangeBegin.add(PrimaryKeyValue.INF_MAX);
        rangeBegin.add(PrimaryKeyValue.INF_MAX);
        conf.setRangeBegin(rangeBegin);

        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        rangeEnd.add(PrimaryKeyValue.INF_MIN);
        rangeEnd.add(PrimaryKeyValue.INF_MIN);
        rangeEnd.add(PrimaryKeyValue.INF_MIN);
        rangeEnd.add(PrimaryKeyValue.INF_MIN);
        conf.setRangeEnd(rangeEnd);
        
        OTSRange range = new OTSRange();
        range.setBegin(begin);
        range.setEnd(end);

        long expectCount = test(pkTypes, -100, 200, conf, range, Direction.BACKWARD);
        
        assertEquals(200, expectCount);
    }
    
    /**
     * Partition Key为Int时，第一个PK相等，第二个PK逆序
     * 输入：
     *      begin = [10, INF_MAX, INF_MAX, INF_MAX]
     *      end   = [10, INF_MIN, INF_MIN, INF_MIN]
     * 期望：导出指定的数据，且数据正确
     * @throws Exception 
     */
    @Test
    public void testReverseRangeByIntegerPartitionKey2() throws Exception {
        List<PrimaryKeyType> pkTypes = new ArrayList<PrimaryKeyType>();
        pkTypes.add(PrimaryKeyType.INTEGER);
        pkTypes.add(PrimaryKeyType.STRING);
        pkTypes.add(PrimaryKeyType.STRING);
        pkTypes.add(PrimaryKeyType.STRING);
        
        RowPrimaryKey begin = new RowPrimaryKey();
        begin.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(10));
        begin.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.INF_MAX);
        begin.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.INF_MAX);
        begin.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.INF_MAX);
        
        RowPrimaryKey end = new RowPrimaryKey();
        end.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(10));
        end.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.INF_MIN);
        end.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.INF_MIN);
        end.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.INF_MIN);
        
        OTSConf conf = this.getConf();
        
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        rangeBegin.add(PrimaryKeyValue.fromLong(10));
        rangeBegin.add(PrimaryKeyValue.INF_MAX);
        rangeBegin.add(PrimaryKeyValue.INF_MAX);
        rangeBegin.add(PrimaryKeyValue.INF_MAX);
        conf.setRangeBegin(rangeBegin);

        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        rangeEnd.add(PrimaryKeyValue.fromLong(10));
        rangeEnd.add(PrimaryKeyValue.INF_MIN);
        rangeEnd.add(PrimaryKeyValue.INF_MIN);
        rangeEnd.add(PrimaryKeyValue.INF_MIN);
        conf.setRangeEnd(rangeEnd);
        
        OTSRange range = new OTSRange();
        range.setBegin(begin);
        range.setEnd(end);

        long expectCount = test(pkTypes, -100, 200, conf, range, Direction.BACKWARD);
        
        assertEquals(1, expectCount);
    }
    
    /**
     * 实际插入数据范围为配置指定的range范围的子集
     * 输入：
     *      1.构造数据，数据范围-300~100
     *      2.设定请求范围是-600~700
     * 期望：能获取到所有数据，且数据正确
     * @throws Exception 
     */
    @Test
    public void testSpecialIntegerRange1() throws Exception {
        List<PrimaryKeyType> pkTypes = new ArrayList<PrimaryKeyType>();
        pkTypes.add(PrimaryKeyType.INTEGER);
        pkTypes.add(PrimaryKeyType.STRING);
        pkTypes.add(PrimaryKeyType.STRING);
        pkTypes.add(PrimaryKeyType.STRING);
        
        RowPrimaryKey begin = new RowPrimaryKey();
        begin.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(-600));
        begin.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.INF_MIN);
        begin.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.INF_MIN);
        begin.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.INF_MIN);
        
        RowPrimaryKey end = new RowPrimaryKey();
        end.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(700));
        end.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.INF_MAX);
        end.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.INF_MAX);
        end.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.INF_MAX);
        
        OTSConf conf = this.getConf();
        
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        rangeBegin.add(PrimaryKeyValue.fromLong(-600));
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        conf.setRangeBegin(rangeBegin);

        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        rangeEnd.add(PrimaryKeyValue.fromLong(700));
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        conf.setRangeEnd(rangeEnd);
        
        OTSRange range = new OTSRange();
        range.setBegin(begin);
        range.setEnd(end);

        long expectCount = test(pkTypes, -300, 400, conf, range, Direction.FORWARD);
        
        assertEquals(400, expectCount);
    }
    
    /**
     * 指定的range范围是实际插入数据范围的子集
     * 输入：
     *      1.构造数据，数据范围-300~100
     *      2.设定请求范围是-1~10
     * 期望：能获取到指定的数据，且数据正确
     * @throws Exception 
     */
    @Test
    public void testSpecialIntegerRange2() throws Exception {
        List<PrimaryKeyType> pkTypes = new ArrayList<PrimaryKeyType>();
        pkTypes.add(PrimaryKeyType.INTEGER);
        pkTypes.add(PrimaryKeyType.STRING);
        pkTypes.add(PrimaryKeyType.STRING);
        pkTypes.add(PrimaryKeyType.STRING);
        
        RowPrimaryKey begin = new RowPrimaryKey();
        begin.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(-1));
        begin.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.INF_MIN);
        begin.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.INF_MIN);
        begin.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.INF_MIN);
        
        RowPrimaryKey end = new RowPrimaryKey();
        end.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(10));
        end.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.INF_MAX);
        end.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.INF_MAX);
        end.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.INF_MAX);
        
        OTSConf conf = this.getConf();
        
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        rangeBegin.add(PrimaryKeyValue.fromLong(-1));
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        conf.setRangeBegin(rangeBegin);

        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        rangeEnd.add(PrimaryKeyValue.fromLong(10));
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        conf.setRangeEnd(rangeEnd);
        
        OTSRange range = new OTSRange();
        range.setBegin(begin);
        range.setEnd(end);

        long expectCount = test(pkTypes, -300, 400, conf, range, Direction.FORWARD);
        
        assertEquals(12, expectCount);
    }
    
    /**
     * 左交集
     * 输入：
     *      1.构造数据，数据范围-100~100
     *      2.设定请求范围是-200~0
     * 期望：能获取到指定的数据，且数据正确
     * @throws Exception 
     */
    @Test
    public void testSpecialIntegerRange3() throws Exception {
        List<PrimaryKeyType> pkTypes = new ArrayList<PrimaryKeyType>();
        pkTypes.add(PrimaryKeyType.INTEGER);
        pkTypes.add(PrimaryKeyType.STRING);
        pkTypes.add(PrimaryKeyType.STRING);
        pkTypes.add(PrimaryKeyType.STRING);
        
        RowPrimaryKey begin = new RowPrimaryKey();
        begin.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(-200));
        begin.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.INF_MIN);
        begin.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.INF_MIN);
        begin.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.INF_MIN);
        
        RowPrimaryKey end = new RowPrimaryKey();
        end.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(0));
        end.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.INF_MAX);
        end.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.INF_MAX);
        end.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.INF_MAX);
        
        OTSConf conf = this.getConf();
        
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        rangeBegin.add(PrimaryKeyValue.fromLong(-200));
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        conf.setRangeBegin(rangeBegin);

        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        rangeEnd.add(PrimaryKeyValue.fromLong(0));
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        conf.setRangeEnd(rangeEnd);
        
        OTSRange range = new OTSRange();
        range.setBegin(begin);
        range.setEnd(end);

        long expectCount = test(pkTypes, -100, 200, conf, range, Direction.FORWARD);
        
        assertEquals(101, expectCount);
    }
    
    /**
     * 右交集
     * 输入：
     *      1.构造数据，数据范围-100~100
     *      2.设定请求范围是0~200
     * 期望：能获取到指定的数据，且数据正确
     * @throws Exception 
     */
    @Test
    public void testSpecialIntegerRange4() throws Exception {
        List<PrimaryKeyType> pkTypes = new ArrayList<PrimaryKeyType>();
        pkTypes.add(PrimaryKeyType.INTEGER);
        pkTypes.add(PrimaryKeyType.STRING);
        pkTypes.add(PrimaryKeyType.STRING);
        pkTypes.add(PrimaryKeyType.STRING);
        
        RowPrimaryKey begin = new RowPrimaryKey();
        begin.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(0));
        begin.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.INF_MIN);
        begin.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.INF_MIN);
        begin.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.INF_MIN);
        
        RowPrimaryKey end = new RowPrimaryKey();
        end.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(200));
        end.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.INF_MAX);
        end.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.INF_MAX);
        end.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.INF_MAX);
        
        OTSConf conf = this.getConf();
        
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        rangeBegin.add(PrimaryKeyValue.fromLong(0));
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        conf.setRangeBegin(rangeBegin);

        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        rangeEnd.add(PrimaryKeyValue.fromLong(200));
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        conf.setRangeEnd(rangeEnd);
        
        OTSRange range = new OTSRange();
        range.setBegin(begin);
        range.setEnd(end);

        long expectCount = test(pkTypes, -100, 200, conf, range, Direction.FORWARD);
        
        assertEquals(100, expectCount);
    }
    
    /**
     * 左无交集（包括边界情况）
     * 输入：
     *      1.构造数据，数据范围-100~100
     *      2.设定请求范围是-200~-100
     * 期望：数据为空
     * @throws Exception 
     */
    @Test
    public void testSpecialIntegerRange5() throws Exception {
        List<PrimaryKeyType> pkTypes = new ArrayList<PrimaryKeyType>();
        pkTypes.add(PrimaryKeyType.INTEGER);
        pkTypes.add(PrimaryKeyType.STRING);
        pkTypes.add(PrimaryKeyType.STRING);
        pkTypes.add(PrimaryKeyType.STRING);
        
        RowPrimaryKey begin = new RowPrimaryKey();
        begin.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(-200));
        begin.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromString("-200"));
        begin.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.fromString("-200"));
        begin.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.fromString("-200"));
        
        RowPrimaryKey end = new RowPrimaryKey();
        end.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(-100));
        end.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromString("-100"));
        end.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.fromString("-100"));
        end.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.fromString("-100"));
        
        OTSConf conf = this.getConf();
        
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        rangeBegin.add(PrimaryKeyValue.fromLong(-200));
        rangeBegin.add(PrimaryKeyValue.fromString("-200"));
        rangeBegin.add(PrimaryKeyValue.fromString("-200"));
        rangeBegin.add(PrimaryKeyValue.fromString("-200"));
        conf.setRangeBegin(rangeBegin);

        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        rangeEnd.add(PrimaryKeyValue.fromLong(-100));
        rangeEnd.add(PrimaryKeyValue.fromString("-100"));
        rangeEnd.add(PrimaryKeyValue.fromString("-100"));
        rangeEnd.add(PrimaryKeyValue.fromString("-100"));
        conf.setRangeEnd(rangeEnd);
        
        OTSRange range = new OTSRange();
        range.setBegin(begin);
        range.setEnd(end);

        long expectCount = test(pkTypes, -100, 200, conf, range, Direction.FORWARD);
        
        assertEquals(0, expectCount);
    }
    
    /**
     * 右无交集（包括边界情况）
     * 输入：
     *      1.构造数据，数据范围-100~100
     *      2.设定请求范围是100~200
     * 期望：数据为空
     * @throws Exception 
     */
    @Test
    public void testSpecialIntegerRange6() throws Exception {
        List<PrimaryKeyType> pkTypes = new ArrayList<PrimaryKeyType>();
        pkTypes.add(PrimaryKeyType.INTEGER);
        pkTypes.add(PrimaryKeyType.STRING);
        pkTypes.add(PrimaryKeyType.STRING);
        pkTypes.add(PrimaryKeyType.STRING);
        
        RowPrimaryKey begin = new RowPrimaryKey();
        begin.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(100));
        begin.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromString("100"));
        begin.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.fromString("100"));
        begin.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.fromString("100"));
        
        RowPrimaryKey end = new RowPrimaryKey();
        end.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(200));
        end.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromString("200"));
        end.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.fromString("200"));
        end.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.fromString("200"));
        
        OTSConf conf = this.getConf();
        
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        rangeBegin.add(PrimaryKeyValue.fromLong(100));
        rangeBegin.add(PrimaryKeyValue.fromString("100"));
        rangeBegin.add(PrimaryKeyValue.fromString("100"));
        rangeBegin.add(PrimaryKeyValue.fromString("100"));
        conf.setRangeBegin(rangeBegin);

        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        rangeEnd.add(PrimaryKeyValue.fromLong(200));
        rangeEnd.add(PrimaryKeyValue.fromString("200"));
        rangeEnd.add(PrimaryKeyValue.fromString("200"));
        rangeEnd.add(PrimaryKeyValue.fromString("200"));
        conf.setRangeEnd(rangeEnd);
        
        OTSRange range = new OTSRange();
        range.setBegin(begin);
        range.setEnd(end);

        long expectCount = test(pkTypes, -100, 200, conf, range, Direction.FORWARD);
        
        assertEquals(0, expectCount);
    }
    
    // string 相关case
    
    /**
     * Partition Key为String时，第一个PK逆序
     * 输入：
     *      begin = [INF_MAX, INF_MAX, INF_MAX, INF_MAX]
     *      end   = [INF_MIN, INF_MIN, INF_MIN, INF_MIN]
     * 期望：导出指定的数据，且数据正确
     * @throws Exception 
     */
    @Test
    public void testReverseRangeByStringPartitionKey1() throws Exception {
        List<PrimaryKeyType> pkTypes = new ArrayList<PrimaryKeyType>();
        pkTypes.add(PrimaryKeyType.STRING);
        pkTypes.add(PrimaryKeyType.STRING);
        pkTypes.add(PrimaryKeyType.INTEGER);
        pkTypes.add(PrimaryKeyType.INTEGER);
        
        RowPrimaryKey begin = new RowPrimaryKey();
        begin.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MAX);
        begin.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.INF_MAX);
        begin.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.INF_MAX);
        begin.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.INF_MAX);
        
        RowPrimaryKey end = new RowPrimaryKey();
        end.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MIN);
        end.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.INF_MIN);
        end.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.INF_MIN);
        end.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.INF_MIN);
        
        OTSConf conf = this.getConf();
        
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        rangeBegin.add(PrimaryKeyValue.INF_MAX);
        rangeBegin.add(PrimaryKeyValue.INF_MAX);
        rangeBegin.add(PrimaryKeyValue.INF_MAX);
        rangeBegin.add(PrimaryKeyValue.INF_MAX);
        conf.setRangeBegin(rangeBegin);

        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        rangeEnd.add(PrimaryKeyValue.INF_MIN);
        rangeEnd.add(PrimaryKeyValue.INF_MIN);
        rangeEnd.add(PrimaryKeyValue.INF_MIN);
        rangeEnd.add(PrimaryKeyValue.INF_MIN);
        conf.setRangeEnd(rangeEnd);
        
        OTSRange range = new OTSRange();
        range.setBegin(begin);
        range.setEnd(end);

        long expectCount = test(pkTypes, -100, 100, conf, range, Direction.BACKWARD);
        
        assertEquals(100, expectCount);
    }
    
    /**
     * Partition Key为String时，第一个PK相等，第二个PK逆序
     * 输入：
     *      begin = ["-10", INF_MAX, INF_MAX, INF_MAX]
     *      end   = ["-10", INF_MIN, INF_MIN, INF_MIN]
     * 期望：导出指定的数据，且数据正确
     * @throws Exception 
     */
    @Test
    public void testReverseRangeByStringPartitionKey2() throws Exception {
        List<PrimaryKeyType> pkTypes = new ArrayList<PrimaryKeyType>();
        pkTypes.add(PrimaryKeyType.STRING);
        pkTypes.add(PrimaryKeyType.STRING);
        pkTypes.add(PrimaryKeyType.INTEGER);
        pkTypes.add(PrimaryKeyType.INTEGER);
        
        RowPrimaryKey begin = new RowPrimaryKey();
        begin.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("-10"));
        begin.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.INF_MAX);
        begin.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.INF_MAX);
        begin.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.INF_MAX);
        
        RowPrimaryKey end = new RowPrimaryKey();
        end.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("-10"));
        end.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.INF_MIN);
        end.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.INF_MIN);
        end.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.INF_MIN);
        
        OTSConf conf = this.getConf();
        
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        rangeBegin.add(PrimaryKeyValue.fromString("-10"));
        rangeBegin.add(PrimaryKeyValue.INF_MAX);
        rangeBegin.add(PrimaryKeyValue.INF_MAX);
        rangeBegin.add(PrimaryKeyValue.INF_MAX);
        conf.setRangeBegin(rangeBegin);

        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        rangeEnd.add(PrimaryKeyValue.fromString("-10"));
        rangeEnd.add(PrimaryKeyValue.INF_MIN);
        rangeEnd.add(PrimaryKeyValue.INF_MIN);
        rangeEnd.add(PrimaryKeyValue.INF_MIN);
        conf.setRangeEnd(rangeEnd);
        
        OTSRange range = new OTSRange();
        range.setBegin(begin);
        range.setEnd(end);

        long expectCount = test(pkTypes, -100, 150, conf, range, Direction.BACKWARD);
        
        assertEquals(1, expectCount);
    }
    
    /**
     * 实际插入数据范围为配置指定的range范围的子集
     * 输入：
     *      1.构造数据，数据范围-300~100
     *      2.设定请求范围是-600~700
     * 期望：能获取到所有数据，且数据正确
     * @throws Exception 
     */
    @Test
    public void testSpecialStringRange1() throws Exception {
        List<PrimaryKeyType> pkTypes = new ArrayList<PrimaryKeyType>();
        pkTypes.add(PrimaryKeyType.STRING);
        pkTypes.add(PrimaryKeyType.STRING);
        pkTypes.add(PrimaryKeyType.INTEGER);
        pkTypes.add(PrimaryKeyType.INTEGER);
        
        RowPrimaryKey begin = new RowPrimaryKey();
        begin.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("-600"));
        begin.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.INF_MIN);
        begin.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.INF_MIN);
        begin.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.INF_MIN);
        
        RowPrimaryKey end = new RowPrimaryKey();
        end.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("700"));
        end.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.INF_MAX);
        end.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.INF_MAX);
        end.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.INF_MAX);
        
        OTSConf conf = this.getConf();
        
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        rangeBegin.add(PrimaryKeyValue.fromString("-600"));
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        conf.setRangeBegin(rangeBegin);

        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        rangeEnd.add(PrimaryKeyValue.fromString("700"));
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        conf.setRangeEnd(rangeEnd);
        
        OTSRange range = new OTSRange();
        range.setBegin(begin);
        range.setEnd(end);

        long expectCount = test(pkTypes, -300, 400, conf, range, Direction.FORWARD);
        
        assertEquals(111, expectCount);
    }
    
    /**
     * 指定的range范围是实际插入数据范围的子集
     * 输入：
     *      1.构造数据，数据范围-300~100
     *      2.设定请求范围是-1~10
     * 期望：能获取到指定的数据，且数据正确
     * @throws Exception 
     */
    @Test
    public void testSpecialStringRange2() throws Exception {
        List<PrimaryKeyType> pkTypes = new ArrayList<PrimaryKeyType>();
        pkTypes.add(PrimaryKeyType.STRING);
        pkTypes.add(PrimaryKeyType.STRING);
        pkTypes.add(PrimaryKeyType.INTEGER);
        pkTypes.add(PrimaryKeyType.INTEGER);
        
        RowPrimaryKey begin = new RowPrimaryKey();
        begin.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("-1"));
        begin.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.INF_MIN);
        begin.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.INF_MIN);
        begin.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.INF_MIN);
        
        RowPrimaryKey end = new RowPrimaryKey();
        end.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("10"));
        end.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.INF_MAX);
        end.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.INF_MAX);
        end.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.INF_MAX);
        
        OTSConf conf = this.getConf();
        
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        rangeBegin.add(PrimaryKeyValue.fromString("-1"));
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        conf.setRangeBegin(rangeBegin);

        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        rangeEnd.add(PrimaryKeyValue.fromString("10"));
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        conf.setRangeEnd(rangeEnd);
        
        OTSRange range = new OTSRange();
        range.setBegin(begin);
        range.setEnd(end);

        long expectCount = test(pkTypes, -300, 400, conf, range, Direction.FORWARD);
        
        assertEquals(303, expectCount);
    }
    
    /**
     * 左交集
     * 输入：
     *      1.构造数据，数据范围-100~100
     *      2.设定请求范围是-200~0
     * 期望：能获取到指定的数据，且数据正确
     * @throws Exception 
     */
    @Test
    public void testSpecialStringRange3() throws Exception {
        List<PrimaryKeyType> pkTypes = new ArrayList<PrimaryKeyType>();
        pkTypes.add(PrimaryKeyType.STRING);
        pkTypes.add(PrimaryKeyType.STRING);
        pkTypes.add(PrimaryKeyType.INTEGER);
        pkTypes.add(PrimaryKeyType.INTEGER);
        
        RowPrimaryKey begin = new RowPrimaryKey();
        begin.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("-200"));
        begin.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.INF_MIN);
        begin.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.INF_MIN);
        begin.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.INF_MIN);
        
        RowPrimaryKey end = new RowPrimaryKey();
        end.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("0"));
        end.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.INF_MAX);
        end.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.INF_MAX);
        end.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.INF_MAX);
        
        OTSConf conf = this.getConf();
        
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        rangeBegin.add(PrimaryKeyValue.fromString("-200"));
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        conf.setRangeBegin(rangeBegin);

        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        rangeEnd.add(PrimaryKeyValue.fromString("0"));
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        conf.setRangeEnd(rangeEnd);
        
        OTSRange range = new OTSRange();
        range.setBegin(begin);
        range.setEnd(end);

        long expectCount = test(pkTypes, -100, 200, conf, range, Direction.FORWARD);
        
        assertEquals(87, expectCount);
    }
    
    /**
     * 右交集
     * 输入：
     *      1.构造数据，数据范围-100~100
     *      2.设定请求范围是0~200
     * 期望：能获取到指定的数据，且数据正确
     * @throws Exception 
     */
    @Test
    public void testSpecialStringRange4() throws Exception {
        List<PrimaryKeyType> pkTypes = new ArrayList<PrimaryKeyType>();
        pkTypes.add(PrimaryKeyType.STRING);
        pkTypes.add(PrimaryKeyType.STRING);
        pkTypes.add(PrimaryKeyType.INTEGER);
        pkTypes.add(PrimaryKeyType.INTEGER);
        
        RowPrimaryKey begin = new RowPrimaryKey();
        begin.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("0"));
        begin.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.INF_MIN);
        begin.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.INF_MIN);
        begin.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.INF_MIN);
        
        RowPrimaryKey end = new RowPrimaryKey();
        end.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("200"));
        end.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.INF_MAX);
        end.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.INF_MAX);
        end.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.INF_MAX);
        
        OTSConf conf = this.getConf();
        
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        rangeBegin.add(PrimaryKeyValue.fromString("0"));
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        conf.setRangeBegin(rangeBegin);

        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        rangeEnd.add(PrimaryKeyValue.fromString("200"));
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        conf.setRangeEnd(rangeEnd);
        
        OTSRange range = new OTSRange();
        range.setBegin(begin);
        range.setEnd(end);

        long expectCount = test(pkTypes, -100, 200, conf, range, Direction.FORWARD);
        
        assertEquals(14, expectCount);
    }
    
    /**
     * 左无交集（包括边界情况）
     * 输入：
     *      1.构造数据，数据范围-200~-300
     *      2.设定请求范围是-100~-200
     * 期望：数据为空
     * @throws Exception 
     */
    @Test
    public void testSpecialStringRange5() throws Exception {
        List<PrimaryKeyType> pkTypes = new ArrayList<PrimaryKeyType>();
        pkTypes.add(PrimaryKeyType.STRING);
        pkTypes.add(PrimaryKeyType.STRING);
        pkTypes.add(PrimaryKeyType.INTEGER);
        pkTypes.add(PrimaryKeyType.INTEGER);
        
        RowPrimaryKey begin = new RowPrimaryKey();
        begin.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("-100"));
        begin.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromString("-100"));
        begin.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.fromLong(-100));
        begin.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.fromLong(-100));
        
        RowPrimaryKey end = new RowPrimaryKey();
        end.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("-200"));
        end.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromString("-200"));
        end.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.fromLong(-200));
        end.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.fromLong(-200));
        
        OTSConf conf = this.getConf();
        
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        rangeBegin.add(PrimaryKeyValue.fromString("-100"));
        rangeBegin.add(PrimaryKeyValue.fromString("-100"));
        rangeBegin.add(PrimaryKeyValue.fromLong(-100));
        rangeBegin.add(PrimaryKeyValue.fromLong(-100));
        conf.setRangeBegin(rangeBegin);

        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        rangeEnd.add(PrimaryKeyValue.fromString("-200"));
        rangeEnd.add(PrimaryKeyValue.fromString("-200"));
        rangeEnd.add(PrimaryKeyValue.fromLong(-200));
        rangeEnd.add(PrimaryKeyValue.fromLong(-200));
        conf.setRangeEnd(rangeEnd);
        
        OTSRange range = new OTSRange();
        range.setBegin(begin);
        range.setEnd(end);

        long expectCount = test(pkTypes, -300, 100, conf, range, Direction.FORWARD);
        
        assertEquals(0, expectCount);
    }
    
    /**
     * 右无交集（包括边界情况）
     * 输入：
     *      1.构造数据，数据范围-200~-300
     *      2.设定请求范围是-300~-400
     * 期望：数据为空
     * @throws Exception 
     */
    @Test
    public void testSpecialStringRange6() throws Exception {
        List<PrimaryKeyType> pkTypes = new ArrayList<PrimaryKeyType>();
        pkTypes.add(PrimaryKeyType.STRING);
        pkTypes.add(PrimaryKeyType.STRING);
        pkTypes.add(PrimaryKeyType.INTEGER);
        pkTypes.add(PrimaryKeyType.INTEGER);
        
        RowPrimaryKey begin = new RowPrimaryKey();
        begin.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("-300"));
        begin.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromString("-300"));
        begin.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.fromLong(-300));
        begin.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.fromLong(-300));
        
        RowPrimaryKey end = new RowPrimaryKey();
        end.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("-400"));
        end.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromString("-400"));
        end.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.fromLong(-400));
        end.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.fromLong(-400));
        
        OTSConf conf = this.getConf();
        
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        rangeBegin.add(PrimaryKeyValue.fromString("-300"));
        rangeBegin.add(PrimaryKeyValue.fromString("-300"));
        rangeBegin.add(PrimaryKeyValue.fromLong(-300));
        rangeBegin.add(PrimaryKeyValue.fromLong(-300));
        conf.setRangeBegin(rangeBegin);

        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        rangeEnd.add(PrimaryKeyValue.fromString("-400"));
        rangeEnd.add(PrimaryKeyValue.fromString("-400"));
        rangeEnd.add(PrimaryKeyValue.fromLong(-400));
        rangeEnd.add(PrimaryKeyValue.fromLong(-400));
        conf.setRangeEnd(rangeEnd);
        
        OTSRange range = new OTSRange();
        range.setBegin(begin);
        range.setEnd(end);

        long expectCount = test(pkTypes, -299, 100, conf, range, Direction.FORWARD);
        
        assertEquals(0, expectCount);
    }
}
