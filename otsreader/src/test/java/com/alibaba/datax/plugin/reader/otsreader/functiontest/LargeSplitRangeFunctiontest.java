package com.alibaba.datax.plugin.reader.otsreader.functiontest;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.OtsReaderMasterProxy;
import com.alibaba.datax.plugin.reader.otsreader.common.ReaderConf;
import com.alibaba.datax.plugin.reader.otsreader.common.Table;
import com.alibaba.datax.plugin.reader.otsreader.common.Utils;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.plugin.reader.otsreader.utils.Common;
import com.aliyun.openservices.ots.OTSClient;
import com.aliyun.openservices.ots.model.ColumnType;
import com.aliyun.openservices.ots.model.PrimaryKeyType;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;

/**
 * 主要是测试Reader是否能够拆分非常多的切分点
 * @author redchen
 *
 */
public class LargeSplitRangeFunctiontest {
    
    private static String tableName = "ots_reader_proxy_extreme_split_functiontest";
    private static Configuration p = Utils.loadConf();
    private static OTSClient ots = new OTSClient(
            p.getString("endpoint"), 
            p.getString("accessid"), 
            p.getString("accesskey"), 
            p.getString("instance-name"));
    
    private ReaderConf readerConf = null;
    
    @AfterClass
    public static void close() {
        ots.shutdown();
    }
    
    @Before
    public void setUp() throws Exception {

        readerConf = new ReaderConf();
        OTSConf conf = new OTSConf();
        Configuration p = Utils.loadConf();
        conf.setEndpoint(p.getString("endpoint"));
        conf.setAccessId(p.getString("accessid"));
        conf.setAccessKey(p.getString("accesskey"));
        conf.setInstanceName(p.getString("instance-name"));
        conf.setTableName(tableName);

        List<OTSColumn> columns = new ArrayList<OTSColumn>();
        columns.add(OTSColumn.fromNormalColumn("col0"));
        
        conf.setColumns(columns);

        conf.setRangeBegin(Collections.<PrimaryKeyValue> emptyList());
        conf.setRangeEnd(Collections.<PrimaryKeyValue> emptyList());
        conf.setRangeSplit(null);

        readerConf.setConf(conf);
    }

    /**
     * 测是系统自动切分String时，输入10000的adviceNum，Reader能够正常的生成对应的Task
     * 输入：
     *      begin = []
     *      end = []
     *      adviceNum = 10000
     *      split = null
     * 期望：系统能正常切分出10000个Task
     * @throws Exception
     */
    @Test
    public void testStringAutoSplitBy10000AdviceNum() throws Exception {
        {
            List<PrimaryKeyType> pkType = new ArrayList<PrimaryKeyType>();
            pkType.add(PrimaryKeyType.STRING);
            pkType.add(PrimaryKeyType.INTEGER);
            pkType.add(PrimaryKeyType.STRING);
            pkType.add(PrimaryKeyType.INTEGER);
            List<ColumnType> attriTypes = new ArrayList<ColumnType>();
            attriTypes.add(ColumnType.STRING);
            Table t = new Table(ots, tableName, pkType, attriTypes, 0.5);
            t.create();
            t.insertData(0, 200);
        }
        Configuration p = Configuration.from(readerConf.toString());
        
        OtsReaderMasterProxy master = new OtsReaderMasterProxy();

        master.init(p);
        List<Configuration> cs = master.split(10000);
        master.close();
        
        assertEquals(10000, cs.size());
    }
    
    /**
     * 测是系统自动切分Integer时，输入10000的adviceNum，Reader能够正常的生成对应的Task
     * 输入：
     *      begin = []
     *      end = []
     *      adviceNum = 10000
     *      split = null
     * 期望：系统能正常切分出10000个Task
     * @throws Exception 
     */
    @Test
    public void testIntegerAutoSplitBy10000AdviceNum() throws Exception {
        {
            List<PrimaryKeyType> pkType = new ArrayList<PrimaryKeyType>();
            pkType.add(PrimaryKeyType.INTEGER);
            pkType.add(PrimaryKeyType.INTEGER);
            pkType.add(PrimaryKeyType.STRING);
            pkType.add(PrimaryKeyType.INTEGER);
            List<ColumnType> attriTypes = new ArrayList<ColumnType>();
            attriTypes.add(ColumnType.STRING);
            Table t = new Table(ots, tableName, pkType, attriTypes, 0.5);
            t.create();
            t.insertData(0, 10010);
        }
        Configuration p = Configuration.from(readerConf.toString());
        
        OtsReaderMasterProxy master = new OtsReaderMasterProxy();

        master.init(p);
        List<Configuration> cs = master.split(10000);
        master.close();
        
        assertEquals(10000, cs.size());
    }
//    
    
    /**
     *  测试在用户输入10000个切分点时，Reader能够正常的生成对应的Task
     *  输入：
     *      begin = []
     *      end = []
     *      adviceNum = 9
     *      split = ["0" ~ "10000"]
     * 期望：系统能正常切分出10000个Task
     * @throws Exception 
     */
    @Test
    public void testStringUserSpcialPointBy10000Point() throws Exception {
        {
            List<PrimaryKeyType> pkType = new ArrayList<PrimaryKeyType>();
            pkType.add(PrimaryKeyType.STRING);
            pkType.add(PrimaryKeyType.INTEGER);
            pkType.add(PrimaryKeyType.STRING);
            pkType.add(PrimaryKeyType.INTEGER);
            List<ColumnType> attriTypes = new ArrayList<ColumnType>();
            attriTypes.add(ColumnType.STRING);
            Table t = new Table(ots, tableName, pkType, attriTypes, 0.5);
            t.create();
            t.insertData(0, 10100);
        }
        OTSConf conf = readerConf.getConf();
        List<PrimaryKeyValue> rangeSplit  = new ArrayList<PrimaryKeyValue>();
        
        for (int i = -5000; i < 5000; i++) {
            rangeSplit.add(PrimaryKeyValue.fromString(String.valueOf(i)));
        }
        
        Collections.sort(rangeSplit, new Comparator<PrimaryKeyValue>(){
            public int compare(PrimaryKeyValue arg0, PrimaryKeyValue arg1) {
                return Common.primaryKeyValueCmp(arg0, arg1);
            }
        });
        
        conf.setRangeSplit(rangeSplit);
        
        Configuration p = Configuration.from(readerConf.toString());
        
        OtsReaderMasterProxy master = new OtsReaderMasterProxy();

        master.init(p);
        List<Configuration> cs = master.split(9);
        master.close();
        
        assertEquals(10001, cs.size());
    }
    
    /**
     *  测试在用户输入10000个切分点时，Reader能够正常的生成对应的Task
     *  输入：
     *      begin = []
     *      end = []
     *      adviceNum = 9
     *      split = [-5000~5000]
     * 期望：系统能正常切分出2000个Task
     * @throws Exception 
     */
    @Test
    public void testIntegerUserSpcialPointBy10000Point() throws Exception {
        {
            List<PrimaryKeyType> pkType = new ArrayList<PrimaryKeyType>();
            pkType.add(PrimaryKeyType.INTEGER);
            pkType.add(PrimaryKeyType.INTEGER);
            pkType.add(PrimaryKeyType.STRING);
            pkType.add(PrimaryKeyType.STRING);
            List<ColumnType> attriTypes = new ArrayList<ColumnType>();
            attriTypes.add(ColumnType.STRING);
            Table t = new Table(ots, tableName, pkType, attriTypes, 0.5);
            t.create();
            t.insertData(-5000, 10000);
        }
        OTSConf conf = readerConf.getConf();
        List<PrimaryKeyValue> rangeSplit  = new ArrayList<PrimaryKeyValue>();
        
        for (int i = -5000; i < 5000; i++) {
            rangeSplit.add(PrimaryKeyValue.fromLong(i));
        }
        conf.setRangeSplit(rangeSplit);
        
        Configuration p = Configuration.from(readerConf.toString());
        
        OtsReaderMasterProxy master = new OtsReaderMasterProxy();

        master.init(p);
        List<Configuration> cs = master.split(9);
        master.close();
        
        assertEquals(10001, cs.size());
    }
}
