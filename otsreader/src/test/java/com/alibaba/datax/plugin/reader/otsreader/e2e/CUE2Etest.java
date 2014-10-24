package com.alibaba.datax.plugin.reader.otsreader.e2e;

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
import com.alibaba.datax.plugin.reader.otsreader.common.Utils;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConst;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSRange;
import com.alibaba.datax.plugin.reader.otsreader.utils.GsonParser;
import com.alibaba.datax.test.simulator.util.RecordSenderForTest;
import com.aliyun.openservices.ots.model.BatchWriteRowRequest;
import com.aliyun.openservices.ots.model.CapacityUnit;
import com.aliyun.openservices.ots.model.ColumnValue;
import com.aliyun.openservices.ots.model.CreateTableRequest;
import com.aliyun.openservices.ots.model.DeleteTableRequest;
import com.aliyun.openservices.ots.model.Direction;
import com.aliyun.openservices.ots.model.PrimaryKeyType;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.model.RowPrimaryKey;
import com.aliyun.openservices.ots.model.RowPutChange;
import com.aliyun.openservices.ots.model.TableMeta;

/**
 * 构建一个CU较小的表，测试Reader的行为
 * @author redchen
 *
 */
public class CUE2Etest {
    
    private static String tableName = "ots_reader_cu_e2e_test";
    
    private static BaseTest base = new BaseTest(tableName);
    
    @AfterClass
    public static void close() {
        base.close();
    }
    
    public void create(List<PrimaryKeyType> pkTypes , int readCapacityUnit, int writeCapacityUnit) {
        DeleteTableRequest deleteTableRequest = new DeleteTableRequest();
        deleteTableRequest.setTableName(tableName);
        try {
            base.getOts().deleteTable(deleteTableRequest);
        } catch (Exception e) {
        }
        
        TableMeta meta =  new TableMeta(tableName);
        for (int i = 0; i < pkTypes.size(); i++) {
            String name = String.format("pk_%d", i);
            meta.addPrimaryKeyColumn(name, pkTypes.get(i));
        }
        CapacityUnit capacityUnit = new CapacityUnit(readCapacityUnit, writeCapacityUnit);
        CreateTableRequest createTableRequest = new CreateTableRequest();
        createTableRequest.setTableMeta(meta);
        createTableRequest.setReservedThroughput(capacityUnit);
        try {
            base.getOts().createTable(createTableRequest);
        } catch (Exception e) {
        }
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    
    public void insertData(List<PrimaryKeyType> pkTypes, long begin, long rowCount, int cellSize) {
        List<RowPutChange> array = new ArrayList<RowPutChange>();
        for (long i = begin; i < (begin + rowCount); i++) {
            RowPutChange rowChange = new RowPutChange(tableName);
            RowPrimaryKey primaryKey = new RowPrimaryKey();
            for (int j = 0; j < pkTypes.size(); j++) {
                String name = String.format("pk_%d", j);
                PrimaryKeyType type = pkTypes.get(j);
                if (type == PrimaryKeyType.INTEGER) {
                    primaryKey.addPrimaryKeyColumn(name, PrimaryKeyValue.fromLong(i));
                } else {
                    primaryKey.addPrimaryKeyColumn(name, PrimaryKeyValue.fromString(String.format("%d", i)));
                }
            }
            
            rowChange.setPrimaryKey(primaryKey);
            
            StringBuilder attr = new StringBuilder();
            for(int j = 0; j < cellSize; j++) {
                attr.append("a");
            }
            
            rowChange.addAttributeColumn("attr", ColumnValue.fromString(attr.toString()));
            
            
            array.add(rowChange);
            
            if (array.size() > 70) {
                BatchWriteRowRequest batchWriteRow = new BatchWriteRowRequest();
                for (RowPutChange change : array) {
                    batchWriteRow.addRowPutChange(change);
                }
                base.getOts().batchWriteRow(batchWriteRow);
                array.clear();
            }
        }
        if (!array.isEmpty()) {
            BatchWriteRowRequest batchWriteRow = new BatchWriteRowRequest();
            for (RowPutChange change : array) {
                batchWriteRow.addRowPutChange(change);
            }
            base.getOts().batchWriteRow(batchWriteRow);
            array.clear();
        }
    }
    
    public OTSConf getConf() {
        OTSConf conf = new OTSConf();
        conf.setEndpoint(base.getP().getString("endpoint"));
        conf.setAccessId(base.getP().getString("accessid"));
        conf.setAccesskey(base.getP().getString("accesskey"));
        conf.setInstanceName(base.getP().getString("instance-name"));
        conf.setTableName(tableName);
        
        List<OTSColumn> columns = new ArrayList<OTSColumn>();
        columns.add(OTSColumn.fromNormalColumn("pk_0"));

        conf.setColumns(columns);
        
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        conf.setRangeBegin(rangeBegin);

        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        conf.setRangeEnd(rangeEnd);
        
        List<PrimaryKeyValue> splits = new ArrayList<PrimaryKeyValue>();
        
        conf.setRangeSplit(splits);
        
        conf.setRetry(17);
        conf.setSleepInMilliSecond(100);
        return conf;
    }
    
    public OTSRange getRange() {
        RowPrimaryKey begin = new RowPrimaryKey();
        begin.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MIN);
        begin.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.INF_MIN);
        begin.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.INF_MIN);
        begin.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.INF_MIN);
        
        RowPrimaryKey end = new RowPrimaryKey();
        end.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MAX);
        end.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.INF_MAX);
        end.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.INF_MAX);
        end.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.INF_MAX);
        
        OTSRange range = new OTSRange();
        range.setBegin(begin);
        range.setEnd(end);
        
        return range;
    }
    
    class WaitWhileUpdateCU extends Thread{
        private long sleepTime;
        private int readCU;
        private int writeCU;
        
        public WaitWhileUpdateCU(long sleepTime, int readCU, int writeCU) {
            this.sleepTime = sleepTime;
            this.readCU = readCU;
            this.writeCU = writeCU;
        }
        
        public void run() {
            try {
                Thread.sleep(sleepTime);
                base.updateCU(readCU, writeCU);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    
    private void changeUpdateReadCUInterval(int seconds) throws Exception {
        Runtime run = Runtime.getRuntime();
        String [] cmd = {"ssh", "admin@" + base.getP().getString("ag-ip"), 
                "/apsara/deploy/rpc_caller", 
                "--Server=nuwa://localcluster/sys/sqlonline-OTS/master",
                "--Method=/fuxi/SetGlobalFlag",
                "--Parameter=\"{\\\"sqlol_master_AdjustCapacityUnitInterval\\\": "+ seconds +"}\"'"};
        run.exec(cmd);
    }
    
    /**
     * create table的时候CU设置特别小N1，每一行的数据大小都大于N1，重复很长时间之后update table将CU调大，预期全部数据都能被取出
     * 输入：构造所有的行数据都大于 1CU(1025Byte)
     * 期望：预期全部数据都能被取出,且数据正确
     * @throws Exception
     */
    @Test
    public void testNoEnoughCUForAllRowAndUpdateCU() throws Exception {
        // prepare data
        {
            List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
            pk.add(PrimaryKeyType.STRING);
            pk.add(PrimaryKeyType.STRING);
            pk.add(PrimaryKeyType.INTEGER);
            pk.add(PrimaryKeyType.INTEGER);
            
            create(pk, 1, 5000);

            insertData(pk, -1, 100, 1025);
        }

        Configuration configuration = Configuration.newDefault();
        configuration.set(OTSConst.OTS_CONF, GsonParser.confToJson(this.getConf()));
        configuration.set(OTSConst.OTS_RANGE, GsonParser.rangeToJson(this.getRange()));
        configuration.set(OTSConst.OTS_DIRECTION, GsonParser.directionToJson(Direction.FORWARD));
        
        List<Record> noteRecordForTest = new LinkedList<Record>();
        RecordSender sender = new RecordSenderForTest(null, noteRecordForTest);

        OtsReaderSlaveProxy slave = new OtsReaderSlaveProxy();
        
        changeUpdateReadCUInterval(1);
        
        try {
            WaitWhileUpdateCU wait = new WaitWhileUpdateCU(60000, 5000, 5000);
            wait.start();
            slave.read(sender, configuration);
        } finally {
            changeUpdateReadCUInterval(600);
        }
        
        assertEquals(100, noteRecordForTest.size());
        assertEquals(true, Utils.checkOutput(base.getOts(), this.getConf(), noteRecordForTest));
    }
    
    /**
     * create table的时候CU设置正常，启动data之后在data未完成的时候update CU 将CU调小，预期出现CU不够的错误，重试一段时间之后update CU 将CU恢复，预期能取出所有数据。
     * 输入：构造所有的行数据都大于 1CU(1025Byte)
     * 期望：预期全部数据都能被取出,且数据正确
     * @throws Exception
     */
    @Test
    public void testEnoughCUForAllRowAndUpdateCU() throws Exception {
        // prepare data
        {
            List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
            pk.add(PrimaryKeyType.STRING);
            pk.add(PrimaryKeyType.STRING);
            pk.add(PrimaryKeyType.INTEGER);
            pk.add(PrimaryKeyType.INTEGER);
            
            create(pk, 100, 5000);

            insertData(pk, -1, 10000, 1025);
        }

        Configuration configuration = Configuration.newDefault();
        configuration.set(OTSConst.OTS_CONF, GsonParser.confToJson(this.getConf()));
        configuration.set(OTSConst.OTS_RANGE, GsonParser.rangeToJson(this.getRange()));
        configuration.set(OTSConst.OTS_DIRECTION, GsonParser.directionToJson(Direction.FORWARD));
        
        List<Record> noteRecordForTest = new LinkedList<Record>();
        RecordSender sender = new RecordSenderForTest(null, noteRecordForTest);

        OtsReaderSlaveProxy slave = new OtsReaderSlaveProxy();       
        
        changeUpdateReadCUInterval(1);
        
        try {
            WaitWhileUpdateCU wait1 = new WaitWhileUpdateCU(10000, 1, 5000);
            wait1.start();
            
            WaitWhileUpdateCU wait2 = new WaitWhileUpdateCU(60000, 5000, 5000);
            wait2.start();
            slave.read(sender, configuration);
        } finally {
            changeUpdateReadCUInterval(600);
        }
        
        assertEquals(10000, noteRecordForTest.size());
        assertEquals(true, Utils.checkOutput(base.getOts(), this.getConf(), noteRecordForTest));
    }
}
