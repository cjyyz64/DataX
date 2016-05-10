package com.alibaba.datax.plugin.writer.otswriter.functiontest;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.plugin.writer.otswriter.OtsWriterSlaveProxy;
import com.alibaba.datax.plugin.writer.otswriter.common.TestPluginCollector;
import com.alibaba.datax.plugin.writer.otswriter.model.*;
import com.alibaba.datax.plugin.writer.otswriter.utils.GsonParser;
import com.alibaba.datax.test.simulator.util.RecordReceiverForTest;
import com.aliyun.openservices.ots.*;
import com.aliyun.openservices.ots.internal.OTSCallback;
import com.aliyun.openservices.ots.internal.writer.WriterConfig;
import com.aliyun.openservices.ots.model.*;
import org.junit.*;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.otswriter.common.Utils;

import static org.junit.Assert.assertEquals;

/**
 * 通过Update的方式导入数据到OTS中，验证数据的正确性
 */
public class ExportDataByDeleteFunctiontest {
    public static String tableName = "ots_writer_delete_ft";
    private Configuration dxConf;
    private OTS ots;
    private OTSAsync otsAsync;
    private OTSWriter otsWriter;

    @Before
    public void setup() {
        dxConf = Utils.loadConf();
        ots = new OTSClient(dxConf.getString("endpoint"), dxConf.getString("accessid"), dxConf.getString("accesskey"), dxConf.getString("instance-name"));
        otsAsync = new OTSClientAsync(dxConf.getString("endpoint"), dxConf.getString("accessid"), dxConf.getString("accesskey"), dxConf.getString("instance-name"));
        createTable();
        OTSCallback<RowChange,ConsumedCapacity> callback = new OTSCallback<RowChange, ConsumedCapacity>() {
            @Override
            public void onCompleted(OTSContext<RowChange, ConsumedCapacity> otsContext) {
            }

            @Override
            public void onFailed(OTSContext<RowChange, ConsumedCapacity> otsContext, OTSException ex) {
                ex.printStackTrace();
            }

            @Override
            public void onFailed(OTSContext<RowChange, ConsumedCapacity> otsContext, ClientException ex) {
                ex.printStackTrace();
            }
        };
        otsWriter = new DefaultOTSWriter(otsAsync, tableName, new WriterConfig(), callback, Executors.newFixedThreadPool(2));
    }

    @After
    public void shutdown() {
        otsWriter.close();
        otsAsync.shutdown();
        ots.shutdown();
    }

    private void createTable() {
        try {
            DeleteTableRequest request = new DeleteTableRequest();
            request.setTableName(tableName);
            ots.deleteTable(request);
        } catch (OTSException e) {

        }

        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("pk0", PrimaryKeyType.INTEGER);
        tableMeta.addPrimaryKeyColumn("pk1", PrimaryKeyType.INTEGER);
        tableMeta.addPrimaryKeyColumn("pk2", PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn("pk3", PrimaryKeyType.STRING);

        CreateTableRequest request = new CreateTableRequest();
        request.setTableMeta(tableMeta);
        request.setReservedThroughput(new CapacityUnit(0, 0));
        ots.createTable(request);
    }

    private void prepareData(int start, int end) {
        for (int i = start; i < end; i++) {
            RowPrimaryKey primaryKey = new RowPrimaryKey();
            primaryKey.addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(i));
            primaryKey.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(i));
            primaryKey.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromString("pk2_" + i));
            primaryKey.addPrimaryKeyColumn("pk3", PrimaryKeyValue.fromString("pk3_" + i));

            RowPutChange rowChange = new RowPutChange(tableName);
            rowChange.setPrimaryKey(primaryKey);
            rowChange.addAttributeColumn("col0", ColumnValue.fromLong(i));
            rowChange.addAttributeColumn("col1", ColumnValue.fromString("col1_" + i));
            rowChange.addAttributeColumn("col2", ColumnValue.fromLong(i + 1));

            otsWriter.addRowChange(rowChange);
        }

        otsWriter.flush();
    }

    private void checkData(int deleteStart, int deleteEnd, int start, int end) {
        RangeIteratorParameter param = new RangeIteratorParameter(tableName);
        RowPrimaryKey startPrimaryKey = new RowPrimaryKey();
        startPrimaryKey.addPrimaryKeyColumn("pk0", PrimaryKeyValue.INF_MIN);
        startPrimaryKey.addPrimaryKeyColumn("pk1", PrimaryKeyValue.INF_MIN);
        startPrimaryKey.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
        startPrimaryKey.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);

        RowPrimaryKey endPrimaryKey = new RowPrimaryKey();
        endPrimaryKey.addPrimaryKeyColumn("pk0", PrimaryKeyValue.INF_MAX);
        endPrimaryKey.addPrimaryKeyColumn("pk1", PrimaryKeyValue.INF_MAX);
        endPrimaryKey.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MAX);
        endPrimaryKey.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MAX);

        param.setInclusiveStartPrimaryKey(startPrimaryKey);
        param.setExclusiveEndPrimaryKey(endPrimaryKey);

        Iterator<Row> rowIter = ots.createRangeIterator(param);

        List<Row> rows = new ArrayList<Row>();
        while (rowIter.hasNext()) {
            rows.add(rowIter.next());
        }

        SortedSet<Integer> ids = new TreeSet<Integer>();
        for (int i = start; i < deleteStart; i++) {
            ids.add(i);
        }

        for (int i = deleteEnd; i < end; i++) {
            ids.add(i);
        }

        Integer[] idToCheck = ids.toArray(new Integer[0]);
        assertEquals(rows.size(), idToCheck.length);
        for (int i = 0; i < rows.size(); i++) {
            Row row = rows.get(i);
            assertEquals(idToCheck[i].longValue(), row.getColumns().get("pk0").asLong());
            assertEquals(idToCheck[i].longValue(), row.getColumns().get("pk1").asLong());
            assertEquals("pk2_" + idToCheck[i].longValue(), row.getColumns().get("pk2").asString());
            assertEquals("pk3_" + idToCheck[i].longValue(), row.getColumns().get("pk3").asString());

            assertEquals(idToCheck[i].longValue(), row.getColumns().get("col0").asLong());
            assertEquals("col1_" + idToCheck[i].longValue(), row.getColumns().get("col1").asString());
            assertEquals(idToCheck[i].longValue() + 1, row.getColumns().get("col2").asLong());
        }
    }

    private void deleteByWriter(int start, int end) throws Exception {
        List<Record> rs = new ArrayList<Record>();

        for (int i = start; i < end; i++) {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(i));
            r.addColumn(new LongColumn(i));
            r.addColumn(new StringColumn("pk2_" + i));
            r.addColumn(new StringColumn("pk3_" + i));

            r.addColumn(new LongColumn(i));
            r.addColumn(new LongColumn(i));
            rs.add(r);
        }

        OTSConf conf = new OTSConf();
        conf.setEndpoint(dxConf.getString("endpoint"));
        conf.setAccessId(dxConf.getString("accessid"));
        conf.setAccessKey(dxConf.getString("accesskey"));
        conf.setInstanceName(dxConf.getString("instance-name"));
        conf.setTableName(tableName);

        List<OTSPKColumn> primaryKeyColumn = new ArrayList<OTSPKColumn>();
        primaryKeyColumn.add(new OTSPKColumn("pk0", PrimaryKeyType.INTEGER));
        primaryKeyColumn.add(new OTSPKColumn("pk1", PrimaryKeyType.INTEGER));
        primaryKeyColumn.add(new OTSPKColumn("pk2", PrimaryKeyType.STRING));
        primaryKeyColumn.add(new OTSPKColumn("pk3", PrimaryKeyType.STRING));
        conf.setPrimaryKeyColumn(primaryKeyColumn);

        List<OTSAttrColumn> attributeColumn = new ArrayList<OTSAttrColumn>();
        attributeColumn.add(new OTSAttrColumn("col0", ColumnType.INTEGER));
        attributeColumn.add(new OTSAttrColumn("col1", ColumnType.INTEGER));
        conf.setAttributeColumn(attributeColumn);

        conf.setOperation(OTSOpType.DELETE_ROW);

        conf.setRetry(18);
        conf.setSleepInMillisecond(100);
        conf.setBatchWriteCount(100);
        conf.setConcurrencyWrite(5);
        conf.setIoThreadCount(1);
        conf.setSocketTimeout(60000);
        conf.setConnectTimeout(60000);

        OTSConf.RestrictConf restrictConf = conf.new RestrictConf();
        restrictConf.setRequestTotalSizeLimition(1024 * 1024);
        conf.setRestrictConf(restrictConf);

        Configuration configuration = Configuration.newDefault();
        TestPluginCollector collector = new TestPluginCollector(configuration, null, null);

        configuration.set(OTSConst.OTS_CONF, GsonParser.confToJson(conf));
        RecordReceiverForTest recordReceiver = new RecordReceiverForTest(rs);
        OtsWriterSlaveProxy slave = new OtsWriterSlaveProxy();
        slave.init(configuration);
        try {
            slave.write(recordReceiver, collector);
        } finally {
            slave.close();
        }

        assertEquals(collector.getContent().size(), 0);
    }

    @Test
    public void testDeleteRow() throws Exception {
        prepareData(0, 10000);
        checkData(0, 0, 0, 10000);
        deleteByWriter(2001, 6012);
        checkData(2001, 6012, 0, 10000);

        deleteByWriter(0, 2001);
        checkData(0, 6012, 0, 10000);

        deleteByWriter(6012, 10000);
        checkData(0, 10000, 0, 10000);
    }
}
