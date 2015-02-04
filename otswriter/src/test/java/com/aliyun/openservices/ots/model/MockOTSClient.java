package com.aliyun.openservices.ots.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.plugin.writer.otswriter.model.OTSOpType;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSRowPrimaryKey;
import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.OTS;
import com.aliyun.openservices.ots.OTSErrorCode;
import com.aliyun.openservices.ots.OTSException;
import com.aliyun.openservices.ots.model.BatchGetRowRequest;
import com.aliyun.openservices.ots.model.BatchGetRowResult;
import com.aliyun.openservices.ots.model.BatchWriteRowRequest;
import com.aliyun.openservices.ots.model.BatchWriteRowResult;
import com.aliyun.openservices.ots.model.BatchWriteRowResult.RowStatus;
import com.aliyun.openservices.ots.model.CreateTableRequest;
import com.aliyun.openservices.ots.model.CreateTableResult;
import com.aliyun.openservices.ots.model.DeleteRowRequest;
import com.aliyun.openservices.ots.model.DeleteRowResult;
import com.aliyun.openservices.ots.model.DeleteTableRequest;
import com.aliyun.openservices.ots.model.DeleteTableResult;
import com.aliyun.openservices.ots.model.DescribeTableRequest;
import com.aliyun.openservices.ots.model.DescribeTableResult;
import com.aliyun.openservices.ots.model.GetRangeRequest;
import com.aliyun.openservices.ots.model.GetRangeResult;
import com.aliyun.openservices.ots.model.GetRowRequest;
import com.aliyun.openservices.ots.model.GetRowResult;
import com.aliyun.openservices.ots.model.ListTableResult;
import com.aliyun.openservices.ots.model.PutRowRequest;
import com.aliyun.openservices.ots.model.PutRowResult;
import com.aliyun.openservices.ots.model.RangeIteratorParameter;
import com.aliyun.openservices.ots.model.Row;
import com.aliyun.openservices.ots.model.UpdateRowRequest;
import com.aliyun.openservices.ots.model.UpdateRowResult;
import com.aliyun.openservices.ots.model.UpdateTableRequest;
import com.aliyun.openservices.ots.model.UpdateTableResult;


public class MockOTSClient implements OTS{

    private AtomicInteger invokeTimes = new AtomicInteger(0);
    private AtomicInteger conInvokeTimes = new AtomicInteger(0);
    private int conMaxInvokeTimes = 0;
    private Exception exception = null;

    private Lock lock = new ReentrantLock(); 

    private long writeCU = 0;
    private long remaingCU = 0;
    private long lastTime = 0;
    private long elapsedTime = 0;
    
    private List<Integer> rows = new ArrayList<Integer>();

    private Map<OTSRowPrimaryKey, Row> lines = new HashMap<OTSRowPrimaryKey, Row>();

    private static final Logger LOG = LoggerFactory.getLogger(MockOTSClient.class);

    public MockOTSClient() {
        this(5000, null, null);
    }

    public MockOTSClient(
            int writeCU,
            Exception exception,
            Map<OTSRowPrimaryKey, Row> prepare
            ) {
        this(writeCU, exception, prepare, 10);
    }

    public MockOTSClient(
            int writeCU,
            Exception exception,
            Map<OTSRowPrimaryKey, Row> prepare,
            long elapsedTime
            ) {
        this.writeCU = writeCU;
        this.remaingCU = writeCU;
        this.lastTime = (new Date()).getTime();
        this.exception = exception;
        this.elapsedTime = elapsedTime;
        if (prepare != null) {
            lines.putAll(prepare);
        }
    }

    public int getInvokeTimes() {
        return invokeTimes.intValue();
    }

    public int getMaxConcurrenyInvokeTimes() {
        return conMaxInvokeTimes;
    }

    public Map<OTSRowPrimaryKey, Row> getData() {
        return lines;
    }
    
    public List<Integer> getRows() {
        return rows;
    }

    private void add(OTSOpType type, Map<String, PrimaryKeyValue> pk, Map<String, ColumnValue> attr) {
        if (type == OTSOpType.PUT_ROW) {
            lines.put(new OTSRowPrimaryKey(pk), Helper.buildRow(type, pk, attr, null));
        } else {
            Row old = lines.get(new OTSRowPrimaryKey(pk));
            if (old != null) {
                lines.put(new OTSRowPrimaryKey(pk), Helper.buildRow(type, pk, attr, old.getColumns()));
            } else {
                lines.put(new OTSRowPrimaryKey(pk), Helper.buildRow(type, pk, attr, null));
            }
        }
    }
    
    private void send(OTSOpType type, String tableName, Map<String, PrimaryKeyValue> pk, Map<String, ColumnValue> attr) {
        long expectCU = Helper.getCU(pk, attr);

        try {
            lock.lock();
            long curTime = (new Date()).getTime();
            long rangeTime = curTime - lastTime;//上次操作的间隔

            long tmpCU = writeCU * rangeTime / 1000;
            long tmpRemaingCU = (remaingCU + tmpCU) < writeCU ? (remaingCU + tmpCU) : writeCU; // 计算CU
            if ((tmpRemaingCU - 1) >= 0) { // 预扣CU
                lastTime = curTime;
                remaingCU =  remaingCU - expectCU + tmpCU; // 补扣CU
                // add data
                add(type, pk, attr);
                try {
                    Thread.sleep(elapsedTime);
                } catch (InterruptedException e) {
                }
            } else {
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                }
                throw new RuntimeException(OTSErrorCode.NOT_ENOUGH_CAPACITY_UNIT);
            }
        } finally {
            lock.unlock();
        }
    }

    private void handlePutRow(Map<String, List<RowPutChange>> input, BatchWriteRowResult result) throws InterruptedException {
        // mock ots
        int totalRow = 0;
        int totalSize = 0;
        for (Entry<String, List<RowPutChange>> en : input.entrySet()) {
            // Row count的检查
            totalRow += en.getValue().size();
            if (totalRow > 100) {
                throw new OTSException(
                        "Total Row count > 100", 
                        null, 
                        OTSErrorCode.INVALID_PARAMETER, 
                        "RequestId",
                        400);
            }
            for (RowPutChange change : en.getValue()) {
                totalSize += Helper.getPKSize(change.getRowPrimaryKey().getPrimaryKey());

                // column number的检查
                if (change.getAttributeColumns().size() > 128) {
                    throw new OTSException(
                            "Attribute column > 128", 
                            null, 
                            OTSErrorCode.INVALID_PARAMETER, 
                            "RequestId",
                            400);
                }

                // column name 合法性检查
                totalSize += Helper.getAttrSize(change.getAttributeColumns());

                // Total Size的检查
                if (totalSize > (1024*1024)) {
                    throw new OTSException(
                            "Total Size > 1MB", 
                            null, 
                            OTSErrorCode.INVALID_PARAMETER, 
                            "RequestId",
                            400);
                }
            }

        }

        // mock sql clinet
        // STRING PK, STRING ATTR, BINARY ATTR的检查

        for (Entry<String, List<RowPutChange>> en : input.entrySet()) {
            for (RowPutChange change : en.getValue()) {// row
                boolean flag = true;
                for (Entry<String, PrimaryKeyValue> pk : change.getRowPrimaryKey().getPrimaryKey().entrySet()) {
                    switch(pk.getValue().getType()) {
                        case STRING:
                            if (pk.getValue().asString().length() > 1024) {
                                result.addPutRowResult(en.getKey(), new RowStatus(new Error(OTSErrorCode.INVALID_PARAMETER, "STRING PK SIZE > 1KB")));
                                flag = false;
                            }
                            break;
                        default:
                            break;
                    }
                }
                for (Entry<String, ColumnValue> attr : change.getAttributeColumns().entrySet()) {
                    switch (attr.getValue().getType()) {
                        case BINARY:
                            if (attr.getValue().asBinary().length > 64*1024) {
                                result.addPutRowResult(en.getKey(), new RowStatus(new Error(OTSErrorCode.INVALID_PARAMETER, "BINARY ATTR SIZE > 64KB")));
                                flag = false;
                            }
                            break;
                        case STRING:
                            if (attr.getValue().asString().length() > 64*1024) {
                                result.addPutRowResult(en.getKey(), new RowStatus(new Error(OTSErrorCode.INVALID_PARAMETER, "STRING ATTR SIZE > 64KB")));
                                flag = false;
                            }
                            break;
                        default:
                            break;
                    }
                }
                if (flag) {
                    // send to worker
                    // mock sql worker
                    try {
                        send(OTSOpType.PUT_ROW, change.getTableName(), change.getRowPrimaryKey().getPrimaryKey(), change.getAttributeColumns());
                        result.addPutRowResult(change.getTableName(), new RowStatus(new ConsumedCapacity()));
                    } catch (RuntimeException e) {
                        result.addPutRowResult(change.getTableName(), new RowStatus(new Error(OTSErrorCode.NOT_ENOUGH_CAPACITY_UNIT, "CU NOT ENOUGH")));
                    }
                }
            }
        }
    }

    private void handleUpdateRow(Map<String, List<RowUpdateChange>> input, BatchWriteRowResult result) throws InterruptedException {
        // mock ots
        int totalRow = 0;
        int totalSize = 0;
        for (Entry<String, List<RowUpdateChange>> en : input.entrySet()) {
            // Row count的检查
            totalRow += en.getValue().size();
            if (totalRow > 100) {
                throw new OTSException(
                        "Total Row count > 100", 
                        null, 
                        OTSErrorCode.INVALID_PARAMETER, 
                        "RequestId",
                        400);
            }
            for (RowUpdateChange change : en.getValue()) {
                totalSize += Helper.getPKSize(change.getRowPrimaryKey().getPrimaryKey());

                // column number的检查
                if (change.getAttributeColumns().size() > 128) {
                    throw new OTSException(
                            "Attribute column > 128", 
                            null, 
                            OTSErrorCode.INVALID_PARAMETER, 
                            "RequestId",
                            400);
                }
                
                
                // column name 合法性检查
                totalSize += Helper.getAttrSize(change.getAttributeColumns());

                // Total Size的检查
                if (totalSize > (1024*1024)) {
                    throw new OTSException(
                            "Total Size > 1MB", 
                            null, 
                            OTSErrorCode.INVALID_PARAMETER, 
                            "RequestId",
                            400);
                }
            }

        }

        // mock sql clinet
        // STRING PK, STRING ATTR, BINARY ATTR的检查

        for (Entry<String, List<RowUpdateChange>> en : input.entrySet()) {
            for (RowUpdateChange change : en.getValue()) {// row
                boolean flag = true;
                for (Entry<String, PrimaryKeyValue> pk : change.getRowPrimaryKey().getPrimaryKey().entrySet()) {
                    switch(pk.getValue().getType()) {
                        case STRING:
                            if (pk.getValue().asString().length() > 1024) {
                                result.addUpdateRowResult(en.getKey(), new RowStatus(new Error(OTSErrorCode.INVALID_PARAMETER, "STRING PK SIZE > 1KB")));
                                flag = false;
                            }
                            break;
                        default:
                            break;}
                }
                for (Entry<String, ColumnValue> attr : change.getAttributeColumns().entrySet()) {
                    if (attr.getValue() == null) {
                        continue;
                    }
                    switch (attr.getValue().getType()) {
                        case BINARY:
                            if (attr.getValue().asBinary().length > 64*1024) {
                                result.addUpdateRowResult(en.getKey(), new RowStatus(new Error(OTSErrorCode.INVALID_PARAMETER, "BINARY ATTR SIZE > 64KB")));
                                flag = false;
                            }

                            break;
                        case STRING:
                            if (attr.getValue().asString().length() > 64*1024) {
                                result.addUpdateRowResult(en.getKey(), new RowStatus(new Error(OTSErrorCode.INVALID_PARAMETER, "STRING ATTR SIZE > 64KB")));
                                flag = false;
                            }
                            break;
                        default:
                            break;
                    }
                }
                if (flag) {
                    // send to worker
                    // mock worker
                    try {
                        send(OTSOpType.UPDATE_ROW, change.getTableName(), change.getRowPrimaryKey().getPrimaryKey(), change.getAttributeColumns());
                        result.addUpdateRowResult(change.getTableName(), new RowStatus(new ConsumedCapacity()));
                    } catch (RuntimeException e) {
                        result.addUpdateRowResult(change.getTableName(), new RowStatus(new Error(OTSErrorCode.NOT_ENOUGH_CAPACITY_UNIT, "CU NOT ENOUGH")));
                    }
                }
            }
        }
    }


    @Override
    public BatchWriteRowResult batchWriteRow(
            BatchWriteRowRequest batchWriteRowRequest) throws OTSException,
            ClientException {
        try {
            invokeTimes.incrementAndGet();
            conInvokeTimes.incrementAndGet();
            
            int rowsCount = 0;
            for (Entry<String, List<RowPutChange>> en : batchWriteRowRequest.getRowPutChange().entrySet()) {
                rowsCount += en.getValue().size();
            }
            for (Entry<String, List<RowUpdateChange>> en : batchWriteRowRequest.getRowUpdateChange().entrySet()) {
                rowsCount += en.getValue().size();
            }
            
            rows.add(rowsCount);
            
            if (conInvokeTimes.intValue() > conMaxInvokeTimes) {
                conMaxInvokeTimes = conInvokeTimes.intValue();
            }

            if (exception != null) {
                if (exception instanceof ClientException) {
                    throw (ClientException)exception;
                } else {
                    throw (OTSException)exception;
                }
            }

            OTSResult meta = new OTSResult();
            meta.setRequestID("requsetid batch write row");
            meta.setTraceId("tracerid");
            BatchWriteRowResult result = new BatchWriteRowResult(meta);
            
            

            try {
                handlePutRow(batchWriteRowRequest.getRowPutChange(), result);
                handleUpdateRow(batchWriteRowRequest.getRowUpdateChange(), result);
            } catch (InterruptedException e) {
                LOG.error(e.getMessage());
            }
            return result;
        } finally{
            conInvokeTimes.decrementAndGet();
        }
    }

    /**
     * PutRow 检查流程
     * OTS
     * 1.Column的个数检查
     * 2.ColumnName的合法性检查
     * 
     * SQL CLIENT
     * 1.Cell Value长度的检查
     * 
     * SQL WORKER
     * 1.CU计算
     * 2.写入数据/抛异常
     */
    @Override
    public PutRowResult putRow(PutRowRequest putRowRequest)
            throws OTSException, ClientException {
        try {
            invokeTimes.incrementAndGet();
            conInvokeTimes.incrementAndGet();
            rows.add(1);

            if (conInvokeTimes.intValue() > conMaxInvokeTimes) {
                conMaxInvokeTimes = conInvokeTimes.intValue();
            }

            if (exception != null) {
                if (exception instanceof ClientException) {
                    throw (ClientException)exception;
                } else {
                    throw (OTSException)exception;
                }
            }
            
            RowPutChange change = putRowRequest.getRowChange();

            // mock ots
            // column number的检查
            if (change.getAttributeColumns().size() > 128) {
                throw new OTSException(
                        "Attribute column > 128", 
                        null, 
                        OTSErrorCode.INVALID_PARAMETER, 
                        "RequestId",
                        400);
            }
            
            for (Entry<String, ColumnValue> attr : change.getAttributeColumns().entrySet()) {
                if (!Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*").matcher(attr.getKey()).matches()) {
                    throw new OTSException(
                            "Column name invalid", 
                            null, 
                            OTSErrorCode.INVALID_PARAMETER, 
                            "RequestId",
                            400);
                }
            }
            
            // mock sql client
            for (Entry<String, PrimaryKeyValue> pk : change.getRowPrimaryKey().getPrimaryKey().entrySet()) {
                switch(pk.getValue().getType()) {
                    case STRING:
                        if (pk.getValue().asString().length() > 1024) {
                            throw new OTSException("STRING PK SIZE > 1KB", null, OTSErrorCode.INVALID_PARAMETER, "request_id", 400);
                        }
                        break;
                    default:
                        break;}
            }
            for (Entry<String, ColumnValue> attr : change.getAttributeColumns().entrySet()) {
                if (attr.getValue() == null) {
                    continue;
                }
                switch (attr.getValue().getType()) {
                    case BINARY:
                        if (attr.getValue().asBinary().length > 64*1024) {
                            throw new OTSException("BINARY ATTR SIZE > 64KB", null, OTSErrorCode.INVALID_PARAMETER, "request_id", 400);
                        }
                        break;
                    case STRING:
                        if (attr.getValue().asString().length() > 64*1024) {
                            throw new OTSException("STRING ATTR SIZE > 64KB", null, OTSErrorCode.INVALID_PARAMETER, "request_id", 400);
                        }
                        break;
                    default:
                        break;
                }
            }
            
            try {
                send(OTSOpType.PUT_ROW, change.getTableName(), change.getRowPrimaryKey().getPrimaryKey(), change.getAttributeColumns());
                OTSResult meta = new OTSResult();
                meta.setRequestID("requsetid put row");
                meta.setTraceId("tracerid");
                return new PutRowResult(meta);
            } catch (RuntimeException e) {
                throw new OTSException("CU NOT ENOUGH", null, OTSErrorCode.NOT_ENOUGH_CAPACITY_UNIT, "request_id", 403);
            }
        } finally {
            conInvokeTimes.decrementAndGet();
        }
    }

    /**
     * UpdateRow 检查流程
     * OTS
     * 1.Column的个数检查
     * 2.ColumnName的合法性检查
     * 
     * SQL CLIENT
     * 1.Cell Value长度的检查
     * 
     * SQL WORKER
     * 1.CU计算
     * 2.写入数据/抛异常
     */
    @Override
    public UpdateRowResult updateRow(UpdateRowRequest updateRowRequest)
            throws OTSException, ClientException {
        try {
            invokeTimes.incrementAndGet();
            conInvokeTimes.incrementAndGet();
            rows.add(1);

            if (conInvokeTimes.intValue() > conMaxInvokeTimes) {
                conMaxInvokeTimes = conInvokeTimes.intValue();
            }

            if (exception != null) {
                if (exception instanceof ClientException) {
                    throw (ClientException)exception;
                } else {
                    throw (OTSException)exception;
                }
            }

            RowUpdateChange change = updateRowRequest.getRowChange();

            // mock ots
            // column number的检查
            if (change.getAttributeColumns().size() > 128) {
                throw new OTSException(
                        "Attribute column > 128", 
                        null, 
                        OTSErrorCode.INVALID_PARAMETER, 
                        "RequestId",
                        400);
            }
            
            for (Entry<String, ColumnValue> attr : change.getAttributeColumns().entrySet()) {
                if (!Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*").matcher(attr.getKey()).matches()) {
                    throw new OTSException(
                            "Column name invalid", 
                            null, 
                            OTSErrorCode.INVALID_PARAMETER, 
                            "RequestId",
                            400);
                }
            }

            // mock sql client
            for (Entry<String, PrimaryKeyValue> pk : change.getRowPrimaryKey().getPrimaryKey().entrySet()) {
                switch(pk.getValue().getType()) {
                    case STRING:
                        if (pk.getValue().asString().length() > 1024) {
                            throw new OTSException("STRING PK SIZE > 1KB", null, OTSErrorCode.INVALID_PARAMETER, "request_id", 400);
                        }
                        break;
                    default:
                        break;}
            }
            for (Entry<String, ColumnValue> attr : change.getAttributeColumns().entrySet()) {
                if (attr.getValue() == null) {
                    continue;
                }
                switch (attr.getValue().getType()) {
                    case BINARY:
                        if (attr.getValue().asBinary().length > 64*1024) {
                            throw new OTSException("BINARY ATTR SIZE > 64KB", null, OTSErrorCode.INVALID_PARAMETER, "request_id", 400);
                        }
                        break;
                    case STRING:
                        if (attr.getValue().asString().length() > 64*1024) {
                            throw new OTSException("STRING ATTR SIZE > 64KB", null, OTSErrorCode.INVALID_PARAMETER, "request_id", 400);
                        }
                        break;
                    default:
                        break;
                }
            }
            
            try {
                send(OTSOpType.UPDATE_ROW, change.getTableName(), change.getRowPrimaryKey().getPrimaryKey(), change.getAttributeColumns());
                OTSResult meta = new OTSResult();
                meta.setRequestID("requsetid update row");
                meta.setTraceId("tracerid");
                return new UpdateRowResult(meta);
            } catch (RuntimeException e) {
                throw new OTSException("CU NOT ENOUGH", null, OTSErrorCode.NOT_ENOUGH_CAPACITY_UNIT, "request_id", 403);
            }
        } finally {
            conInvokeTimes.decrementAndGet();
        }
    }

    @Override
    public void shutdown() {
    }

    //##########################################################################

    @Override
    public CreateTableResult createTable(CreateTableRequest createTableRequest)
            throws OTSException, ClientException {
        throw new RuntimeException("Unimplements");
    }

    @Override
    public DescribeTableResult describeTable(
            DescribeTableRequest describeTableRequest) throws OTSException,
            ClientException {
        throw new RuntimeException("Unimplements");
    }

    @Override
    public ListTableResult listTable() throws OTSException, ClientException {
        throw new RuntimeException("Unimplements");
    }

    @Override
    public DeleteTableResult deleteTable(DeleteTableRequest deleteTableRequest)
            throws OTSException, ClientException {
        throw new RuntimeException("Unimplements");
    }

    @Override
    public UpdateTableResult updateTable(UpdateTableRequest updateTableRequest)
            throws OTSException, ClientException {
        throw new RuntimeException("Unimplements");
    }

    @Override
    public GetRowResult getRow(GetRowRequest getRowRequest)
            throws OTSException, ClientException {
        throw new RuntimeException("Unimplements");
    }

    @Override
    public DeleteRowResult deleteRow(DeleteRowRequest deleteRowRequest)
            throws OTSException, ClientException {
        throw new RuntimeException("Unimplements");
    }

    @Override
    public BatchGetRowResult batchGetRow(BatchGetRowRequest batchGetRowRequest)
            throws OTSException, ClientException {
        throw new RuntimeException("Unimplements");
    }


    @Override
    public GetRangeResult getRange(GetRangeRequest getRangeRequest)
            throws OTSException, ClientException {
        throw new RuntimeException("Unimplements");
    }

    @Override
    public Iterator<Row> createRangeIterator(
            RangeIteratorParameter rangeIteratorParameter) throws OTSException,
            ClientException {
        throw new RuntimeException("Unimplements");
    }
}
