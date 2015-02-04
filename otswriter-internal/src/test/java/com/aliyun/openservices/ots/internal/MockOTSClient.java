package com.aliyun.openservices.ots.internal;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.plugin.writer.otswriter.model.OTSOpType;
import com.aliyun.openservices.ots.internal.model.BatchGetRowRequest;
import com.aliyun.openservices.ots.internal.model.BatchGetRowResult;
import com.aliyun.openservices.ots.internal.model.BatchWriteRowRequest;
import com.aliyun.openservices.ots.internal.model.BatchWriteRowResult;
import com.aliyun.openservices.ots.internal.model.CapacityUnit;
import com.aliyun.openservices.ots.internal.model.Column;
import com.aliyun.openservices.ots.internal.model.ColumnValue;
import com.aliyun.openservices.ots.internal.model.ConsumedCapacity;
import com.aliyun.openservices.ots.internal.model.CreateTableRequest;
import com.aliyun.openservices.ots.internal.model.CreateTableResult;
import com.aliyun.openservices.ots.internal.model.DeleteRowRequest;
import com.aliyun.openservices.ots.internal.model.DeleteRowResult;
import com.aliyun.openservices.ots.internal.model.DeleteTableRequest;
import com.aliyun.openservices.ots.internal.model.DeleteTableResult;
import com.aliyun.openservices.ots.internal.model.DescribeTableRequest;
import com.aliyun.openservices.ots.internal.model.DescribeTableResult;
import com.aliyun.openservices.ots.internal.model.Error;
import com.aliyun.openservices.ots.internal.model.GetRangeRequest;
import com.aliyun.openservices.ots.internal.model.GetRangeResult;
import com.aliyun.openservices.ots.internal.model.GetRowRequest;
import com.aliyun.openservices.ots.internal.model.GetRowResult;
import com.aliyun.openservices.ots.internal.model.ListTableResult;
import com.aliyun.openservices.ots.internal.model.LoadTableRequest;
import com.aliyun.openservices.ots.internal.model.LoadTableResult;
import com.aliyun.openservices.ots.internal.model.MockBatchWriteRowResult;
import com.aliyun.openservices.ots.internal.model.MockGetRangeResult;
import com.aliyun.openservices.ots.internal.model.OTSResult;
import com.aliyun.openservices.ots.internal.model.PrimaryKey;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyColumn;
import com.aliyun.openservices.ots.internal.model.PutRowRequest;
import com.aliyun.openservices.ots.internal.model.PutRowResult;
import com.aliyun.openservices.ots.internal.model.RangeIteratorParameter;
import com.aliyun.openservices.ots.internal.model.Row;
import com.aliyun.openservices.ots.internal.model.RowPutChange;
import com.aliyun.openservices.ots.internal.model.RowUpdateChange;
import com.aliyun.openservices.ots.internal.model.UnloadTableRequest;
import com.aliyun.openservices.ots.internal.model.UnloadTableResult;
import com.aliyun.openservices.ots.internal.model.UpdateRowRequest;
import com.aliyun.openservices.ots.internal.model.UpdateRowResult;
import com.aliyun.openservices.ots.internal.model.UpdateTableRequest;
import com.aliyun.openservices.ots.internal.model.UpdateTableResult;
import com.aliyun.openservices.ots.internal.model.BatchWriteRowResult.RowResult;
import com.aliyun.openservices.ots.internal.model.RowUpdateChange.Type;
import com.aliyun.openservices.ots.internal.utils.Pair;

/**
 * Mock OTS Client
 * @author redchen
 *
 */
public class MockOTSClient implements OTS{
    
    private AtomicInteger invokeTimes = new AtomicInteger(0); // 调用次数
    private AtomicInteger conInvokeTimes = new AtomicInteger(0); // 并发调用次数
    private int conMaxInvokeTimes = 0; // 最大的并发调用次数
    private Exception exception = null; // 异常设置

    private Lock lock = new ReentrantLock(); 

    private long writeCU = 0; // 写CU
    private long remaingCU = 0; // 剩余写CU
    private long lastTime = 0; // 上次操作的时间
    private long elapsedTime = 0; // 操作执行的时间
    
    private List<Integer> rows = new ArrayList<Integer>(); // 记录每次操作的函数

    private Map<PrimaryKey, Row> lines = new HashMap<PrimaryKey, Row>(); //

    private static final Logger LOG = LoggerFactory.getLogger(MockOTSClient.class);

    
    public MockOTSClient() {
        this(5000, null, null);
    }

    public MockOTSClient(
            int writeCU,
            Exception exception,
            Map<PrimaryKey, Row> prepare
            ) {
        this(writeCU, exception, prepare, 10);
    }

    public MockOTSClient(
            int writeCU,
            Exception exception,
            Map<PrimaryKey, Row> prepare,
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

    public Map<PrimaryKey, Row> getData() {
        return lines;
    }
    
    /**
     * 每次操作的行数
     * @return
     */
    public List<Integer> getRowsCountPerRequest() {
        return rows;
    }

    private void add (OTSOpType type, PrimaryKey pk, List<Pair<Column, Type>> attr) {
        if (type == OTSOpType.PUT_ROW) {
            lines.put(pk, new Row(pk, toColumns(attr)));
        } else {
            Row old = lines.get(pk);
            if (old == null) {
                List<Column> columns =  new ArrayList<Column>();

                for (Pair<Column, Type> p : attr) {
                    if (p.getSecond() == Type.PUT) {
                        columns.add(p.getFirst());
                    }
                }
                lines.put(pk, new Row(pk, columns));
            } else {
                // merge
                NavigableMap<String, NavigableMap<Long, ColumnValue>> mapping = old.getColumnsMap();
                for (Pair<Column, Type> p : attr) {
                    if (p.getSecond() == Type.PUT) {
                        NavigableMap<Long, ColumnValue> cells = mapping.get(p.getFirst().getName());
                        if (cells != null) {
                            cells.put(p.getFirst().getTimestamp(), p.getFirst().getValue());
                        } else {
                            cells = new TreeMap<Long, ColumnValue>(new Comparator<Long>() {
                                public int compare(Long l1, Long l2) {
                                    return l2.compareTo(l1);
                                }
                            });
                            cells.put(p.getFirst().getTimestamp(), p.getFirst().getValue());
                            mapping.put(p.getFirst().getName(), cells);
                        }
                        
                    } else if (p.getSecond() == Type.DELETE_ALL) {
                        mapping.remove(p.getFirst().getName());
                    } else {
                        mapping.get(p.getFirst().getName()).remove(p.getFirst().getTimestamp());
                    }
                }
                List<Column> columns =  new ArrayList<Column>();
                for (Entry<String, NavigableMap<Long, ColumnValue>> en : mapping.entrySet()) {
                    for (Entry<Long, ColumnValue> ennn : en.getValue().entrySet()) {
                        columns.add(new Column(en.getKey(), ennn.getValue(), ennn.getKey()));
                    }
                }
                lines.put(pk, new Row(pk, columns));
            }
        }
    }
    
    private List<Column> toColumns(List<Pair<Column, Type>> attr) {
        List<Column> r = new ArrayList<Column>();
        for (Pair<Column, Type> p : attr) {
            r.add(p.getFirst());
        }
        return r;
    }
    
    private List<Pair<Column, Type>> toPairColumns(List<Column> attr) {
        List<Pair<Column, Type>> r = new ArrayList<Pair<Column, Type>>();
        for (Column p : attr) {
            r.add(new Pair<Column, Type>(p, Type.PUT));
        }
        return r;
    }
    
    private void send(OTSOpType type, String tableName, PrimaryKey pk, List<Pair<Column, Type>> attr) throws OTSException {
        long expectCU = 10;
        
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
    
    private void handlePutRow(Map<String, List<RowPutChange>> input, MockBatchWriteRowResult result) throws InterruptedException, OTSException {
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
                totalSize += Helper.getPKSize(change.getPrimaryKey());

                // column number的检查
                if (change.getColumnsToPut().size() > 128) {
                    throw new OTSException(
                            "Attribute column > 128", 
                            null, 
                            OTSErrorCode.INVALID_PARAMETER, 
                            "RequestId",
                            400);
                }

                // column name 合法性检查
                totalSize += Helper.getAttrSize(change.getColumnsToPut());

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

        int index = 0;
        for (Entry<String, List<RowPutChange>> en : input.entrySet()) {
            for (RowPutChange change : en.getValue()) {// row
                boolean flag = true;
                for (PrimaryKeyColumn pk : change.getPrimaryKey().getPrimaryKeyColumns()) {
                    switch(pk.getValue().getType()) {
                        case STRING:
                            if (pk.getValue().asString().length() > 1024) {
                                result.addPutRowResult(
                                        new RowResult(
                                                en.getKey(), 
                                                new Error(OTSErrorCode.INVALID_PARAMETER, "STRING PK SIZE > 1KB"),
                                                index
                                        )
                                );
                                flag = false;
                            }
                            break;
                        default:
                            break;
                    }
                }

                for (Column attr : change.getColumnsToPut()) {
                    switch (attr.getValue().getType()) {
                        case BINARY:
                            if (attr.getValue().asBinary().length > 64*1024) {
                                result.addPutRowResult(
                                        new RowResult(
                                                en.getKey(), 
                                                new Error(OTSErrorCode.INVALID_PARAMETER, "BINARY ATTR SIZE > 64KB"),
                                                index
                                        ));
                                flag = false;
                            }
                            break;
                        case STRING:
                            if (attr.getValue().asString().length() > 64*1024) {
                                result.addPutRowResult(
                                        new RowResult(
                                                en.getKey(), 
                                                new Error(OTSErrorCode.INVALID_PARAMETER, "STRING ATTR SIZE > 64KB"),
                                                index
                                        ));
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
                        send(OTSOpType.PUT_ROW, change.getTableName(), change.getPrimaryKey(), toPairColumns(change.getColumnsToPut()));
                        result.addPutRowResult(
                                new RowResult(
                                        change.getTableName(), 
                                        new ConsumedCapacity(new CapacityUnit(0, 1)),
                                        index
                                )
                            );
                    } catch (RuntimeException e) {
                        LOG.warn("RuntimeException:{}", e.getMessage(), e);
                        result.addPutRowResult(
                                new RowResult(
                                        change.getTableName(), 
                                        new Error(OTSErrorCode.NOT_ENOUGH_CAPACITY_UNIT, "CU NOT ENOUGH"),
                                        index));
                    }
                }
                index++;
            }
        }
    }

    private void handleUpdateRow(Map<String, List<RowUpdateChange>> input, MockBatchWriteRowResult result) throws InterruptedException, OTSException {
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
                totalSize += Helper.getPKSize(change.getPrimaryKey());

                // column number的检查
                if (change.getColumnsToUpdate().size() > 128) {
                    throw new OTSException(
                            "Attribute column > 128", 
                            null, 
                            OTSErrorCode.INVALID_PARAMETER, 
                            "RequestId",
                            400);
                }
                
                
                // column name 合法性检查
                totalSize += Helper.getAttrSize(toColumns(change.getColumnsToUpdate()));

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

        int index = 0;
        for (Entry<String, List<RowUpdateChange>> en : input.entrySet()) {
            for (RowUpdateChange change : en.getValue()) {// row
                boolean flag = true;
                for (PrimaryKeyColumn pk : change.getPrimaryKey().getPrimaryKeyColumns()) {
                    switch(pk.getValue().getType()) {
                        case STRING:
                            if (pk.getValue().asString().length() > 1024) {
                                result.addUpdateRowResult( 
                                        new RowResult(
                                                en.getKey(), 
                                                new Error(OTSErrorCode.INVALID_PARAMETER, "STRING PK SIZE > 1KB"),
                                                index
                                                ));
                                flag = false;
                            }
                            break;
                        default:
                            break;}
                }
                for (Pair<Column, Type> attr : change.getColumnsToUpdate()) {
                    switch (attr.getFirst().getValue().getType()) {
                        case BINARY:
                            if (attr.getFirst().getValue().asBinary().length > 64*1024) {
                                result.addUpdateRowResult(
                                        new RowResult(
                                                en.getKey(), 
                                                new Error(OTSErrorCode.INVALID_PARAMETER, "BINARY ATTR SIZE > 64KB"),
                                                index
                                        ));
                                flag = false;
                            }

                            break;
                        case STRING:
                            if (attr.getFirst().getValue().asString() != null && attr.getFirst().getValue().asString().length() > 64*1024) {
                                result.addUpdateRowResult( 
                                        new RowResult(
                                                en.getKey(), 
                                                new Error(OTSErrorCode.INVALID_PARAMETER, "STRING ATTR SIZE > 64KB"),
                                                index
                                                ));
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
                        send(OTSOpType.UPDATE_ROW, change.getTableName(), change.getPrimaryKey(), change.getColumnsToUpdate());
                        result.addUpdateRowResult(
                                new RowResult(
                                        change.getTableName(), 
                                        new ConsumedCapacity(new CapacityUnit(0, 1)),
                                        index));
                    } catch (RuntimeException e) {
                        LOG.warn("RuntimeException:{}", e.getMessage(), e);
                        result.addUpdateRowResult(
                                new RowResult(
                                        change.getTableName(), 
                                        new Error(OTSErrorCode.NOT_ENOUGH_CAPACITY_UNIT, "CU NOT ENOUGH"),
                                        index
                                        ));
                    }
                }
            }
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
            if (change.getColumnsToPut().size() > 128) {
                throw new OTSException(
                        "Attribute column > 128", 
                        null, 
                        OTSErrorCode.INVALID_PARAMETER, 
                        "RequestId",
                        400);
            }
            
            for (Column c : change.getColumnsToPut()) {
                if (!Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*").matcher(c.getName()).matches()) {
                    throw new OTSException(
                            "Column name invalid", 
                            null, 
                            OTSErrorCode.INVALID_PARAMETER, 
                            "RequestId",
                            400);
                }
            }
            
            // mock sql client
            for (PrimaryKeyColumn pkc : change.getPrimaryKey().getPrimaryKeyColumns()) {
                switch(pkc.getValue().getType()) {
                    case STRING:
                        if (pkc.getValue().asString().length() > 1024) {
                            throw new OTSException("STRING PK SIZE > 1KB", null, OTSErrorCode.INVALID_PARAMETER, "request_id", 400);
                        }
                        break;
                    default:
                        break;}
            }
            for (Column attr : change.getColumnsToPut()) {
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
                send(OTSOpType.PUT_ROW, change.getTableName(), change.getPrimaryKey(), toPairColumns(change.getColumnsToPut()));
                OTSResult meta = new OTSResult();
                meta.setRequestID("requsetid put row");
                meta.setTraceId("tracerid");
                return new PutRowResult(
                        meta, 
                        new ConsumedCapacity(new CapacityUnit(0, 1))
                );
            } catch (RuntimeException e) {
                throw new OTSException("CU NOT ENOUGH", null, OTSErrorCode.NOT_ENOUGH_CAPACITY_UNIT, "request_id", 403);
            }
        } finally {
            conInvokeTimes.decrementAndGet();
        }
    }

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
            if (change.getColumnsToUpdate().size() > 128) {
                throw new OTSException(
                        "Attribute column > 128", 
                        null, 
                        OTSErrorCode.INVALID_PARAMETER, 
                        "RequestId",
                        400);
            }
            
            for (Pair<Column, Type> pair : change.getColumnsToUpdate()) {
                if (!Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*").matcher(pair.getFirst().getName()).matches()) {
                    throw new OTSException(
                            "Column name invalid", 
                            null, 
                            OTSErrorCode.INVALID_PARAMETER, 
                            "RequestId",
                            400);
                }
            }
            
            // mock sql client
            for (PrimaryKeyColumn pkc : change.getPrimaryKey().getPrimaryKeyColumns()) {
                switch(pkc.getValue().getType()) {
                    case STRING:
                        if (pkc.getValue().asString().length() > 1024) {
                            throw new OTSException("STRING PK SIZE > 1KB", null, OTSErrorCode.INVALID_PARAMETER, "request_id", 400);
                        }
                        break;
                    default:
                        break;}
            }

            for (Pair<Column, Type> pair : change.getColumnsToUpdate()) {
                switch (pair.getFirst().getValue().getType()) {
                    case BINARY:
                        if (pair.getFirst().getValue().asBinary().length > 64*1024) {
                            throw new OTSException("BINARY ATTR SIZE > 64KB", null, OTSErrorCode.INVALID_PARAMETER, "request_id", 400);
                        }
                        break;
                    case STRING:
                        if (pair.getFirst().getValue().asString().length() > 64*1024) {
                            throw new OTSException("STRING ATTR SIZE > 64KB", null, OTSErrorCode.INVALID_PARAMETER, "request_id", 400);
                        }
                        break;
                    default:
                        break;
                }
            }
            
            try {
                send(OTSOpType.UPDATE_ROW, change.getTableName(), change.getPrimaryKey(), change.getColumnsToUpdate());
                OTSResult meta = new OTSResult();
                meta.setRequestID("requsetid update row");
                meta.setTraceId("tracerid");
                return new UpdateRowResult(meta, new ConsumedCapacity(new CapacityUnit(0, 1)));
            } catch (RuntimeException e) {
                throw new OTSException("CU NOT ENOUGH", null, OTSErrorCode.NOT_ENOUGH_CAPACITY_UNIT, "request_id", 403);
            }
        } finally {
            conInvokeTimes.decrementAndGet();
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
            MockBatchWriteRowResult result = new MockBatchWriteRowResult(meta);
            
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
    
    @Override
    public GetRangeResult getRange(GetRangeRequest getRangeRequest)
            throws OTSException, ClientException {
        
        MockGetRangeResult result = new MockGetRangeResult(new ArrayList<Row>(lines.values()));
        return result.toGetRangeResult();
    }
    
    // ########################################################################
    
    @Override
    public CreateTableResult createTable(CreateTableRequest createTableRequest)
            throws OTSException, ClientException {
        throw new RuntimeException("Unimplements");
    }

    @Override
    public UpdateTableResult updateTable(UpdateTableRequest updateTableRequest)
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
    public LoadTableResult loadTable(LoadTableRequest loadTableRequest)
            throws OTSException, ClientException {
        throw new RuntimeException("Unimplements");
    }

    @Override
    public UnloadTableResult unloadTable(UnloadTableRequest unloadTableRequest)
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
    public Iterator<Row> createRangeIterator(
            RangeIteratorParameter rangeIteratorParameter) throws OTSException,
            ClientException {
        throw new RuntimeException("Unimplements");
    }

    @Override
    public void shutdown() {
    }
    
}
