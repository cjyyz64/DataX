package com.alibaba.datax.plugin.reader.otsstreamreader.internal.common;

import com.alibaba.datax.plugin.reader.otsstreamreader.internal.model.OTSErrorCode;
import com.aliyun.openservices.ots.internal.ClientException;
import com.aliyun.openservices.ots.internal.OTS;
import com.aliyun.openservices.ots.internal.OTSException;
import com.aliyun.openservices.ots.internal.model.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MockOTS implements OTS {

    private OTSModelGenerator otsModelGenerator = new OTSModelGenerator();
    private long creationTime;

    private NavigableMap<String, StreamShard> shardMap = new ConcurrentSkipListMap<String, StreamShard>();
    private Map<String, List<StreamRecord>> shardRecords = new ConcurrentHashMap<String, List<StreamRecord>>();
    private Map<String, Boolean> isShardClose = new ConcurrentHashMap<String, Boolean>();
    private Map<String, Integer> startIteratorIdx = new ConcurrentHashMap<String, Integer>();

    private String tableName;
    private boolean enableStream;
    private int expirationTime;
    private OTS ots;

    public MockOTS() {

    }

    public MockOTS(OTS ots) {
        this.ots = ots;
    }

    public void setOTS(OTS ots) {
        this.ots = ots;
    }

    public void enableStream(String tableName, int expirationTime) {
        disableStream();
        this.tableName = tableName;
        this.enableStream = true;
        this.expirationTime = expirationTime;
        this.creationTime = System.currentTimeMillis();
    }

    public void disableStream() {
        this.enableStream = false;
        shardMap.clear();
        shardRecords.clear();
        isShardClose.clear();
        startIteratorIdx.clear();
    }

    private void checkEnableStream() {
        if (!enableStream) {
            throw new OTSException("", null, OTSErrorCode.OBJECT_NOT_EXIST, "", 404);
        }
    }
    public ListStreamResult listStream(ListStreamRequest listStreamRequest) throws OTSException, ClientException {
        checkEnableStream();

        return otsModelGenerator.genListStreamResult(listStreamRequest.getTableName(),
                listStreamRequest.getTableName() + "_Stream", creationTime);
    }

    public DescribeStreamResult describeStream(DescribeStreamRequest describeStreamRequest) throws OTSException, ClientException {
        checkEnableStream();

        List<StreamShard> shards = new ArrayList<StreamShard>();
        for (String shardId : shardMap.keySet()) {
            shards.add(shardMap.get(shardId));
        }
        return otsModelGenerator.genDescribeStreamResult(describeStreamRequest.getStreamId().split("_")[0],
                describeStreamRequest.getStreamId(), creationTime, expirationTime, shards);
    }

    public GetShardIteratorResult getShardIterator(GetShardIteratorRequest getShardIteratorRequest) throws OTSException, ClientException {
        checkEnableStream();

        String shardId = getShardIteratorRequest.getShardId();
        if (shardMap.get(shardId) == null) {
            throw new RuntimeException("Illegal state.");
        }
        long idx = startIteratorIdx.get(shardId);
        return otsModelGenerator.genGetShardIteratorResult(shardId + "\t" + idx);
    }

    public GetStreamRecordResult getStreamRecord(GetStreamRecordRequest getStreamRecordRequest) throws OTSException, ClientException {
        checkEnableStream();

        List<StreamRecord> streamRecords = new ArrayList<StreamRecord>();
        int limit = getStreamRecordRequest.getLimit();
        if (limit < 0) {
            limit = 10000000;
        }
        String iterator = getStreamRecordRequest.getShardIterator();

        if (iterator.split("\t").length != 2) {
            System.out.println(iterator);
            throw new OTSException("Requested stream data is already trimmed or does not exist.",
                    null, "OTSTrimmedDataAccess", "", 404);
        }

        String shardId = iterator.split("\t")[0];
        int idx = Integer.parseInt(iterator.split("\t")[1]);

        List<StreamRecord> list = shardRecords.get(shardId);
        synchronized (list) {
            long minIdx = startIteratorIdx.get(shardId);
            int endIdx = Math.min(idx + limit, list.size());

            if (idx < minIdx || idx > list.size()) {
                throw new OTSException("Requested stream data is already trimmed or does not exist.",
                        null, "OTSTrimmedDataAccess", "", 404);
            }

            for (int i = idx; i < endIdx; i++) {
                streamRecords.add(list.get(i));
            }
            String nextIterator = shardId + "\t" + endIdx;
            if (isShardClose.get(shardId) && endIdx == list.size()) {
                nextIterator = null;
            }
            return otsModelGenerator.genGetStreamRecordResult(streamRecords, nextIterator);
        }
    }

    public void createShard(String shardId, String parentShardId, String parentSiblingShardId) {
        checkEnableStream();

        synchronized (shardMap) {
            if (shardMap.get(shardId) != null) {
                throw new RuntimeException("Illegal state.");
            }
            StreamShard streamShard = new StreamShard(shardId);
            streamShard.setParentId(parentShardId);
            streamShard.setParentSiblingId(parentSiblingShardId);
            shardMap.put(shardId, streamShard);
            shardRecords.put(shardId, new ArrayList<StreamRecord>());
            isShardClose.put(shardId, false);
            startIteratorIdx.put(shardId, 0);
        }
    }

    public void appendRecords(String shardId, List<StreamRecord> records) {
        checkEnableStream();

        if (shardMap.get(shardId) == null || isShardClose.get(shardId)) {
            throw new RuntimeException("Illegal state.");
        }
        List<StreamRecord> list = shardRecords.get(shardId);
        synchronized (list) {
            list.addAll(records);
        }
    }

    private void closeShard(String shardId) {
        checkEnableStream();

        synchronized (isShardClose) {
            if (shardMap.get(shardId) == null || isShardClose.get(shardId)) {
                throw new RuntimeException("Illegal state.");
            }
            isShardClose.put(shardId, true);
        }
    }

    public void splitShard(String shardId, String resShardId1, String resShardId2) {
        checkEnableStream();

        closeShard(shardId);
        createShard(resShardId1, shardId, null);
        createShard(resShardId2, shardId, null);
    }

    public void mergeShard(String shardId1, String shardId2, String resShardId) {
        checkEnableStream();

        closeShard(shardId1);
        closeShard(shardId2);
        createShard(resShardId, shardId1, shardId2);
    }

    public void deleteShard(String shardId) {
        shardMap.remove(shardId);
        shardRecords.remove(shardId);
        isShardClose.remove(shardId);
        startIteratorIdx.remove(shardId);
    }

    public synchronized void clearExpirationRecords() {
        for (String shardId : shardRecords.keySet()) {
            List<StreamRecord> streamRecords = shardRecords.get(shardId);
            int startIdx = startIteratorIdx.get(shardId);
            synchronized (streamRecords) {
                for (int i = startIdx; i < streamRecords.size(); i++) {
                    long ts = streamRecords.get(i).getSequenceInfo().getTimestamp();
                    if (ts < (System.currentTimeMillis() - expirationTime)) {
                        startIdx++;
                        streamRecords.set(i, null);
                    }
                }
                startIteratorIdx.put(shardId, startIdx);
            }
        }
    }

    public List<StreamRecord> getShardRecords(String shardId) {
        checkEnableStream();

        return shardRecords.get(shardId);
    }

    public void shutdown() {
        if (ots != null) {
            ots.shutdown();
        }
    }

    public CreateTableResult createTable(CreateTableRequest createTableRequest) throws OTSException, ClientException {
        if (ots != null) {
            return ots.createTable(createTableRequest);
        }
        throw new UnsupportedOperationException("");
    }

    public UpdateTableResult updateTable(UpdateTableRequest updateTableRequest) throws OTSException, ClientException {
        if (ots != null) {
            return ots.updateTable(updateTableRequest);
        }
        throw new UnsupportedOperationException("");
    }

    public DescribeTableResult describeTable(DescribeTableRequest describeTableRequest) throws OTSException, ClientException {
        if (describeTableRequest.getTableName().equals(tableName)) {
            StreamDetails streamDetails = new StreamDetails(enableStream, tableName + "_Stream", expirationTime, 0);
            return otsModelGenerator.genDescribeTableResult(streamDetails);
        }
        if (ots != null) {
            return ots.describeTable(describeTableRequest);
        }
        throw new UnsupportedOperationException("");
    }

    public ListTableResult listTable() throws OTSException, ClientException {
        if (ots != null) {
            return ots.listTable();
        }
        throw new UnsupportedOperationException("");
    }

    public DeleteTableResult deleteTable(DeleteTableRequest deleteTableRequest) throws OTSException, ClientException {
        if (ots != null) {
            return ots.deleteTable(deleteTableRequest);
        }
        throw new UnsupportedOperationException("");
    }

    public LoadTableResult loadTable(LoadTableRequest loadTableRequest) throws OTSException, ClientException {
        throw new UnsupportedOperationException("");
    }

    public UnloadTableResult unloadTable(UnloadTableRequest unloadTableRequest) throws OTSException, ClientException {
        throw new UnsupportedOperationException("");
    }

    public GetRowResult getRow(GetRowRequest getRowRequest) throws OTSException, ClientException {
        if (ots != null) {
            return ots.getRow(getRowRequest);
        }
        throw new UnsupportedOperationException("");
    }

    public PutRowResult putRow(PutRowRequest putRowRequest) throws OTSException, ClientException {
        if (ots != null) {
            return ots.putRow(putRowRequest);
        }
        throw new UnsupportedOperationException("");
    }

    public UpdateRowResult updateRow(UpdateRowRequest updateRowRequest) throws OTSException, ClientException {
        if (ots != null) {
            return ots.updateRow(updateRowRequest);
        }
        throw new UnsupportedOperationException("");
    }

    public DeleteRowResult deleteRow(DeleteRowRequest deleteRowRequest) throws OTSException, ClientException {
        if (ots != null) {
            return ots.deleteRow(deleteRowRequest);
        }
        throw new UnsupportedOperationException("");
    }

    public BatchGetRowResult batchGetRow(BatchGetRowRequest batchGetRowRequest) throws OTSException, ClientException {
        if (ots != null) {
            return ots.batchGetRow(batchGetRowRequest);
        }
        throw new UnsupportedOperationException("");
    }

    public BatchWriteRowResult batchWriteRow(BatchWriteRowRequest batchWriteRowRequest) throws OTSException, ClientException {
        if (ots != null) {
            return ots.batchWriteRow(batchWriteRowRequest);
        }
        throw new UnsupportedOperationException("");
    }

    public GetRangeResult getRange(GetRangeRequest getRangeRequest) throws OTSException, ClientException {
        if (ots != null) {
            return ots.getRange(getRangeRequest);
        }
        throw new UnsupportedOperationException("");
    }

    public Iterator<Row> createRangeIterator(RangeIteratorParameter rangeIteratorParameter) throws OTSException, ClientException {
        if (ots != null) {
            return ots.createRangeIterator(rangeIteratorParameter);
        }
        throw new UnsupportedOperationException("");
    }
}
