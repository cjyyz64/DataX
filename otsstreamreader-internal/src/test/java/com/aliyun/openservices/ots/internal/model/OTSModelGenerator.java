package com.aliyun.openservices.ots.internal.model;

import java.util.ArrayList;
import java.util.List;

public class OTSModelGenerator {

    public ListStreamResult genListStreamResult(String tableName, String streamId, long creationTime) {
        ListStreamResult result = new ListStreamResult();
        List<Stream> streamList = new ArrayList<Stream>();
        Stream stream = new Stream();
        stream.setTableName(tableName);
        stream.setStreamId(streamId);
        stream.setCreationTime(creationTime);
        streamList.add(stream);
        result.setStreams(streamList);
        return result;
    }

    public DescribeStreamResult genDescribeStreamResult(String tableName,
                                                        String streamId,
                                                        long creationTime,
                                                        int expirationTime,
                                                        List<StreamShard> shards) {
        DescribeStreamResult result = new DescribeStreamResult();
        result.setTableName(tableName);
        result.setStreamId(streamId);
        result.setCreationTime(creationTime);
        result.setExpirationTime(expirationTime);
        result.setStatus(StreamStatus.ACTIVE);
        result.setShards(shards);
        return result;
    }

    public GetShardIteratorResult genGetShardIteratorResult(String shardIterator) {
        GetShardIteratorResult result = new GetShardIteratorResult();
        result.setShardIterator(shardIterator);
        return result;
    }

    public GetStreamRecordResult genGetStreamRecordResult(List<StreamRecord> streamRecords, String nextIterator) {
        GetStreamRecordResult result = new GetStreamRecordResult();
        result.setRecords(streamRecords);
        result.setNextShardIterator(nextIterator);
        return result;
    }

    public DescribeTableResult genDescribeTableResult(StreamDetails streamDetails) {
        DescribeTableResult describeTableResult = new DescribeTableResult();
        describeTableResult.setStreamDetails(streamDetails);
        return describeTableResult;
    }
}
