package com.alibaba.datax.plugin.reader.otsstreamreader.internal.common;

import com.aliyun.openservices.ots.internal.OTS;
import com.aliyun.openservices.ots.internal.model.*;

import java.util.List;

public class StreamHelper {

    public static String getStreamId(OTS ots, String tableName) {
        ListStreamResult listStreamResult = ots.listStream(new ListStreamRequest(tableName));
        return listStreamResult.getStreams().get(0).getStreamId();
    }

    public static List<StreamShard> getShardList(OTS ots, String tableName) {
        DescribeStreamResult describeStreamResult = ots.describeStream(new DescribeStreamRequest(getStreamId(ots, tableName)));
        return describeStreamResult.getShards();
    }

    public static String getShardIterator(OTS ots, String tableName) {
        GetShardIteratorResult getShardIteratorResult = ots.getShardIterator(
                new GetShardIteratorRequest(getStreamId(ots, tableName), getShardList(ots, tableName).get(0).getShardId()));
        return getShardIteratorResult.getShardIterator();
    }

}
