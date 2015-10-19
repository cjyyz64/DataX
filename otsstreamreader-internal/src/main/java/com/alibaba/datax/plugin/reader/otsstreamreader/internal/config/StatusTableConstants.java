package com.alibaba.datax.plugin.reader.otsstreamreader.internal.config;

import com.aliyun.openservices.ots.internal.model.PrimaryKeySchema;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyType;

import java.util.Arrays;
import java.util.List;

public class StatusTableConstants {

    public static List<PrimaryKeySchema> STATUS_TABLE_PK_SCHEMA = Arrays.asList(
            new PrimaryKeySchema("StreamId", PrimaryKeyType.STRING),
            new PrimaryKeySchema("StatusType", PrimaryKeyType.STRING),
            new PrimaryKeySchema("StatusValue", PrimaryKeyType.STRING));

    public static String PK1_STREAM_ID = "StreamId";
    public static String PK2_STATUS_TYPE = "StatusType";
    public static String PK3_STATUS_VALUE = "StatusValue";


    /**
     * 记录对应某一时刻的所有Shard的Checkpoint，假如n个Shard，会记录n＋1行，其中一行记录Shard总数，即n。
     * 格式如下：
     *
     * PK1 : StreamId   : "dataTable_131231"
     * PK2 : StatusType  : "CheckpointsAtTimePoint"
     *
     * 记录Checkpoint：
     *      PK3    : StatusValue : "1444357620415   shard1"  (Time + \t + ShardId)
     *      Column : Checkpoint  : "checkpoint"
     * 记录ShardCount：
     *      PK3    : StatusValue : "1444357620415"   (Time)
     *      Column : ShardCount  : 3
     *
     */
    public static String STATUS_TYPE_CHECKPOINT = "CheckpointForDataxReader";
    public static String TIME_SHARD_SEPARATOR = "\t";
    public static String LARGEST_SHARD_ID = String.valueOf((char)127);  //用于确定GetRange的范围。
    public static String CHECKPOINT_COLUMN_NAME = "Checkpoint";
    public static String SHARDCOUNT_COLUMN_NAME = "ShardCount";


    /**
     * 记录Shard的Lease信息，格式如下：
     * PK1 : StreamId   : "dataTable_13321231"
     * PK2 : StatusType  : "LeaseKey"
     * PK3 : StatusValue : "shardId1"
     * Columns : LeaseOwner, LeaseCounter...
     */
    public static String STATUS_TYPE_LEASE = "LeaseKey";
}
