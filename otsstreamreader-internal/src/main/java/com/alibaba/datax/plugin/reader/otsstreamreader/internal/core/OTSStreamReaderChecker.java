package com.alibaba.datax.plugin.reader.otsstreamreader.internal.core;

import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConstants;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConfig;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.OTSStreamReaderException;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.StatusTableConstants;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.OTSHelper;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.TimeUtils;
import com.aliyun.openservices.ots.internal.OTS;
import com.aliyun.openservices.ots.internal.model.*;
import com.aliyun.openservices.ots.internal.streamclient.Worker;
import com.aliyun.openservices.ots.internal.streamclient.model.CheckpointPosition;
import com.aliyun.openservices.ots.internal.streamclient.model.WorkerStatus;

import java.util.List;
import java.util.Map;

public class OTSStreamReaderChecker {

    private final OTS ots;
    private final OTSStreamReaderConfig config;

    public OTSStreamReaderChecker(OTS ots, OTSStreamReaderConfig config) {
        this.ots = ots;
        this.config = config;
    }

    /**
     * 1. 检查dataTable是否开启了stream。
     * 2. 检查要导出的时间范围是否合理：
     *      最大可导出的时间范围为： ［now － expirationTime, now]
     *      为了避免时间误差影响，允许导出的范围为： [now - expirationTime + beforeOffset, now - afterOffset]
     */
    public void checkStreamEnabledAndTimeRangeOK() {
        boolean exists = OTSHelper.checkTableExists(ots, config.getDataTable());
        if (!exists) {
            throw new OTSStreamReaderException("The data table is not existing.");
        }
        StreamDetails streamDetails = OTSHelper.getStreamDetails(ots, config.getDataTable());
        if (!streamDetails.isEnableStream()) {
            throw new OTSStreamReaderException("The data table stream is not enabled.");
        }
        long now = System.currentTimeMillis();
        long startTime = config.getStartTimestampMillis();
        long endTime = config.getEndTimestampMillis();
        long beforeOffset = OTSStreamReaderConstants.BEFORE_OFFSET_TIME_MILLIS;
        long afterOffset = OTSStreamReaderConstants.AFTER_OFFSET_TIME_MILLIS;
        long expirationTime = streamDetails.getExpirationTime() * TimeUtils.HOUR_IN_MILLIS;

        if (startTime < now - expirationTime + beforeOffset) {
            throw new OTSStreamReaderException("Start timestamp(" + startTime + ") is too small with StreamExpirationTime:"
                    + expirationTime + ", BeforeOffsetTime:" + beforeOffset + ", Now:" + now + ".");
        }

        if (endTime > now - afterOffset) {
            throw new OTSStreamReaderException("End timestamp(" + endTime + ") is too large with AfterOffsetTime:"
                + afterOffset + ", Now:" + now + ".");
        }
    }

    /**
     * 检查statusTable的tableMeta
     * @param tableMeta
     */
    private void checkTableMetaOfStatusTable(TableMeta tableMeta) {
        List<PrimaryKeySchema> pkSchema = tableMeta.getPrimaryKeyList();
        if (!pkSchema.equals(StatusTableConstants.STATUS_TABLE_PK_SCHEMA)) {
            throw new OTSStreamReaderException("Unexpected table meta in status table, please check your config.");
        }
    }

    /**
     * 检查statusTable是否存在，如果不存在就创建statusTable，并等待表ready。
     */
    public void checkAndCreateStatusTableIfNotExist() {
        boolean tableExist = OTSHelper.checkTableExists(ots, config.getStatusTable());
        if (tableExist) {
            DescribeTableResult describeTableResult = OTSHelper.describeTable(ots, config.getStatusTable());
            checkTableMetaOfStatusTable(describeTableResult.getTableMeta());
        } else {
            TableMeta tableMeta = new TableMeta(config.getStatusTable());
            tableMeta.addPrimaryKeyColumns(StatusTableConstants.STATUS_TABLE_PK_SCHEMA);
            TableOptions tableOptions = new TableOptions(OTSStreamReaderConstants.STATUS_TABLE_TTL, 1);
            OTSHelper.createTable(ots, tableMeta, tableOptions);
            boolean tableReady = OTSHelper.waitUntilTableReady(ots, config.getStatusTable(),
                    OTSStreamReaderConstants.MAX_WAIT_TABLE_READY_TIME_MILLIS);
            if (!tableReady) {
                throw new OTSStreamReaderException("Check table ready timeout, MaxWaitTableReadyTimeMillis:"
                    + OTSStreamReaderConstants.MAX_WAIT_TABLE_READY_TIME_MILLIS + ".");
            }
        }
    }

    /**
     * 查找并设置初始的checkpoints，如果查找到对应时间的checkpoints，返回true，否则返回false。
     *
     * 注意事项：
     *  1. 当找到的checkpoints不全时，说明statusTable中可能有脏数据，需要清除这部分数据。
     *  2. 若之前记录的shard已经不存在，且该shard仍未读完，需要抛错。
     *
     * @param checkpointTimeTracker
     * @param currentShardCheckpointMap
     * @return
     */
    public boolean checkAndSetCheckpointsAtStartTimestamp(CheckpointTimeTracker checkpointTimeTracker,
                                                          Map<String, String> currentShardCheckpointMap) {
        long timestamp = config.getStartTimestampMillis();
        Map<String, String> map = checkpointTimeTracker.getAllCheckpoints(timestamp);
        int shardCount = checkpointTimeTracker.getShardCountForCheck(timestamp);
        if (shardCount == -1 && map.size() == 0) {
            return false;
        }
        if (shardCount != map.size()) {
            checkpointTimeTracker.clearShardCountAndAllCheckpoints(timestamp);
            return false;
        }
        for (String shardId : map.keySet()) {
            if (currentShardCheckpointMap.get(shardId) == null) {
                if (!map.get(shardId).equals(CheckpointPosition.SHARD_END)) {
                    throw new OTSStreamReaderException("Shard does not exist now, ShardId:"
                        + shardId + ", Checkpoint:" + map.get(shardId));
                }
            } else {
                currentShardCheckpointMap.put(shardId, map.get(shardId));
            }
        }
        return true;
    }

    /**
     * 检查Worker的状态。
     *
     * @param worker
     * @throws Exception
     */
    public void checkWorkerStatus(Worker worker) throws Exception {
        if (worker.getWorkerStatus().equals(WorkerStatus.RUNNING)) {
            return;
        } else {
            if (worker.getException() != null) {
                throw worker.getException();
            } else {
                throw new OTSStreamReaderException("The status of stream client worker should be RUNNING but "
                    + worker.getWorkerStatus() + ".");
            }
        }
    }

    /**
     * 检查shard的LastProcessTime，防止有shard hang住。
     * @param shardToReadyTimeMap
     * @param lastProcessTimeMap
     * @param maxProcessTime
     */
    public void checkLastProcessTime(Map<String, Long> shardToReadyTimeMap, Map<String, Long> lastProcessTimeMap, long maxProcessTime) {
        long now = System.currentTimeMillis();
        for (String shardId : shardToReadyTimeMap.keySet()) {
            if (lastProcessTimeMap.get(shardId) == null) {
                long readyTime = shardToReadyTimeMap.get(shardId);
                if (now - readyTime > maxProcessTime) {
                    throw new OTSStreamReaderException("Too long didn't initialize shard:" + shardId + ", ReadyTime"
                        + readyTime + ", MaxProcessTime:" + maxProcessTime + ", Now:" + now + ".");
                }
            } else {
                if (now - lastProcessTimeMap.get(shardId) > maxProcessTime) {
                    throw new OTSStreamReaderException("Process shard timeout, ShardId:" + shardId + ", LastProcessTime:"
                            + lastProcessTimeMap.get(shardId) + ", MaxProcessTime:" + maxProcessTime + ", Now:" + now + ".");
                }
            }
        }
    }

    /**
     * 检查是否所有shard都处理完成了。
     * shard处理完成后在statusTable中记录checkpoint，以此判断处理完成。
     *
     * @param shardCount
     * @return
     */
    public boolean checkAllShardsProcessDone(Map<String, String> checkpointMap,
                                             int shardCount) {
        if (checkpointMap.size() == shardCount) {
            return true;
        }
        if (checkpointMap.size() < shardCount) {
            return false;
        }
        throw new OTSStreamReaderException("Find more number of checkpoints(" + checkpointMap.size()
                + ") than shardCount(" + shardCount + ").");
    }
}
