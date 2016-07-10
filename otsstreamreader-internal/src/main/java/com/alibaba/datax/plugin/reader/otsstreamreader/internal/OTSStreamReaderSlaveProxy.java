package com.alibaba.datax.plugin.reader.otsstreamreader.internal;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConfig;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConstants;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.core.*;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.OTSHelper;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.StreamClientHelper;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.TimeUtils;
import com.aliyun.openservices.ots.internal.OTS;
import com.aliyun.openservices.ots.internal.OTSException;
import com.aliyun.openservices.ots.internal.model.StreamDetails;
import com.aliyun.openservices.ots.internal.model.StreamShard;
import com.aliyun.openservices.ots.internal.streamclient.Worker;
import com.aliyun.openservices.ots.internal.streamclient.model.CheckpointPosition;
import com.aliyun.openservices.ots.internal.streamclient.model.IRecordProcessorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class OTSStreamReaderSlaveProxy {
    private static final Logger LOG = LoggerFactory.getLogger(OTSStreamReaderSlaveProxy.class);

    private OTSStreamReaderConfig config;
    private OTS ots;
    private Worker worker;
    private Map<String, String> shardToCheckpointMap = new ConcurrentHashMap<String, String>();
    private Map<String, Long> shardToLastProcessTimeMap = new ConcurrentHashMap<String, Long>();
    private Map<String, Long> shardToReadyTimeMap = new ConcurrentHashMap<String, Long>();
    private CheckpointTimeTracker checkpointInfoTracker;
    private OTSStreamReaderChecker checker;
    private int shardCount;
    private boolean shouldSkip;
    private String streamId;
    private List<StreamShard> shards;

    public void init(final OTSStreamReaderConfig otsStreamReaderConfig) {
        this.config = otsStreamReaderConfig;
        this.ots = OTSHelper.getOTSInstance(config);
        StreamDetails streamDetails = OTSHelper.getStreamDetails(ots, config.getDataTable());
        this.streamId = streamDetails.getStreamId();
        this.checkpointInfoTracker = new CheckpointTimeTracker(ots, config.getStatusTable(), streamId);
        this.checker = new OTSStreamReaderChecker(ots, config);

        shards = OTSHelper.getOrderedShardList(ots, streamId);
        shardCount = shards.size();

        for (StreamShard shard : shards) {
            shardToCheckpointMap.put(shard.getShardId(), CheckpointPosition.TRIM_HORIZON);
        }
        boolean findCheckpoints = checker.checkAndSetCheckpointsAtStartTimestamp(checkpointInfoTracker, shardToCheckpointMap);
        shouldSkip = !findCheckpoints;

        /**
         * 为了减少扫描量, 获取每个分片最近一次的位点.
         */
        if(!findCheckpoints) {
            long expirationTime = (streamDetails.getExpirationTime() - 1) * TimeUtils.HOUR_IN_MILLIS;
            long timeRangeBegin = System.currentTimeMillis() - expirationTime;
            long timeRangeEnd = this.config.getStartTimestampMillis() - 1;
            if (timeRangeBegin < timeRangeEnd) {
                for (StreamShard shard : shards) {
                    String checkpoint = this.checkpointInfoTracker.getShardLargestCheckpointInTimeRange(shard.getShardId(), timeRangeBegin, timeRangeEnd);
                    if (checkpoint != null) {
                        shardToCheckpointMap.put(shard.getShardId(), checkpoint);
                    }
                }
            }
        }
    }

    private void updateShardToReadyTimeMap(List<StreamShard> orderedShardList, Map<String, String> checkpointMap) {
        List<String> readyShards = ShardStatusCalculator.getShardsNotBlockOnParents(orderedShardList, checkpointMap);
        LOG.info("ReadyShards: {}.", readyShards);
        for (String shardId : readyShards) {
            if (shardToReadyTimeMap.get(shardId) == null) {
                shardToReadyTimeMap.put(shardId, System.currentTimeMillis());
            }
        }
    }

    private void setCheckpointsOfShardHasParentDone(List<StreamShard> orderedShardList, Map<String, String> checkpointMap) {
        List<String> shardsHasParentProcessDone = ShardStatusCalculator.getShardsHasParentProcessDone(orderedShardList, checkpointMap);
        LOG.info("ShardsHasParentProcessDone: {}.", shardsHasParentProcessDone);
        for (String shardId : shardsHasParentProcessDone) {
            if (checkpointMap.get(shardId) == null) {
                checkpointInfoTracker.setCheckpoint(config.getEndTimestampMillis(), shardId, CheckpointPosition.TRIM_HORIZON);
            }
        }
    }

    private int calcThreadNum(Map<String, String> shardToCheckpointMap) {
        if (this.config.getThreadNum() > 0) {
            LOG.info("ThreadNum: {}.", this.config.getThreadNum());
            return this.config.getThreadNum();
        }
        int shardNotEnd = 0;
        for (Map.Entry<String, String> entry : shardToCheckpointMap.entrySet()) {
            if (!entry.getValue().equals(CheckpointPosition.SHARD_END)) {
                shardNotEnd++;
            }
        }
        int threadNum = Math.min(Runtime.getRuntime().availableProcessors() * 3, shardNotEnd + 2);
        LOG.info("ThreadNum: {}.", threadNum);
        return threadNum;
    }

    public void startRead(RecordSender recordSender) {
        try {
            IRecordProcessorFactory processorFactory =
                    StreamClientHelper.getRecordProcessorFactory(ots, config, shouldSkip, shardToCheckpointMap,
                            shardToLastProcessTimeMap, checkpointInfoTracker, recordSender);
            ExecutorService executorService = Executors.newFixedThreadPool(calcThreadNum(shardToCheckpointMap));
            worker = StreamClientHelper.getWorkerInstance(ots, config, processorFactory, executorService);

            Thread thread = new Thread(worker);
            thread.start();
            while (true) {
                TimeUtils.sleepMillis(OTSStreamReaderConstants.MAIN_THREAD_CHECK_INTERVAL_MILLIS);
                Map<String, String> checkpointMap = checkpointInfoTracker.getAllCheckpoints(config.getEndTimestampMillis());
                updateShardToReadyTimeMap(shards, checkpointMap);
                setCheckpointsOfShardHasParentDone(shards, checkpointMap);
                checker.checkWorkerStatus(worker);
                checker.checkLastProcessTime(shardToReadyTimeMap, shardToLastProcessTimeMap,
                        OTSStreamReaderConstants.MAX_ONCE_PROCESS_TIME_MILLIS);
                if (checker.checkAllShardsProcessDone(checkpointMap, shardCount)) {
                    checkpointInfoTracker.setShardCountForCheck(config.getEndTimestampMillis(), shardCount);
                    worker.shutdown();
                    StreamClientHelper.clearAllLeaseStatus(ots, config.getStatusTable(), streamId);
                    break;
                }
            }
            thread.join();
            executorService.shutdown();
        } catch (OTSException ex) {
            throw DataXException.asDataXException(new OTSReaderError(ex.getErrorCode(), "OTS Error"), ex.toString(), ex);
        } catch (OTSStreamReaderException ex) {
            throw DataXException.asDataXException(OTSReaderError.ERROR, ex.toString(), ex);
        } catch (Exception ex) {
            throw DataXException.asDataXException(OTSReaderError.ERROR, ex.toString(), ex);
        }
    }

    public void close() {
        ots.shutdown();
    }
}
