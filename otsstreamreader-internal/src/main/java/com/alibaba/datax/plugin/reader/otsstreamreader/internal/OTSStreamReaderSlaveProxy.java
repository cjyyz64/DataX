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
import com.aliyun.openservices.ots.internal.model.StreamShard;
import com.aliyun.openservices.ots.internal.streamclient.Worker;
import com.aliyun.openservices.ots.internal.streamclient.model.CheckpointPosition;
import com.aliyun.openservices.ots.internal.streamclient.model.IRecordProcessorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public void init(final OTSStreamReaderConfig otsStreamReaderConfig) {
        this.config = otsStreamReaderConfig;
        this.ots = OTSHelper.getOTSInstance(config);
        this.streamId = OTSHelper.getStreamDetails(ots, config.getDataTable()).getStreamId();
        this.checkpointInfoTracker = new CheckpointTimeTracker(ots, config.getStatusTable(), streamId);
        this.checker = new OTSStreamReaderChecker(ots, config);

        List<StreamShard> shards = OTSHelper.getOrderedShardList(ots, streamId);
        shardCount = shards.size();
        for (StreamShard shard : shards) {
            shardToCheckpointMap.put(shard.getShardId(), CheckpointPosition.TRIM_HORIZON);
        }
        boolean findCheckpoints = checker.checkAndSetCheckpointsAtStartTimestamp(checkpointInfoTracker, shardToCheckpointMap);
        shouldSkip = !findCheckpoints;
    }

    private void updateShardToReadyTimeMap() {
        List<StreamShard> orderedShardList = OTSHelper.getOrderedShardList(ots, streamId);
        Map<String, String> checkpointMap = checkpointInfoTracker.getAllCheckpoints(config.getEndTimestampMillis());
        List<String> readyShards = ShardStatusCalculator.getShardsNotBlockOnParents(orderedShardList, checkpointMap);
        LOG.info("ReadyShards: {}.", readyShards);
        for (String shardId : readyShards) {
            if (shardToReadyTimeMap.get(shardId) == null) {
                shardToReadyTimeMap.put(shardId, System.currentTimeMillis());
            }
        }
    }

    private void setCheckpointsOfShardHasParentDone() {
        List<StreamShard> orderedShardList = OTSHelper.getOrderedShardList(ots, streamId);
        Map<String, String> checkpointMap = checkpointInfoTracker.getAllCheckpoints(config.getEndTimestampMillis());
        List<String> shardsHasParentProcessDone = ShardStatusCalculator.getShardsHasParentProcessDone(orderedShardList, checkpointMap);
        LOG.info("ShardsHasParentProcessDone: {}.", shardsHasParentProcessDone);
        for (String shardId : shardsHasParentProcessDone) {
            if (checkpointMap.get(shardId) == null) {
                checkpointInfoTracker.setCheckpoint(config.getEndTimestampMillis(), shardId, CheckpointPosition.TRIM_HORIZON);
            }
        }
    }

    public void startRead(RecordSender recordSender) {
        try {
            IRecordProcessorFactory processorFactory =
                    StreamClientHelper.getRecordProcessorFactory(ots, config, shouldSkip, shardToCheckpointMap,
                            shardToLastProcessTimeMap, checkpointInfoTracker, recordSender);
            ExecutorService executorService = Executors.newCachedThreadPool();
            worker = StreamClientHelper.getWorkerInstance(ots, config, processorFactory, executorService);

            Thread thread = new Thread(worker);
            thread.start();
            while (true) {
                TimeUtils.sleepMillis(OTSStreamReaderConstants.MAIN_THREAD_CHECK_INTERVAL_MILLIS);
                updateShardToReadyTimeMap();
                setCheckpointsOfShardHasParentDone();
                checker.checkWorkerStatus(worker);
                checker.checkLastProcessTime(shardToReadyTimeMap, shardToLastProcessTimeMap,
                        OTSStreamReaderConstants.MAX_ONCE_PROCESS_TIME_MILLIS);
                if (checker.checkAllShardsProcessDone(checkpointInfoTracker, shardCount)) {
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
