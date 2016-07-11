package com.alibaba.datax.plugin.reader.otsstreamreader.internal.core;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.Mode;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConfig;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.OTSStreamReaderException;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.TimeUtils;
import com.aliyun.openservices.ots.internal.OTS;
import com.aliyun.openservices.ots.internal.model.GetShardIteratorRequest;
import com.aliyun.openservices.ots.internal.model.GetStreamRecordRequest;
import com.aliyun.openservices.ots.internal.model.GetStreamRecordResult;
import com.aliyun.openservices.ots.internal.model.StreamRecord;
import com.aliyun.openservices.ots.internal.streamclient.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class RecordProcessor implements IRecordProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(RecordProcessor.class);
    private static final long LARGE_TIMESTAMP = System.currentTimeMillis() * 10;
    private static final long RECORD_CHECKPOINT_INTERVAL = 10 * TimeUtils.MINUTE_IN_MILLIS;

    private final OTS ots;
    private final long startTimestampMillis;
    private final long endTimestampMillis;
    private final OTSStreamReaderConfig readerConfig;
    private boolean shouldSkip;
    private final Map<String, String> shardToCheckpointMap;
    private final Map<String, Long> shardToLastProcessTimeMap;
    private final CheckpointTimeTracker checkpointTimeTracker;
    private final RecordSender recordSender;
    private final boolean isExportSequenceInfo;
    private IStreamRecordSender otsStreamRecordSender;
    private long lastRecordCheckpointTime;

    private String lastCheckpoint;
    protected String shardId; // "protected" for unit test

    public RecordProcessor(OTS ots,
                           OTSStreamReaderConfig config,
                           boolean shouldSkip,
                           Map<String, String> shardToCheckpointMap,
                           Map<String, Long> shardToLastProcessTimeMap,
                           CheckpointTimeTracker checkpointTimeTracker,
                           RecordSender recordSender) {
        this.ots = ots;
        this.readerConfig = config;
        this.startTimestampMillis = config.getStartTimestampMillis();
        this.endTimestampMillis = config.getEndTimestampMillis();
        this.shouldSkip = shouldSkip;
        this.shardToCheckpointMap = shardToCheckpointMap;
        this.shardToLastProcessTimeMap = shardToLastProcessTimeMap;
        this.checkpointTimeTracker = checkpointTimeTracker;
        this.recordSender = recordSender;
        this.isExportSequenceInfo = config.isExportSequenceInfo();
        this.lastRecordCheckpointTime = startTimestampMillis;
    }

    public void initialize(InitializationInput initializationInput) {
        try {
            this.shardId = initializationInput.getShardInfo().getShardId();
            LOG.info("Initialize: shardId: {}.", shardId);

            if (readerConfig.getMode().equals(Mode.MULTI_VERSION)) {
                this.otsStreamRecordSender = new MultiVerModeRecordSender(recordSender, shardId, isExportSequenceInfo);
            } else if (readerConfig.getMode().equals(Mode.SINGLE_VERSION_AND_UPDATE_ONLY)) {
                this.otsStreamRecordSender = new SingleVerAndUpOnlyModeRecordSender(recordSender, shardId, isExportSequenceInfo, readerConfig.getColumns());
            } else {
                throw new OTSStreamReaderException("Internal Error. Unhandled Mode: " + readerConfig.getMode());
            }

            String checkpoint = shardToCheckpointMap.get(shardId);
            if (checkpoint == null) {
                /**
                 * shardToCheckpointMap不包含该Shard，说明该Shard是在job启动后生成的。
                 * 该Shard内不会存在过去时间范围内的数据，因此不对其处理。
                 */
                initializationInput.getShutdownMarker().markForProcessDone();
                return;
            } else {
                /**
                 * 初始化Checkpoint。
                 */
                if (checkpoint.equals(CheckpointPosition.TRIM_HORIZON)) {
                    String streamId = initializationInput.getShardInfo().getStreamId();
                    checkpoint = ots.getShardIterator(new GetShardIteratorRequest(streamId, shardId)).getShardIterator();
                }
                IRecordProcessorCheckpointer checkpointer = initializationInput.getCheckpointer();
                checkpointer.checkpoint(checkpoint);
                lastCheckpoint = checkpoint;
            }

            if (shardToLastProcessTimeMap.get(shardId) != null) {
                /**
                 * 不允许重复初始化，即期望一个Shard自始至终都由一个Consumer处理。
                 */
                throw new OTSStreamReaderException("Internal state error: reinitialized shard: " + shardId + ".");
            }

            /**
             * 用于保持与Reader插件主线程的心跳，防止hang死。
             */
            shardToLastProcessTimeMap.put(shardId, System.currentTimeMillis());
        } catch (Exception ex) {
            LOG.error("{}", ex);
            throw new OTSStreamReaderException(ex.getMessage(), ex);
        }
    }

    private long getTimestamp(StreamRecord record) {
        return record.getSequenceInfo().getTimestamp() / 1000;
    }

    String getIterator(List<StreamRecord> records, int idx) {
        if (idx == 0) {
            return lastCheckpoint;
        } else {
            GetStreamRecordRequest request = new GetStreamRecordRequest(lastCheckpoint);
            request.setLimit(idx);
            GetStreamRecordResult result = ots.getStreamRecord(request);
            if (result.getRecords().size() != idx) {
                throw new OTSStreamReaderException("Expect " + idx + " records but " +
                    result.getRecords().size() + " records.");
            }
            return result.getNextShardIterator();
        }
    }

    void sendRecord(StreamRecord record) {
        otsStreamRecordSender.sendToDatax(record);
    }

    boolean process(List<StreamRecord> records, String largestPermittedCheckpoint) {
        if (records.size() == 0) {
            if (!largestPermittedCheckpoint.equals(CheckpointPosition.SHARD_END)) {
                checkpointTimeTracker.setCheckpoint(endTimestampMillis, shardId, lastCheckpoint);
                checkpointTimeTracker.setShardTimeCheckpoint(shardId, endTimestampMillis, lastCheckpoint);
                return true;
            }
        }
        int size = records.size();
        for (int i = 0; i < size; i++) {
            long timestamp = getTimestamp(records.get(i));
            if (timestamp < endTimestampMillis) {
                if (timestamp >= lastRecordCheckpointTime + RECORD_CHECKPOINT_INTERVAL) {
                    lastRecordCheckpointTime = timestamp;
                    checkpointTimeTracker.setShardTimeCheckpoint(shardId, timestamp, getIterator(records, i));
                }
                if (shouldSkip && (timestamp < startTimestampMillis)) {
                    continue;
                }
                shouldSkip = false;
                sendRecord(records.get(i));
            } else {
                String iterator = getIterator(records, i);
                checkpointTimeTracker.setCheckpoint(endTimestampMillis, shardId, iterator);
                checkpointTimeTracker.setShardTimeCheckpoint(shardId, endTimestampMillis, iterator);
                return true;
            }
        }
        return false;
    }

    public void processRecords(ProcessRecordsInput processRecordsInput) {
        try {
            List<StreamRecord> records = processRecordsInput.getRecords();

            if (records.isEmpty()) {
                LOG.info("StartProcessRecords: size: {}.", records.size());
            } else {
                LOG.info("StartProcessRecords: size: {}, recordTime: {}.", records.size(), getTimestamp(records.get(0)));
            }

            if (process(records, processRecordsInput.getCheckpointer().getLargestPermittedCheckpointValue())) {
                /**
                 * 该Shard处理完成，LastProcessTime设为一个很大的时间戳。
                 */
                shardToLastProcessTimeMap.put(shardId, LARGE_TIMESTAMP);
                processRecordsInput.getShutdownMarker().markForProcessDone();
            } else {
                shardToLastProcessTimeMap.put(shardId, System.currentTimeMillis());
            }

            IRecordProcessorCheckpointer checkpointer = processRecordsInput.getCheckpointer();
            lastCheckpoint = checkpointer.getLargestPermittedCheckpointValue();
            checkpointer.checkpoint();

            LOG.info("ProcessRecords, Size:{}, ProcessTime:{}, LastCheckpoint:{}",
                    records.size(), shardToLastProcessTimeMap.get(shardId), lastCheckpoint);
        } catch (Exception ex) {
            LOG.error("{}", ex);
            throw new OTSStreamReaderException(ex.getMessage(), ex);
        }
    }

    public void shutdown(ShutdownInput shutdownInput) {
        try {
            ShutdownReason reason = shutdownInput.getShutdownReason();

            LOG.info("Shutdown: shardId: {}, reason: {}.", shardId, reason);

            if (reason == ShutdownReason.TERMINATE) {
                /**
                 * 该Shard处理完成，LastProcessTime设为一个很大的时间戳。
                 */
                shardToLastProcessTimeMap.put(shardId, LARGE_TIMESTAMP);
                checkpointTimeTracker.setCheckpoint(endTimestampMillis, shardId, CheckpointPosition.SHARD_END);
                checkpointTimeTracker.setShardTimeCheckpoint(shardId, endTimestampMillis, CheckpointPosition.SHARD_END);
                shutdownInput.getCheckpointer().checkpoint();
            }
            if (reason != ShutdownReason.TERMINATE && reason != ShutdownReason.PROCESS_DONE) {
                throw new OTSStreamReaderException("Internal state error: unexpected shutdown, reason: " + reason + ".", null);
            }
        } catch (Exception ex) {
            LOG.debug("{}", ex);
            throw new OTSStreamReaderException(ex.getMessage(), ex);
        }
    }
}
