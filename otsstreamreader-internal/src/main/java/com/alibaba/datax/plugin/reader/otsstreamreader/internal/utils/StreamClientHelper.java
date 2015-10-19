package com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConfig;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConstants;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.StatusTableConstants;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.core.CheckpointTimeTracker;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.core.RecordProcessor;
import com.aliyun.openservices.ots.internal.OTS;
import com.aliyun.openservices.ots.internal.model.*;
import com.aliyun.openservices.ots.internal.streamclient.ClientConfig;
import com.aliyun.openservices.ots.internal.streamclient.StreamConfig;
import com.aliyun.openservices.ots.internal.streamclient.Worker;
import com.aliyun.openservices.ots.internal.streamclient.model.IRecordProcessor;
import com.aliyun.openservices.ots.internal.streamclient.model.IRecordProcessorFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class StreamClientHelper {

    public static IRecordProcessorFactory getRecordProcessorFactory(
            final OTS ots,
            final OTSStreamReaderConfig config,
            final boolean shouldSkip,
            final Map<String, String> shardToCheckpointMap,
            final Map<String, Long> shardToLastProcessTimeMap,
            final CheckpointTimeTracker checkpointTimeTracker,
            final RecordSender recordSender) {

        return new IRecordProcessorFactory() {
            public IRecordProcessor createProcessor() {
                return new RecordProcessor(ots, config, shouldSkip, shardToCheckpointMap,
                        shardToLastProcessTimeMap, checkpointTimeTracker, recordSender);
            }
        };
    }

    public static Worker getWorkerInstance(OTS ots, OTSStreamReaderConfig config, IRecordProcessorFactory factory, ExecutorService executorService) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setLeaseDurationMillis(OTSStreamReaderConstants.LEASE_DURATION_TIME_MILLIS);

        StreamConfig streamConfig = new StreamConfig();
        streamConfig.setOTSClient(ots);
        streamConfig.setDataTableName(config.getDataTable());
        streamConfig.setStatusTableName(config.getStatusTable());

        Worker worker = new Worker("Datax_OTSStream_Reader", clientConfig,
                streamConfig, factory, executorService);
        return worker;
    }

    public static void clearAllLeaseStatus(OTS ots, String statusTable, String streamId) {
        List<PrimaryKeyColumn> startPkCols = Arrays.asList(
                new PrimaryKeyColumn(StatusTableConstants.PK1_STREAM_ID, PrimaryKeyValue.fromString(streamId)),
                new PrimaryKeyColumn(StatusTableConstants.PK2_STATUS_TYPE, PrimaryKeyValue.fromString(StatusTableConstants.STATUS_TYPE_LEASE)),
                new PrimaryKeyColumn(StatusTableConstants.PK3_STATUS_VALUE, PrimaryKeyValue.INF_MIN));
        List<PrimaryKeyColumn> endPkCols = Arrays.asList(
                new PrimaryKeyColumn(StatusTableConstants.PK1_STREAM_ID, PrimaryKeyValue.fromString(streamId)),
                new PrimaryKeyColumn(StatusTableConstants.PK2_STATUS_TYPE, PrimaryKeyValue.fromString(StatusTableConstants.STATUS_TYPE_LEASE)),
                new PrimaryKeyColumn(StatusTableConstants.PK3_STATUS_VALUE, PrimaryKeyValue.INF_MAX));

        PrimaryKey startPk = new PrimaryKey(startPkCols);
        PrimaryKey endPk = new PrimaryKey(endPkCols);

        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(statusTable);
        rangeRowQueryCriteria.setMaxVersions(1);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(endPk);
        GetRangeRequest getRangeRequest = new GetRangeRequest(rangeRowQueryCriteria);

        PrimaryKey nextPk = startPk;
        do {
            rangeRowQueryCriteria.setInclusiveStartPrimaryKey(nextPk);
            GetRangeResult result = ots.getRange(getRangeRequest);
            for (Row row : result.getRows()) {
                RowDeleteChange rowDeleteChange = new RowDeleteChange(statusTable, row.getPrimaryKey());
                ots.deleteRow(new DeleteRowRequest(rowDeleteChange));
            }
            nextPk = result.getNextStartPrimaryKey();
        } while (nextPk != null);
    }

}
