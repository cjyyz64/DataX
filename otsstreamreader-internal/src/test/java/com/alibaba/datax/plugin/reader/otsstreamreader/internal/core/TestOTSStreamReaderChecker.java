package com.alibaba.datax.plugin.reader.otsstreamreader.internal.core;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.OTSStreamReaderException;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.common.ConfigurationHelper;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.common.TestHelper;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConfig;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConstants;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.model.OTSErrorCode;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.OTSHelper;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.StreamClientHelper;
import com.aliyun.openservices.ots.internal.OTS;
import com.aliyun.openservices.ots.internal.OTSException;
import com.aliyun.openservices.ots.internal.model.*;
import com.aliyun.openservices.ots.internal.streamclient.Worker;
import com.aliyun.openservices.ots.internal.streamclient.model.CheckpointPosition;
import com.aliyun.openservices.ots.internal.streamclient.model.IRecordProcessorFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestOTSStreamReaderChecker {

    private static OTS ots;
    private static String dataTable = "DataTable_TestOTSStreamReaderChecker";
    private static String statusTable = "StatusTable_TestOTSStreamReaderChecker";
    private static OTSStreamReaderConfig config;

    @BeforeClass
    public static void beforeClass() {
        Configuration configuration = ConfigurationHelper.loadConf(dataTable, statusTable, 1, 2);
        config = OTSStreamReaderConfig.load(configuration);
        ots = OTSHelper.getOTSInstance(config);
    }

    @Before
    public void deleteTable() {
        TestHelper.deleteTable(dataTable);
        TestHelper.deleteTable(statusTable);
    }

    @Test
    public void testCheckStreamEnabledAndTimeStampOK() {
        OTSStreamReaderChecker checker = new OTSStreamReaderChecker(ots, config);
        {
            try {
                checker.checkStreamEnabledAndTimeRangeOK();
                fail();
            } catch (OTSStreamReaderException ex) {
                assertEquals("The data table is not existing.", ex.getMessage());
            }
        }

        {
            TableMeta tableMeta = new TableMeta(config.getDataTable());
            tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("pk", PrimaryKeyType.STRING));
            TableOptions tableOptions = new TableOptions(-1, 1);
            ReservedThroughput reservedThroughput = new ReservedThroughput(1000, 1000);
            CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta, tableOptions, reservedThroughput);
            ots.createTable(createTableRequest);

            try {
                checker.checkStreamEnabledAndTimeRangeOK();
                fail();
            } catch (OTSStreamReaderException ex) {
                assertEquals("The data table stream is not enabled.", ex.getMessage());
            }
        }

        {
            UpdateTableRequest updateTableRequest = new UpdateTableRequest(config.getDataTable());
            updateTableRequest.setStreamSpecification(new StreamSpecification(true, 3));
            ots.updateTable(updateTableRequest);
            config.setStartTimestampMillis(1);
            config.setEndTimestampMillis(2);
            try {
                checker.checkStreamEnabledAndTimeRangeOK();
                fail();
            } catch (OTSStreamReaderException ex) {
                assertTrue(ex.getMessage().startsWith("Start timestamp(1) is too small"));
            }
        }

        {
            config.setStartTimestampMillis(System.currentTimeMillis());
            config.setEndTimestampMillis(System.currentTimeMillis() + 1);
            try {
                checker.checkStreamEnabledAndTimeRangeOK();
                fail();
            } catch (OTSStreamReaderException ex) {
                assertTrue(ex.getMessage().startsWith("End timestamp(" + config.getEndTimestampMillis() + ") is too large"));
            }
        }

        {
            config.setStartTimestampMillis(System.currentTimeMillis()
                    - 3 * 3600 * 1000 + OTSStreamReaderConstants.BEFORE_OFFSET_TIME_MILLIS - 1);
            config.setEndTimestampMillis(System.currentTimeMillis() - 1 * 3600 * 1000);
            try {
                checker.checkStreamEnabledAndTimeRangeOK();
                fail();
            } catch (OTSStreamReaderException ex) {
                assertTrue(ex.getMessage().startsWith("Start timestamp(" + config.getStartTimestampMillis() + ") is too small"));
            }
        }

        {
            config.setStartTimestampMillis(System.currentTimeMillis() - 3 * 3600 * 1000 + OTSStreamReaderConstants.BEFORE_OFFSET_TIME_MILLIS + 10000);
            config.setEndTimestampMillis(System.currentTimeMillis() - OTSStreamReaderConstants.AFTER_OFFSET_TIME_MILLIS - 1);
            checker.checkStreamEnabledAndTimeRangeOK();
        }
    }

    @Test
    public void testCheckAndCreateStatusTable() {
        OTSStreamReaderChecker checker = new OTSStreamReaderChecker(ots, config);

        boolean created = OTSHelper.checkTableExists(ots, config.getStatusTable());
        assertEquals(false, created);

        checker.checkAndCreateStatusTableIfNotExist();

        created = OTSHelper.checkTableExists(ots, config.getStatusTable());
        assertEquals(true, created);
    }

    @Test
    public void testCheckAndSetCheckpointsAtStartTimestamp() {
        OTSStreamReaderChecker checker = new OTSStreamReaderChecker(ots, config);
        checker.checkAndCreateStatusTableIfNotExist();

        CheckpointTimeTracker checkpointTimeTracker = new CheckpointTimeTracker(ots, config.getStatusTable(), "streamId");

        {
            /**
             * findCheckpoints ＝ false
             * 1. 没有checkpoint
             * 2. 有checkpoint，没有shardCount
             * 3. 有checkpoint，有shardCount，shardCount与checkpoint个数不相同
             */
            config.setStartTimestampMillis(1000);

            Map<String, String> currentShardCheckpoints = new HashMap<String, String>();
            boolean findCheckpoints = checker.checkAndSetCheckpointsAtStartTimestamp(checkpointTimeTracker, currentShardCheckpoints);
            assertEquals(false, findCheckpoints);

            List<String> shards = Arrays.asList("shard1", "shard2", "shard3");
            for (String shardId : shards) {
                checkpointTimeTracker.setCheckpoint(config.getStartTimestampMillis(), shardId, shardId + "_checkpoint");
            }

            findCheckpoints = checker.checkAndSetCheckpointsAtStartTimestamp(checkpointTimeTracker, currentShardCheckpoints);
            assertEquals(false, findCheckpoints);
            Map<String, String> allCheckpoints = checkpointTimeTracker.getAllCheckpoints(config.getStartTimestampMillis());
            assertEquals(0, allCheckpoints.size());

            for (String shardId : shards) {
                checkpointTimeTracker.setCheckpoint(config.getStartTimestampMillis(), shardId, shardId + "_cp");
            }
            checkpointTimeTracker.setShardCountForCheck(config.getStartTimestampMillis(), 4);
            findCheckpoints = checker.checkAndSetCheckpointsAtStartTimestamp(checkpointTimeTracker, currentShardCheckpoints);
            assertEquals(false, findCheckpoints);
            allCheckpoints = checkpointTimeTracker.getAllCheckpoints(config.getStartTimestampMillis());
            assertEquals(0, allCheckpoints.size());
        }

        {
            /**
             * statusTable记录了一个Checkpoint不为SHARD_END的Shard，而该Shard目前不存在，需要抛错。
             */
            config.setStartTimestampMillis(1000);
            Map<String, String> currentShardCheckpoints = new HashMap<String, String>();

            List<String> shards = Arrays.asList("shard1", "shard2", "shard3");
            for (String shardId : shards) {
                checkpointTimeTracker.setCheckpoint(config.getStartTimestampMillis(), shardId, shardId + "_checkpoint");
            }
            checkpointTimeTracker.setShardCountForCheck(config.getStartTimestampMillis(), 3);
            try {
                boolean findCheckpoints = checker.checkAndSetCheckpointsAtStartTimestamp(checkpointTimeTracker, currentShardCheckpoints);
                fail();
            } catch (OTSStreamReaderException ex) {
                assertTrue(ex.getMessage().startsWith("Shard does not exist now"));
            }
        }

        {
            /**
             * statusTable记录的Shard比当前Shard数目少，setCheckpoints之后map里包含当前全部shard。
             */
            config.setStartTimestampMillis(1000);
            Map<String, String> currentShardCheckpoints = new HashMap<String, String>();
            List<String> shards = Arrays.asList("shard1", "shard2", "shard3", "shard4", "shard5");
            for (String shardId : shards) {
                currentShardCheckpoints.put(shardId, CheckpointPosition.TRIM_HORIZON);
            }
            for (int i = 0; i < 3; i++) {
                checkpointTimeTracker.setCheckpoint(config.getStartTimestampMillis(), shards.get(i), shards.get(i) + "_checkpoint");
            }

            checkpointTimeTracker.setShardCountForCheck(config.getStartTimestampMillis(), 3);
            boolean findCheckpoints = checker.checkAndSetCheckpointsAtStartTimestamp(checkpointTimeTracker, currentShardCheckpoints);
            assertEquals(true, findCheckpoints);
            for (int i = 0; i < 5; i++) {
                if (i < 3) {
                    assertEquals(shards.get(i) + "_checkpoint", currentShardCheckpoints.get(shards.get(i)));
                } else {
                    assertEquals(CheckpointPosition.TRIM_HORIZON, currentShardCheckpoints.get(shards.get(i)));
                }
            }
        }
    }

    @Test
    public void checkLastProcessTime() {
        OTSStreamReaderChecker checker = new OTSStreamReaderChecker(ots, config);
        long maxProcessTime = OTSStreamReaderConstants.MAX_ONCE_PROCESS_TIME_MILLIS;

        {
            /**
             * 测试空Map
             */
            checker.checkLastProcessTime(new HashMap<String, Long>(), new HashMap<String, Long>(), maxProcessTime);
        }

        {
            /**
             * 测试readyShard没处理的情况
             */
            Map<String, Long> shardToReadyTimeMap = new HashMap<String, Long>();
            shardToReadyTimeMap.put("shard", System.currentTimeMillis() - maxProcessTime - 1);
            try {
                checker.checkLastProcessTime(shardToReadyTimeMap, new HashMap<String, Long>(), maxProcessTime);
                fail();
            } catch (OTSStreamReaderException ex) {
                assertTrue(ex.getMessage().startsWith("Too long didn't initialize shard"));
            }
        }

        {
            /**
             * 测试shard处理超时。
             */
            Map<String, Long> shardToReadyTimeMap = new HashMap<String, Long>();
            Map<String, Long> lastProcessTime = new HashMap<String, Long>();
            for (int i = 0; i < 10; i++) {
                shardToReadyTimeMap.put("shard" + i, 0L);
                if (i == 3) {
                    lastProcessTime.put("shard" + i, System.currentTimeMillis() - maxProcessTime - 1);
                } else {
                    lastProcessTime.put("shard" + i, System.currentTimeMillis() - maxProcessTime + 1000);
                }
            }

            try {
                checker.checkLastProcessTime(shardToReadyTimeMap, lastProcessTime, maxProcessTime);
                fail();
            } catch (OTSStreamReaderException ex) {
                assertTrue(ex.getMessage().startsWith("Process shard timeout, ShardId:shard3"));
            }
        }

        {
            /**
             * 测试没有超时的情况。
             */
            Map<String, Long> shardToReadyTimeMap = new HashMap<String, Long>();
            Map<String, Long> lastProcessTime = new HashMap<String, Long>();
            for (int i = 0; i < 10; i++) {
                shardToReadyTimeMap.put("shard" + i, 0L);
                lastProcessTime.put("shard" + i, System.currentTimeMillis() - maxProcessTime + 1000);
            }

            checker.checkLastProcessTime(shardToReadyTimeMap, lastProcessTime, maxProcessTime);
        }
    }

    @Test
    public void testCheckAllShardsProcessDone() {
        config.setEndTimestampMillis(1000);
        OTSStreamReaderChecker checker = new OTSStreamReaderChecker(ots, config);
        checker.checkAndCreateStatusTableIfNotExist();

        CheckpointTimeTracker checkpointTimeTracker = new CheckpointTimeTracker(ots, config.getStatusTable(), "streamId");
        {
            boolean done = checker.checkAllShardsProcessDone(checkpointTimeTracker.getAllCheckpoints(config.getEndTimestampMillis()), 10);
            assertEquals(false, done);
        }
        {
            for (int i = 0; i < 8; i++) {
                checkpointTimeTracker.setCheckpoint(config.getEndTimestampMillis(), "shard" + i, "checkpoint");
            }
            boolean done = checker.checkAllShardsProcessDone(checkpointTimeTracker.getAllCheckpoints(config.getEndTimestampMillis()), 10);
            assertEquals(false, done);
        }
        {
            for (int i = 8; i < 10; i++) {
                checkpointTimeTracker.setCheckpoint(config.getEndTimestampMillis(), "shard" + i, "checkpoint");
            }
            boolean done = checker.checkAllShardsProcessDone(checkpointTimeTracker.getAllCheckpoints(config.getEndTimestampMillis()), 10);
            assertEquals(true, done);
        }
        {
            for (int i = 10; i < 12; i++) {
                checkpointTimeTracker.setCheckpoint(config.getEndTimestampMillis(), "shard" + i, "checkpoint");
            }
            try {
                checker.checkAllShardsProcessDone(checkpointTimeTracker.getAllCheckpoints(config.getEndTimestampMillis()), 10);
                fail();
            } catch (OTSStreamReaderException ex) {
                assertEquals("Find more number of checkpoints(12) than shardCount(10).", ex.getMessage());
            }
        }
    }
}
