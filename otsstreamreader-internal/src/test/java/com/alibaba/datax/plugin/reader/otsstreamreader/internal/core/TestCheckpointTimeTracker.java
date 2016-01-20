package com.alibaba.datax.plugin.reader.otsstreamreader.internal.core;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.common.ConfigurationHelper;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.common.TestHelper;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConfig;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.OTSHelper;
import com.aliyun.openservices.ots.internal.OTS;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

public class TestCheckpointTimeTracker {

    private static OTS ots;
    private static String statusTableName = "StatusTable_TestCheckpointTimeTracker";
    private static String dataTableNameStreamId = "DataTableStreamId_TestCheckpointTimeTracker";
    private static OTSStreamReaderChecker otsStreamReaderChecker;

    @BeforeClass
    public static void beforeClass() {
        Configuration configuration = ConfigurationHelper.loadConf(dataTableNameStreamId, statusTableName, 1, 2);
        OTSStreamReaderConfig config = OTSStreamReaderConfig.load(configuration);
        ots = OTSHelper.getOTSInstance(config);
        otsStreamReaderChecker = new OTSStreamReaderChecker(ots, config);
    }

    @Before
    public void deleteAndRecreateTable() {
        TestHelper.deleteTable(statusTableName);
        otsStreamReaderChecker.checkAndCreateStatusTableIfNotExist();
    }

    @Test
    public void testSetAndGet() {
        CheckpointTimeTracker checkpointTimeTracker = new CheckpointTimeTracker(ots, statusTableName, dataTableNameStreamId);
        checkpointTimeTracker.setCheckpoint(1000, "shard", "checkpoint");
        String checkpoint = checkpointTimeTracker.getCheckpoint(1000, "shard");
        assertEquals("checkpoint", checkpoint);

        checkpoint = checkpointTimeTracker.getCheckpoint(1000, "dadsadsa");
        assertEquals(null, checkpoint);
        checkpoint = checkpointTimeTracker.getCheckpoint(1001, "shard");
        assertEquals(null, checkpoint);

        checkpointTimeTracker.setShardCountForCheck(10000, 100);
        int shardCount = checkpointTimeTracker.getShardCountForCheck(10000);
        assertEquals(100, shardCount);

        shardCount = checkpointTimeTracker.getShardCountForCheck(10001);
        assertEquals(-1, shardCount);
        shardCount = checkpointTimeTracker.getShardCountForCheck(100000);
        assertEquals(-1, shardCount);
    }

    @Test
    public void testGetAllCheckpoints() {
        CheckpointTimeTracker checkpointTimeTracker = new CheckpointTimeTracker(ots, statusTableName, dataTableNameStreamId);

        for (int i = 0; i < 10001; i++) {
            checkpointTimeTracker.setCheckpoint(1000, "shard" + i, "checkpoint" + i);
        }

        Map<String, String> allCheckpoints = checkpointTimeTracker.getAllCheckpoints(1000);
        assertEquals(10001, allCheckpoints.size());
        for (int i = 0; i < 10001; i++) {
            String expect = "checkpoint" + i;
            assertEquals(expect, allCheckpoints.get("shard" + i));
        }

        allCheckpoints = checkpointTimeTracker.getAllCheckpoints(999);
        assertEquals(0, allCheckpoints.size());
        allCheckpoints = checkpointTimeTracker.getAllCheckpoints(10000);
        assertEquals(0, allCheckpoints.size());
        allCheckpoints = checkpointTimeTracker.getAllCheckpoints(1001);
        assertEquals(0, allCheckpoints.size());
    }

    @Test
    public void testClearShardCountAndAllCheckpoints() {
        CheckpointTimeTracker checkpointTimeTracker = new CheckpointTimeTracker(ots, statusTableName, dataTableNameStreamId);

        for (int i = 0; i < 100; i++) {
            checkpointTimeTracker.setCheckpoint(Long.MAX_VALUE, "shard" + i, "checkpoint" + i);
        }
        checkpointTimeTracker.setShardCountForCheck(Long.MAX_VALUE, 10000);

        checkpointTimeTracker.clearShardCountAndAllCheckpoints(Long.MAX_VALUE);

        int shardCount = checkpointTimeTracker.getShardCountForCheck(Long.MAX_VALUE);
        Map<String, String> allCheckpoints = checkpointTimeTracker.getAllCheckpoints(Long.MAX_VALUE);

        assertEquals(-1, shardCount);
        assertEquals(0, allCheckpoints.size());
    }
}
