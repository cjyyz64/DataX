package com.alibaba.datax.plugin.reader.otsstreamreader.internal.core;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.OTSStreamReaderException;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.common.ConfigurationHelper;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.common.RecordSenderForTest;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConfig;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.OTSHelper;
import com.aliyun.openservices.ots.internal.OTS;
import com.aliyun.openservices.ots.internal.model.RecordSequenceInfo;
import com.aliyun.openservices.ots.internal.model.StreamRecord;
import com.aliyun.openservices.ots.internal.streamclient.DependencyException;
import com.aliyun.openservices.ots.internal.streamclient.ShutdownException;
import com.aliyun.openservices.ots.internal.streamclient.StreamClientException;
import com.aliyun.openservices.ots.internal.streamclient.model.*;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestRecordProcessor {


    private static OTS ots;
    private static String dataTable = "DataxReaderTestDataTable";
    private static String statusTable = "DataxReaderTestStatusTable";
    private static OTSStreamReaderConfig config;

    @BeforeClass
    public static void beforeClass() {
        Configuration configuration = ConfigurationHelper.loadConf(dataTable, statusTable, 1, 2);
        config = OTSStreamReaderConfig.load(configuration);
        ots = OTSHelper.getOTSInstance(config);
    }

    @Test
    public void testInitialize() {
        Map<String, String> shardToCheckpoint = new HashMap<String, String>();
        Map<String, Long> shardToLastProcessTime = new HashMap<String, Long>();
        boolean shouldSkip = true;
        CheckpointTimeTracker checkpointTimeTracker = new CheckpointTimeTracker(ots, config.getStatusTable(), "streamId");
        RecordProcessor recordProcessor = new RecordProcessor(ots, config, shouldSkip,
                shardToCheckpoint, shardToLastProcessTime, checkpointTimeTracker, new RecordSenderForTest());

        {
            /**
             * shardToCheckpoint里没有对应shard
             */
            final boolean[] shutdown = {false};
            InitializationInput initializationInput = new InitializationInput();
            initializationInput.setShardInfo(new ShardInfo("shard", "", new ArrayList<String>(), ""));
            initializationInput.setShutdownMarker(new IShutdownMarker() {
                public void markForProcessDone() {
                    shutdown[0] = true;
                }

                public void markForProcessRestart() {

                }
            });

            recordProcessor.initialize(initializationInput);
            assertEquals(true, shutdown[0]);
        }

        {
            /**
             * shardToCheckpoint里有对应shard
             * 测试更新checkpoint，shardToLastProcessTime
             */
            shardToCheckpoint.put("shard", "checkpoint");
            final boolean[] shutdown = {false};
            InitializationInput initializationInput = new InitializationInput();
            initializationInput.setShardInfo(new ShardInfo("shard", "", new ArrayList<String>(), ""));
            initializationInput.setShutdownMarker(new IShutdownMarker() {
                public void markForProcessDone() {
                    shutdown[0] = true;
                }

                public void markForProcessRestart() {

                }
            });
            final String[] checkpoint = new String[1];
            initializationInput.setCheckpointer(new IRecordProcessorCheckpointer() {
                public void checkpoint() throws ShutdownException, StreamClientException, DependencyException {

                }

                public void checkpoint(String s) throws ShutdownException, StreamClientException, DependencyException {
                    checkpoint[0] = s;
                }

                public String getLargestPermittedCheckpointValue() {
                    return null;
                }
            });
            recordProcessor.initialize(initializationInput);
            assertEquals(false, shutdown[0]);
            assertEquals("checkpoint", checkpoint[0]);
            assertTrue((System.currentTimeMillis() - shardToLastProcessTime.get("shard")) < 1000);
        }

        {
            /**
             * 测试重复initialize
             */
            shardToCheckpoint.put("shard", "checkpoint");
            final boolean[] shutdown = {false};
            InitializationInput initializationInput = new InitializationInput();
            initializationInput.setShardInfo(new ShardInfo("shard", "", new ArrayList<String>(), ""));
            initializationInput.setShutdownMarker(new IShutdownMarker() {
                public void markForProcessDone() {
                    shutdown[0] = true;
                }

                public void markForProcessRestart() {

                }
            });
            final String[] checkpoint = new String[1];
            initializationInput.setCheckpointer(new IRecordProcessorCheckpointer() {
                public void checkpoint() throws ShutdownException, StreamClientException, DependencyException {

                }

                public void checkpoint(String s) throws ShutdownException, StreamClientException, DependencyException {
                    checkpoint[0] = s;
                }

                public String getLargestPermittedCheckpointValue() {
                    return null;
                }
            });
            try {
                recordProcessor.initialize(initializationInput);
                fail();
            } catch (OTSStreamReaderException ex) {
                assertTrue(ex.getMessage().startsWith("Internal state error: reinitialized"));
            }
        }
    }

    class RecordProcessorEx extends RecordProcessor {

        private List<StreamRecord> records = new ArrayList<StreamRecord>();

        public RecordProcessorEx(OTS ots, OTSStreamReaderConfig config, boolean shouldSkip, Map<String, String> shardToCheckpointMap, Map<String, Long> shardToLastProcessTimeMap, CheckpointTimeTracker checkpointTimeTracker, RecordSender recordSender) {
            super(ots, config, shouldSkip, shardToCheckpointMap, shardToLastProcessTimeMap, checkpointTimeTracker, recordSender);
            shardId = "shard";
        }

        @Override
        String getIterator(List<StreamRecord> records, int idx) {
            return "iterator" + idx;
        }

        @Override
        void sendRecord(StreamRecord record) {
            records.add(record);
        }

        public List<StreamRecord> getRecordsToSend() {
            return records;
        }
    }

    @Test
    public void testProcess() {
        OTSStreamReaderChecker checker = new OTSStreamReaderChecker(ots, config);
        checker.checkAndCreateStatusTableIfNotExist();

        long startTime = System.currentTimeMillis() - 3000 * 1000;
        long endTime = System.currentTimeMillis() - 1000 * 1000;
        config.setStartTimestampMillis(startTime);
        config.setEndTimestampMillis(endTime);

        Map<String, String> shardToCheckpoint = new HashMap<String, String>();
        Map<String, Long> shardToLastProcessTime = new HashMap<String, Long>();
        CheckpointTimeTracker checkpointTimeTracker = new CheckpointTimeTracker(ots, config.getStatusTable(), "streamId");

        ProcessRecordsInput processRecordsInput = new ProcessRecordsInput();
        final boolean shutdown[] = {false};
        processRecordsInput.setShutdownMarker(new IShutdownMarker() {
            public void markForProcessDone() {
                shutdown[0] = true;
            }

            public void markForProcessRestart() {

            }
        });
        final String checkpoint[] = {null};
        processRecordsInput.setCheckpointer(new IRecordProcessorCheckpointer() {
            public void checkpoint() throws ShutdownException, StreamClientException, DependencyException {
                checkpoint[0] = "checkpoint";
            }

            public void checkpoint(String s) throws ShutdownException, StreamClientException, DependencyException {
                checkpoint[0] = s;
            }

            public String getLargestPermittedCheckpointValue() {
                return "checkpoint";
            }
        });

        {
            for (int i = 0; i < 2; i++) {
                boolean shouldSkip = (i == 0);
                RecordProcessorEx recordProcessorEx = new RecordProcessorEx(ots, config, shouldSkip,
                        shardToCheckpoint, shardToLastProcessTime, checkpointTimeTracker, new RecordSenderForTest());
                List<StreamRecord> records = new ArrayList<StreamRecord>();
                List<StreamRecord> recordsToSend;
                for (int idx = 0; idx < 1000; idx++) {
                    StreamRecord streamRecord = new StreamRecord();
                    streamRecord.setSequenceInfo(new RecordSequenceInfo(0, startTime - 100 + idx / 2, idx % 2));
                    records.add(streamRecord);
                }
                processRecordsInput.setRecords(records);
                recordProcessorEx.processRecords(processRecordsInput);
                recordsToSend = recordProcessorEx.getRecordsToSend();
                if (shouldSkip) {
                    assertEquals(startTime, recordsToSend.get(0).getSequenceInfo().getTimestamp());
                } else {
                    assertEquals(records.get(0).getSequenceInfo().getTimestamp(), recordsToSend.get(0).getSequenceInfo().getTimestamp());
                }
                assertEquals(records.get(999).getSequenceInfo().getTimestamp(), recordsToSend.get(recordsToSend.size() - 1).getSequenceInfo().getTimestamp());
                assertEquals("checkpoint", checkpoint[0]);
                assertTrue(System.currentTimeMillis() - shardToLastProcessTime.get("shard") < 1000);
            }
        }

        {
            checkpointTimeTracker.clearShardCountAndAllCheckpoints(config.getEndTimestampMillis());

            RecordProcessorEx recordProcessorEx = new RecordProcessorEx(ots, config, true,
                    shardToCheckpoint, shardToLastProcessTime, checkpointTimeTracker, new RecordSenderForTest());
            List<StreamRecord> records = new ArrayList<StreamRecord>();
            List<StreamRecord> recordsToSend;
            for (int idx = 0; idx < 1000; idx++) {
                StreamRecord streamRecord = new StreamRecord();
                streamRecord.setSequenceInfo(new RecordSequenceInfo(0, endTime - 300 + idx / 2, idx % 2));
                records.add(streamRecord);
            }
            processRecordsInput.setRecords(records);
            recordProcessorEx.processRecords(processRecordsInput);
            recordsToSend = recordProcessorEx.getRecordsToSend();
            assertEquals(records.get(0).getSequenceInfo().getTimestamp(), recordsToSend.get(0).getSequenceInfo().getTimestamp());
            assertEquals(endTime - 1, recordsToSend.get(recordsToSend.size() - 1).getSequenceInfo().getTimestamp());
            assertEquals("checkpoint", checkpoint[0]);
            assertEquals("iterator" + recordsToSend.size(), checkpointTimeTracker.getCheckpoint(config.getEndTimestampMillis(), "shard"));
            assertTrue(shardToLastProcessTime.get("shard") > System.currentTimeMillis() * 2);
        }

    }

}
