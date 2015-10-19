package com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils;

import com.alibaba.datax.plugin.reader.otsstreamreader.internal.common.ConfigurationHelper;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.common.TestHelper;
import com.aliyun.openservices.ots.internal.OTS;
import com.aliyun.openservices.ots.internal.streamclient.DependencyException;
import com.aliyun.openservices.ots.internal.streamclient.StreamClientException;
import com.aliyun.openservices.ots.internal.streamclient.lease.LeaseManager;
import com.aliyun.openservices.ots.internal.streamclient.lease.LeaseManagerRetryStrategy;
import com.aliyun.openservices.ots.internal.streamclient.lease.ShardLease;
import com.aliyun.openservices.ots.internal.streamclient.lease.ShardLeaseSerializer;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestStreamClientHelper {

    private static String streamId = "StreamReaderTestDataTable_12312321";
    private static String statusTable = "StreamReaderTestStatusTable";

    @Before
    public void before() {
        TestHelper.deleteTable(statusTable);
    }

    @Test
    public void testClearAllLeaseStatus() throws StreamClientException, DependencyException {
        OTS ots = ConfigurationHelper.getOTSFromConfig();

        LeaseManager<ShardLease> leaseManager = new LeaseManager<ShardLease>(ots, statusTable,
                new ShardLeaseSerializer(statusTable, streamId), new LeaseManagerRetryStrategy(), 100);

        leaseManager.createLeaseTableIfNotExists(1, 1, -1);
        leaseManager.waitUntilTableReady(100000);

        int leaseNum = 12345;
        for (int i = 0; i < leaseNum; i++) {
            ShardLease shardLease = new ShardLease("shard" + i);
            shardLease.setStreamId(streamId);
            leaseManager.createLease(shardLease);
        }

        List<ShardLease> leaseList = leaseManager.listLeases();
        assertEquals(leaseNum, leaseList.size());

        StreamClientHelper.clearAllLeaseStatus(ots, statusTable, streamId);

        leaseList = leaseManager.listLeases();
        assertEquals(0, leaseList.size());
        ots.shutdown();
    }

}
