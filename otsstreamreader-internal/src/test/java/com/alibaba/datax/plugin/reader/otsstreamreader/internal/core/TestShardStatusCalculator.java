package com.alibaba.datax.plugin.reader.otsstreamreader.internal.core;

import com.aliyun.openservices.ots.internal.model.StreamShard;
import com.aliyun.openservices.ots.internal.streamclient.model.CheckpointPosition;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestShardStatusCalculator {

    @Test
    public void testGetShardsNotBlockOnParents() {
        /**
         *     101 102   103
         *        0      1       2 3
         *        *      *       * *
         *      4  5    6 7       8
         *      *       *         *
         *
         *   4     5    6 7       8
         *   *          *         *
         *  9 10        11      12 13
         *                          *
         *                        14 15
         *
         *                          16 17
         *
         * 101, 102, 103: expired shards
         *  *: consumed shard end.
         *
         */
        List<StreamShard> shards = new ArrayList<StreamShard>();
        int parents[][] = {{101, 102}, {103, -1}, {-1, -1}, {-1, -1},
                {0, -1}, {0, -1}, {1, -1}, {1, -1}, {2, 3},
                {4, -1}, {4, -1}, {6, 7}, {8, -1}, {8, -1},
                {13, -1}, {13, -1},
                {15, -1}, {15, -1}};
        boolean consumedEnd[] = {true, true, true, true,
                true, false, true, false, true,
                false, false, false, false, true,
                false, false,
                false, false};
        int notBlockShards[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15};

        Map<String, String> shardToCheckpoint = new HashMap<String, String>();
        for (int i = 0; i < 18; i++) {
            StreamShard shard = new StreamShard("shard" + i);
            if (parents[i][0] != -1) {
                shard.setParentId("shard" + parents[i][0]);
            }
            if (parents[i][1] != -1) {
                shard.setParentSiblingId("shard" + parents[i][1]);
            }
            shards.add(shard);
            if (consumedEnd[i]) {
                shardToCheckpoint.put("shard" + i, CheckpointPosition.SHARD_END);
            }
        }
        List<String> result = ShardStatusCalculator.getShardsNotBlockOnParents(shards, shardToCheckpoint);
        assertEquals(notBlockShards.length, result.size());
        for (int i = 0; i < notBlockShards.length; i++) {
            assertEquals("shard" + notBlockShards[i], result.get(i));
        }
    }

    @Test
    public void testGetShardsHasParentDone() {
        /**
         *     101 102   103
         *        0      1       2 3
         *        *      *       $ *
         *      4  5    6 7       8
         *      $
         *
         *   4     5    6 7       8
         *   $
         *  9 10        11      12 13
         *
         *                        14 15
         *
         *                          16 17
         *
         * 101, 102, 103: expired shards
         *  *: consumed shard end.
         *  $: process done.
         */

        List<StreamShard> shards = new ArrayList<StreamShard>();
        int parents[][] = {{101, 102}, {103, -1}, {-1, -1}, {-1, -1},
                {0, -1}, {0, -1}, {1, -1}, {1, -1}, {2, 3},
                {4, -1}, {4, -1}, {6, 7}, {8, -1}, {8, -1},
                {13, -1}, {13, -1},
                {15, -1}, {15, -1}};
        boolean consumedEnd[] = {true, true, false, true,
                false, false, false, false, false,
                false, false, false, false, false,
                false, false,
                false, false};
        boolean processDone[] = {false, false, true, false,
                true, false, false, false, false,
                false, false, false, false, false,
                false, false,
                false, false};
        int hasParentDone[] = {8, 9, 10, 12, 13, 14, 15, 16, 17};

        Map<String, String> shardToCheckpoint = new HashMap<String, String>();
        for (int i = 0; i < 18; i++) {
            StreamShard shard = new StreamShard("shard" + i);
            if (parents[i][0] != -1) {
                shard.setParentId("shard" + parents[i][0]);
            }
            if (parents[i][1] != -1) {
                shard.setParentSiblingId("shard" + parents[i][1]);
            }
            shards.add(shard);
            if (consumedEnd[i]) {
                shardToCheckpoint.put("shard" + i, CheckpointPosition.SHARD_END);
            }
            if (processDone[i]) {
                shardToCheckpoint.put("shard" + i, "processDone.");
            }
        }
        List<String> result = ShardStatusCalculator.getShardsHasParentProcessDone(shards, shardToCheckpoint);
        assertEquals(hasParentDone.length, result.size());
        for (int i = 0; i < hasParentDone.length; i++) {
            assertEquals("shard" + hasParentDone[i], result.get(i));
        }
    }

}
