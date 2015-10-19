package com.alibaba.datax.plugin.reader.otsstreamreader.internal.core;

import com.aliyun.openservices.ots.internal.model.StreamShard;
import com.aliyun.openservices.ots.internal.streamclient.model.CheckpointPosition;

import java.util.*;

public class ShardStatusCalculator {

    /**
     * 获取没有block在parentShard上的shard列表，用于确定一个shard是否应该开始处理。
     *
     * @param orderedShardList
     * @param checkpointMap
     * @return
     */
    public static List<String> getShardsNotBlockOnParents(List<StreamShard> orderedShardList, Map<String, String> checkpointMap) {
        Set<String> shardsNotReachedEnd = new HashSet<String>();
        List<String> notBlockOnParentsShards = new ArrayList<String>();

        for (StreamShard shard : orderedShardList) {
            if (checkpointMap.get(shard.getShardId()) == null
                    || !checkpointMap.get(shard.getShardId()).equals(CheckpointPosition.SHARD_END)) {
                shardsNotReachedEnd.add(shard.getShardId());
            }
            boolean blockOnParents = false;
            if (shard.getParentId() != null && shardsNotReachedEnd.contains(shard.getParentId())) {
                blockOnParents = true;
            }
            if (shard.getParentSiblingId() != null && shardsNotReachedEnd.contains(shard.getParentSiblingId())) {
                blockOnParents = true;
            }
            if (!blockOnParents) {
                notBlockOnParentsShards.add(shard.getShardId());
            }
        }
        return notBlockOnParentsShards;
    }

    /**
     * 假如一个shard的parentShard存在大于等于endTime的数据，该parentShard处理完成后，其子shard不会再被处理。
     * 该方法返回由于parentShard处理完成而不需要处理的子shard列表。
     *
     * @param orderedShardList
     * @param checkpointMap
     * @return
     */
    public static List<String> getShardsHasParentProcessDone(List<StreamShard> orderedShardList, Map<String, String> checkpointMap) {
        Set<String> shardsProcessDone = new HashSet<String>();
        List<String> hasParentProcessDoneShards = new ArrayList<String>();

        for (StreamShard shard : orderedShardList) {
            if (checkpointMap.get(shard.getShardId()) != null
                    && !checkpointMap.get(shard.getShardId()).equals(CheckpointPosition.SHARD_END)) {
                shardsProcessDone.add(shard.getShardId());
            }
            boolean hasParentProcessDone = false;
            if (shard.getParentId() != null && shardsProcessDone.contains(shard.getParentId())) {
                hasParentProcessDone = true;
            }
            if (shard.getParentSiblingId() != null && shardsProcessDone.contains(shard.getParentSiblingId())) {
                hasParentProcessDone = true;
            }
            if (hasParentProcessDone) {
                hasParentProcessDoneShards.add(shard.getShardId());
                shardsProcessDone.add(shard.getShardId() );
            }
        }
        return hasParentProcessDoneShards;
    }

}
