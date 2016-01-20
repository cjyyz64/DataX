package com.alibaba.datax.plugin.reader.otsstreamreader.internal.core;

import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.StatusTableConstants;
import com.aliyun.openservices.ots.internal.OTS;
import com.aliyun.openservices.ots.internal.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CheckpointTimeTracker {

    private static final Logger LOG = LoggerFactory.getLogger(CheckpointTimeTracker.class);

    private final OTS ots;
    private final String statusTable;
    private final String streamId;

    public CheckpointTimeTracker(OTS ots, String statusTable, String streamId) {
        this.ots = ots;
        this.statusTable = statusTable;
        this.streamId = streamId;
    }

    /**
     * 记录timestamp时刻，shardId对应的shard的checkpoint。
     * @param timestamp
     * @param shardId
     * @param checkpointValue
     */
    public void setCheckpoint(long timestamp, String shardId, String checkpointValue) {
        PutRowRequest putRowRequest = getOTSRequestForSetCheckpoint(timestamp, shardId, checkpointValue);
        ots.putRow(putRowRequest);
        LOG.info("SetCheckpoint: timestamp: {}, shardId: {}, checkpointValue: {}.", timestamp, shardId, checkpointValue);
    }

    /**
     * 获取timestamp时刻，shardId对应的shard的checkpoint。
     * @param timestamp
     * @param shardId
     * @return
     */
    public String getCheckpoint(long timestamp, String shardId) {
        PrimaryKey primaryKey = getPrimaryKeyForCheckpoint(timestamp, shardId);
        GetRowRequest getRowRequest = getOTSRequestForGet(primaryKey);
        Row row = ots.getRow(getRowRequest).getRow();
        if (row == null) {
            return null;
        }
        String checkpoint = row.getColumn(StatusTableConstants.CHECKPOINT_COLUMN_NAME).get(0).getValue().asString();
        LOG.info("GetCheckpoint: timestamp: {}, shardId: {}, checkpoint: {}.", timestamp, shardId, checkpoint);
        return checkpoint;
    }

    /**
     * shardCount为某一时刻记录了checkpoint的shard的数目，用于检查checkpoints是否完整。
     * @param timestamp
     * @param shardCount
     */
    public void setShardCountForCheck(long timestamp, int shardCount) {
        PutRowRequest putRowRequest = getOTSRequestForSetShardCount(timestamp, shardCount);
        ots.putRow(putRowRequest);
        LOG.info("SetShardCount: timestamp: {}, shardCount: {}.", timestamp, shardCount);
    }

    /**
     * 返回timestamp时刻记录了checkpoint的shard的个数，用于检查checkpoints是否完整。
     *
     * @param timestamp
     * @return 如果status表中未记录shardCount信息，返回－1
     */
    public int getShardCountForCheck(long timestamp) {
        PrimaryKey primaryKey = getPrimaryKeyForShardCount(timestamp);
        GetRowRequest getRowRequest = getOTSRequestForGet(primaryKey);
        Row row = ots.getRow(getRowRequest).getRow();
        if (row == null) {
            return -1;
        }
        int shardCount = (int) row.getColumn(StatusTableConstants.SHARDCOUNT_COLUMN_NAME).get(0).getValue().asLong();
        LOG.info("GetShardCount: timestamp: {}, shardCount: {}.", timestamp, shardCount);
        return shardCount;
    }

    public Map<String, String> getAllCheckpoints(long timestamp) {
        GetRangeRequest getRangeRequest = getOTSRequestForGetAllCheckpoints(timestamp);
        List<Row> rows = new ArrayList<Row>();
        GetRangeResult getRangeResult = ots.getRange(getRangeRequest);
        rows.addAll(getRangeResult.getRows());
        while (getRangeResult.getNextStartPrimaryKey() != null) {
            getRangeRequest.getRangeRowQueryCriteria().setInclusiveStartPrimaryKey(
                    getRangeResult.getNextStartPrimaryKey());
            getRangeResult = ots.getRange(getRangeRequest);
            rows.addAll(getRangeResult.getRows());
        }

        Map<String, String> checkpointMap = new ConcurrentHashMap<String, String>();
        for (Row row : rows) {
            String pk3 = row.getPrimaryKey().getPrimaryKeyColumn(StatusTableConstants.PK3_STATUS_VALUE).getValue().asString();
            String shardId = pk3.split(StatusTableConstants.TIME_SHARD_SEPARATOR)[1];
            String checkpoint = row.getColumn(StatusTableConstants.CHECKPOINT_COLUMN_NAME).get(0).getValue().asString();
            checkpointMap.put(shardId, checkpoint);
        }
        if (LOG.isInfoEnabled()) {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("GetAllCheckpoints: size: " + checkpointMap.size());
            for (String shardId : checkpointMap.keySet()) {
                stringBuilder.append(", [shardId: ");
                stringBuilder.append(shardId);
                stringBuilder.append(", checkpoint: ");
                stringBuilder.append(checkpointMap.get(shardId));
                stringBuilder.append("]");
            }
            LOG.info(stringBuilder.toString());
        }
        return checkpointMap;
    }

    public void clearShardCountAndAllCheckpoints(long timestamp) {
        PrimaryKey primaryKey = getPrimaryKeyForShardCount(timestamp);
        DeleteRowRequest deleteRowRequest = getOTSRequestForDelete(primaryKey);
        ots.deleteRow(deleteRowRequest);

        GetRangeRequest getRangeRequest = getOTSRequestForGetAllCheckpoints(timestamp);
        GetRangeResult getRangeResult = ots.getRange(getRangeRequest);
        List<Row> rows = new ArrayList<Row>();
        do {
            rows.addAll(getRangeResult.getRows());
        } while (getRangeResult.getNextStartPrimaryKey() != null);

        for (Row row : rows) {
            deleteRowRequest = getOTSRequestForDelete(row.getPrimaryKey());
            ots.deleteRow(deleteRowRequest);
        }

        LOG.info("ClearShardCountAndAllCheckpoints: timestamp: {}.", timestamp);
    }

    private PrimaryKey getPrimaryKeyForCheckpoint(long timestamp, String shardId) {
        String statusValue = String.format("%16d", timestamp) + StatusTableConstants.TIME_SHARD_SEPARATOR + shardId;

        List<PrimaryKeyColumn> pkCols = new ArrayList<PrimaryKeyColumn>();
        pkCols.add(new PrimaryKeyColumn(StatusTableConstants.PK1_STREAM_ID, PrimaryKeyValue.fromString(streamId)));
        pkCols.add(new PrimaryKeyColumn(StatusTableConstants.PK2_STATUS_TYPE, PrimaryKeyValue.fromString(StatusTableConstants.STATUS_TYPE_CHECKPOINT)));
        pkCols.add(new PrimaryKeyColumn(StatusTableConstants.PK3_STATUS_VALUE, PrimaryKeyValue.fromString(statusValue)));

        PrimaryKey primaryKey = new PrimaryKey(pkCols);
        return primaryKey;
    }

    private PrimaryKey getPrimaryKeyForShardCount(long timestamp) {
        String statusValue = String.format("%16d", timestamp);

        List<PrimaryKeyColumn> pkCols = new ArrayList<PrimaryKeyColumn>();
        pkCols.add(new PrimaryKeyColumn(StatusTableConstants.PK1_STREAM_ID, PrimaryKeyValue.fromString(streamId)));
        pkCols.add(new PrimaryKeyColumn(StatusTableConstants.PK2_STATUS_TYPE, PrimaryKeyValue.fromString(StatusTableConstants.STATUS_TYPE_CHECKPOINT)));
        pkCols.add(new PrimaryKeyColumn(StatusTableConstants.PK3_STATUS_VALUE, PrimaryKeyValue.fromString(statusValue)));

        PrimaryKey primaryKey = new PrimaryKey(pkCols);
        return primaryKey;
    }

    private PutRowRequest getOTSRequestForSetCheckpoint(long timestamp, String shardId, String checkpointValue) {
        PrimaryKey primaryKey = getPrimaryKeyForCheckpoint(timestamp, shardId);

        RowPutChange rowPutChange = new RowPutChange(statusTable, primaryKey);
        rowPutChange.addColumn(StatusTableConstants.CHECKPOINT_COLUMN_NAME, ColumnValue.fromString(checkpointValue));

        PutRowRequest putRowRequest = new PutRowRequest(rowPutChange);
        return putRowRequest;
    }

    private PutRowRequest getOTSRequestForSetShardCount(long timestamp, int shardCount) {
        PrimaryKey primaryKey = getPrimaryKeyForShardCount(timestamp);

        RowPutChange rowPutChange = new RowPutChange(statusTable, primaryKey);
        rowPutChange.addColumn(StatusTableConstants.SHARDCOUNT_COLUMN_NAME, ColumnValue.fromLong(shardCount));

        PutRowRequest putRowRequest = new PutRowRequest(rowPutChange);
        return putRowRequest;
    }

    private GetRowRequest getOTSRequestForGet(PrimaryKey primaryKey) {
        SingleRowQueryCriteria rowQueryCriteria = new SingleRowQueryCriteria(statusTable, primaryKey);
        rowQueryCriteria.setMaxVersions(1);

        GetRowRequest getRowRequest = new GetRowRequest(rowQueryCriteria);
        return getRowRequest;
    }

    private GetRangeRequest getOTSRequestForGetAllCheckpoints(long timestamp) {
        PrimaryKey startPk = getPrimaryKeyForCheckpoint(timestamp, "");
        PrimaryKey endPk = getPrimaryKeyForCheckpoint(timestamp, StatusTableConstants.LARGEST_SHARD_ID);
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(statusTable);
        rangeRowQueryCriteria.setMaxVersions(1);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(startPk);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(endPk);
        GetRangeRequest getRangeRequest = new GetRangeRequest(rangeRowQueryCriteria);
        return getRangeRequest;
    }

    private DeleteRowRequest getOTSRequestForDelete(PrimaryKey primaryKey) {
        RowDeleteChange rowDeleteChange = new RowDeleteChange(statusTable, primaryKey);
        DeleteRowRequest deleteRowRequest = new DeleteRowRequest(rowDeleteChange);
        return deleteRowRequest;
    }
}
