package com.alibaba.datax.plugin.writer.odpswriter;

import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.plugin.writer.odpswriter.util.OdpsUtil;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;

import com.aliyun.odps.data.Record;

import com.aliyun.odps.tunnel.TableTunnel;

import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.ProtobufRecordPack;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class OdpsWriterProxy {
    private static final Logger LOG = LoggerFactory
            .getLogger(OdpsWriterProxy.class);

    private volatile boolean printColumnLess;// 是否打印对于源头字段数小于 ODPS 目的表的行的日志

    private TaskPluginCollector taskPluginCollector;

    private TableTunnel.UploadSession slaveUpload;
    private TableSchema schema;
    private long recordPackByteSize = 0;
    private int max_buffer_length;
    private ProtobufRecordPack protobufRecordPack;
    private AtomicLong blockId;

    private List<Integer> columnPositions;
    private List<OdpsType> tableOriginalColumnTypeList;
    private boolean emptyAsNull;

    public OdpsWriterProxy(TableTunnel.UploadSession slaveUpload, int blockSizeInMB,
                           AtomicLong blockId, List<Integer> columnPositions,
                           TaskPluginCollector taskPluginCollector, boolean emptyAsNull)
            throws IOException, TunnelException {
        this.slaveUpload = slaveUpload;
        this.schema = this.slaveUpload.getSchema();
        this.tableOriginalColumnTypeList = OdpsUtil
                .getTableOriginalColumnTypeList(this.schema);

        this.protobufRecordPack = new ProtobufRecordPack(this.schema);
        this.blockId = blockId;
        this.columnPositions = columnPositions;
        this.taskPluginCollector = taskPluginCollector;
        this.emptyAsNull = emptyAsNull;

        // 初始化与 buffer 区相关的值
        this.max_buffer_length = blockSizeInMB * 1024 * 1024;
        printColumnLess = true;

        //todo:怎么初始化
        //this.tunnelRecordWriter = (TunnelRecordWriter)slaveUpload.openRecordWriter(blockId.longValue());
    }

    public void writeOneRecord(
            com.alibaba.datax.common.element.Record dataXRecord,
            List<Long> blocks) throws Exception {

        Pair<Record, Integer> recordPair = dataxRecordToOdpsRecord(dataXRecord, schema);

        if(recordPair == null) {
            return;
        }
        Record record = recordPair.getLeft();
        int recordByteSize = recordPair.getRight();
        if (null == record) {
            return;
        }

        protobufRecordPack.append(record);
        recordPackByteSize = recordPackByteSize + recordByteSize;

        if (recordPackByteSize >= max_buffer_length) {

            OdpsUtil.slaveWriteOneBlock(this.slaveUpload,
                    protobufRecordPack, blockId.get());
            LOG.info("write block {} ok.", blockId.get());

            blocks.add(blockId.get());
            recordPackByteSize = 0;
            protobufRecordPack = new ProtobufRecordPack(this.schema);

            this.blockId.incrementAndGet();
        }
    }

    public void writeRemainingRecord(List<Long> blocks) throws Exception {
        // complete protobuf stream, then write to http
        if (recordPackByteSize != 0) {
            OdpsUtil.slaveWriteOneBlock(this.slaveUpload,
                    protobufRecordPack, blockId.get());
            LOG.info("write block {} ok.", blockId.get());

            blocks.add(blockId.get());
            // reset the buffer for next block
            recordPackByteSize = 0;
            protobufRecordPack = new ProtobufRecordPack(this.schema);
        }
    }

    public Pair<Record,Integer> dataxRecordToOdpsRecord(
            com.alibaba.datax.common.element.Record dataXRecord,
            TableSchema schema) throws Exception {
        int sourceColumnCount = dataXRecord.getColumnNumber();
        Record odpsRecord = slaveUpload.newRecord();

        int userConfiguredColumnNumber = this.columnPositions.size();
//todo
        if (sourceColumnCount > userConfiguredColumnNumber) {
            throw DataXException
                    .asDataXException(
                            OdpsWriterErrorCode.ILLEGAL_VALUE,
                            String.format(
                                    "亲，配置中的源表的列个数和目的端表不一致，源表中您配置的列数是:%s 大于目的端的列数是:%s , 这样会导致源头数据无法正确导入目的端, 请检查您的配置并修改.",
                                    sourceColumnCount,
                                    userConfiguredColumnNumber));
        } else if (sourceColumnCount < userConfiguredColumnNumber) {
            if (printColumnLess) {
                LOG.warn(
                        "源表的列个数小于目的表的列个数，源表列数是:{} 目的表列数是:{} , 数目不匹配. DataX 会把目的端多出的咧的值设置为空值. 如果这个默认配置不符合您的期望，请保持源表和目的表配置的列数目保持一致.",
                        sourceColumnCount, userConfiguredColumnNumber);
            }
            printColumnLess = false;
        }

        int currentIndex;
        int sourceIndex = 0;

        int recordByteSize = 0;
        try {
            com.alibaba.datax.common.element.Column columnValue;

            for (; sourceIndex < sourceColumnCount; sourceIndex++) {
                currentIndex = columnPositions.get(sourceIndex);
                OdpsType type = this.tableOriginalColumnTypeList
                        .get(currentIndex);
                columnValue = dataXRecord.getColumn(sourceIndex);

                if (columnValue == null) {
                    continue;
                }
                // for compatible dt lib, "" as null
                if(this.emptyAsNull && columnValue instanceof StringColumn && "".equals(columnValue.asString())){
                    continue;
                }

                switch (type) {
                    case STRING:
                        odpsRecord.setString(currentIndex, columnValue.asString());
                        recordByteSize = recordByteSize + columnValue.getByteSize();
                        break;
                    case BIGINT:
                        odpsRecord.setBigint(currentIndex, columnValue.asLong());
                        recordByteSize = recordByteSize + columnValue.getByteSize();
                        break;
                    case BOOLEAN:
                        odpsRecord.setBoolean(currentIndex, columnValue.asBoolean());
                        recordByteSize = recordByteSize + columnValue.getByteSize();
                        break;
                    case DATETIME:
                        odpsRecord.setDatetime(currentIndex, columnValue.asDate());
                        recordByteSize = recordByteSize + columnValue.getByteSize();
                        break;
                    case DOUBLE:
                        odpsRecord.setDouble(currentIndex, columnValue.asDouble());
                        recordByteSize = recordByteSize + columnValue.getByteSize();
                        break;
                    default:
                        break;
                }
            }

            return new ImmutablePair<Record, Integer>(odpsRecord, recordByteSize);
        } catch (Exception e) {
            String message = String.format(
                    "写入 ODPS 目的表时遇到了脏数据, 因为源端第[%s]个字段, 具体值[%s] 的数据不符合 ODPS 对应字段的格式要求，请检查该数据并作出修改 或者您可以增大阀值，忽略这条记录.", sourceIndex,
                    dataXRecord.getColumn(sourceIndex));
            this.taskPluginCollector.collectDirtyRecord(dataXRecord, e,
                    message);

            return null;
        }

    }
}
