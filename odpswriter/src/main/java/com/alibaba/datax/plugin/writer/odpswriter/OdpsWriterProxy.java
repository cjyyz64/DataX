package com.alibaba.datax.plugin.writer.odpswriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.StrUtil;
import com.alibaba.datax.plugin.writer.odpswriter.util.OdpsUtil;
import com.alibaba.odps.tunnel.Column;
import com.alibaba.odps.tunnel.RecordSchema;
import com.alibaba.odps.tunnel.Upload;
import com.alibaba.odps.tunnel.io.ProtobufRecordWriter;
import com.alibaba.odps.tunnel.io.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

public class OdpsWriterProxy {
    private static final Logger LOG = LoggerFactory
            .getLogger(OdpsWriterProxy.class);

    private static final boolean IS_DEBUG = LOG.isDebugEnabled();

    private volatile boolean printColumnLess = true;// 是否打印对于源头字段数小于odps目的表的行的日志
    private volatile boolean is_compatible = true;// TODO tunnelConfig or
    // delete it


    private Upload slaveUpload;
    private com.alibaba.datax.common.element.Record dataxRecord;
    private RecordSchema schema;
    private ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(
            70 * 1024 * 1024);
    private int max_buffer_length = 64 * 1024 * 1024;
    private ProtobufRecordWriter protobufRecordWriter = null;
    private long blockId;
    private int intervalStep;
    private List<Integer> columnPositions;
    private List<Column.Type> tableOriginalColumnTypeList;

    public OdpsWriterProxy(Upload slaveUpload, long blockId, int intervalStep, List<Integer> columnPositions)
            throws IOException {
        this.slaveUpload = slaveUpload;
        this.schema = this.slaveUpload.getSchema();
        this.tableOriginalColumnTypeList = OdpsUtil
                .getTableOriginalColumnTypeList(this.schema);

        this.protobufRecordWriter = new ProtobufRecordWriter(schema, byteArrayOutputStream);
        this.blockId = blockId;
        this.intervalStep = intervalStep;
        this.columnPositions = columnPositions;

    }

    public void writeOneRecord(com.alibaba.datax.common.element.Record dataxRecord, List<Long> blocks)
            throws Exception {
        Record r = dataxRecordToOdpsRecord(dataxRecord, schema);
        if (null != r) {
            protobufRecordWriter.write(r);
        }

        if (byteArrayOutputStream.size() >= max_buffer_length) {
            protobufRecordWriter.close();
            OdpsUtil.slaveWriteOneBlock(this.slaveUpload, this.byteArrayOutputStream, blockId);
            LOG.info("write block {} ok.", blockId);

            blocks.add(blockId);
            byteArrayOutputStream.reset();
            protobufRecordWriter = new ProtobufRecordWriter(schema, byteArrayOutputStream);

            blockId += this.intervalStep;
        }
    }

    public void writeRemainingRecord(List<Long> blocks) throws Exception {
        // complete protobuf stream, then write to http
        protobufRecordWriter.close();
        if (byteArrayOutputStream.size() != 0) {
            OdpsUtil.slaveWriteOneBlock(this.slaveUpload, this.byteArrayOutputStream, blockId);
            LOG.info("write block {} ok.", blockId);

            blocks.add(blockId);
            // reset the buffer for next block
            byteArrayOutputStream.reset();
        }
    }

    public Record dataxRecordToOdpsRecord(com.alibaba.datax.common.element.Record dataXRecord,
                                          RecordSchema schema) throws Exception {
        int sourceColumnCount = dataXRecord.getColumnNumber();
        int destColumnCount = schema.getColumnCount();
        Record odpsRecord = new Record(destColumnCount);

        int userConfiguredColumnNumber = this.columnPositions.size();

        if (sourceColumnCount > userConfiguredColumnNumber) {
            String businessMessage = String.format("source columnNumber=[%s] bigger than configured destination columnNumber=[%s].",
                    sourceColumnCount, userConfiguredColumnNumber);
            String message = StrUtil.buildOriginalCauseMessage(
                    businessMessage, null);
            LOG.error(message);

            throw new DataXException(OdpsWriterErrorCode.COLUMN_NUMBER_ERROR, businessMessage);
        } else if (sourceColumnCount < userConfiguredColumnNumber) {
            if (printColumnLess) {
                printColumnLess = false;
                LOG.warn("source columnNumber={} is less than configured destination columnNumber={}, DataX will fill some column with null.",
                        dataXRecord.getColumnNumber(), userConfiguredColumnNumber);
            }
        }

        int currentIndex = -1;
        for (int i = 0, len = sourceColumnCount; i < len; i++) {
            currentIndex = columnPositions.get(i);
            Column.Type type = this.tableOriginalColumnTypeList.get(currentIndex);
            switch (type) {
                case ODPS_STRING:
                    odpsRecord.setString(currentIndex, dataXRecord.getColumn(i)
                            .asString());
                    break;
                case ODPS_BIGINT:
                    odpsRecord.setBigint(currentIndex, dataXRecord.getColumn(i)
                            .asLong());
                    break;
                case ODPS_BOOLEAN:
                    odpsRecord.setBoolean(currentIndex, dataXRecord.getColumn(i)
                            .asBoolean());
                    break;
                case ODPS_DATETIME:
                    odpsRecord.setDatetime(currentIndex, dataXRecord.getColumn(i)
                            .asDate());
                    break;
                case ODPS_DOUBLE:
                    odpsRecord.setDouble(currentIndex, dataXRecord.getColumn(i)
                            .asDouble());
                    break;
                default:
                    String businessMessage = String.format("Unsupported column type:[%s].", type);
                    String message = StrUtil.buildOriginalCauseMessage(
                            businessMessage, null);

                    LOG.error(message);
                    throw new DataXException(OdpsWriterErrorCode.UNSUPPORTED_COLUMN_TYPE,
                            businessMessage);
            }
        }

        //对只写入不分列的情况，需要对不存在的列补空
        String nullString = null;
        for (int i = 0; i < destColumnCount; i++) {
            if (null == odpsRecord.get(i)) {
                odpsRecord.setString(i, nullString);
            }
        }

        return odpsRecord;
    }
}
