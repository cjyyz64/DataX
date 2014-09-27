package com.alibaba.datax.plugin.writer.odpswriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.SlavePluginCollector;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class WriterProxy {
    private static final Logger LOG = LoggerFactory
            .getLogger(WriterProxy.class);

    private RecordReceiver recordReceiver;
    private UploadSession uploadSession;

    private List<Integer> columnPositions;
    private long blockId;
    private TableSchema tableSchema;
    private SlavePluginCollector slavePluginCollector;

    public WriterProxy(RecordReceiver recordReceiver,
                       UploadSession uploadSession, List<Integer> columnPositions, long blockId,
                       SlavePluginCollector slavePluginCollector) {
        this.recordReceiver = recordReceiver;
        this.uploadSession = uploadSession;
        this.columnPositions = columnPositions;
        this.blockId = blockId;

        this.tableSchema = uploadSession.getSchema();
        this.slavePluginCollector = slavePluginCollector;
    }

    public void doWrite() throws Exception {
        try {
            RecordWriter recordWriter = this.uploadSession.openRecordWriter(this.blockId);

            int currentIndex;
            Record odpsRecord;
            com.alibaba.datax.common.element.Record dataXRecord;
            while ((dataXRecord = this.recordReceiver.getFromReader()) != null) {
                odpsRecord = this.uploadSession.newRecord();

                try {
                    for (int i = 0, len = columnPositions.size(); i < len; i++) {
                        currentIndex = columnPositions.get(i);
                        OdpsType type = tableSchema.getColumn(currentIndex).getType();
                        switch (type) {
                            case STRING:
                                odpsRecord.setString(currentIndex, dataXRecord.getColumn(currentIndex)
                                        .toString());
                                break;
                            case BIGINT:
                                odpsRecord.setBigint(currentIndex, dataXRecord.getColumn(i)
                                        .asLong());
                                break;
                            case BOOLEAN:
                                odpsRecord.setBoolean(currentIndex, dataXRecord.getColumn(i)
                                        .asBoolean());
                                break;
                            case DATETIME:
                                odpsRecord.setDatetime(currentIndex, dataXRecord.getColumn(i)
                                        .asDate());
                                break;
                            case DOUBLE:
                                odpsRecord.setDouble(currentIndex, dataXRecord.getColumn(i)
                                        .asDouble());
                                break;
                            default:
                                throw new DataXException(OdpsWriterErrorCode.UNSUPPORTED_COLUMN_TYPE,
                                        String.format("Unsupported column type:[%s].", type));
                        }
                    }
                    recordWriter.write(odpsRecord);
                } catch (Exception e) {
                    slavePluginCollector.collectDirtyRecord(dataXRecord, e);
                }
            }
            recordWriter.close();

        } catch (Exception e) {
            throw e;
        }
    }

}
