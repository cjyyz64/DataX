package com.alibaba.datax.plugin.writer.odpswriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.plugin.writer.odpswriter.util.OdpsUtil;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;

import java.util.List;

public class WriterProxy {
    private RecordReceiver recordReceiver;

    private TableSchema tableSchema;
    private List<OdpsType> tableOriginalColumnTypeList;
    private UploadSession uploadSession;
    List<Integer> positions;
    private long blockId;

    public WriterProxy(RecordReceiver recordReceiver,
                       UploadSession uploadSession, List<Integer> positions, long blockId) {
        this.recordReceiver = recordReceiver;
        this.uploadSession = uploadSession;
        this.positions = positions;
        this.blockId = blockId;

        this.tableSchema = uploadSession.getSchema();
        this.tableOriginalColumnTypeList = OdpsUtil.getTableOriginalColumnTypeList(this.tableSchema.getColumns());
    }

    public void doWrite() {
        try {
            Record odpsRecord;
            com.alibaba.datax.common.element.Record dataXRecord;
            RecordWriter recordWriter = this.uploadSession.openRecordWriter(this.blockId);

            int currentIndex;
            while ((dataXRecord = this.recordReceiver.getFromReader()) != null) {
                odpsRecord = this.uploadSession.newRecord();
                this.uploadSession.newRecord();

                for (int i = 0, len = positions.size(); i < len; i++) {
                    currentIndex = positions.get(i);
                    OdpsType type = tableSchema.getColumn(currentIndex).getType();
                    switch (type) {
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
                        case STRING:
                            odpsRecord.setString(currentIndex, dataXRecord.getColumn(currentIndex)
                                    .toString());
                            break;
                        default:
                            throw new DataXException(OdpsWriterErrorCode.NOT_SUPPORT_TYPE,
                                    String.format("Unknown column type:[%s].", type));

                    }
                }
                recordWriter.write(odpsRecord);
            }
            recordWriter.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
