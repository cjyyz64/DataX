package com.alibaba.datax.plugin.reader.odpsreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.util.Configuration;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;

import java.util.List;

public class ReaderProxy {
    private RecordSender recordSender;
    private Configuration sliceConfig;
    private RecordReader recordReader;
    private TableSchema tableSchema;
    private List<OdpsType> tableOriginalColumnTypeList;

    public ReaderProxy(RecordSender recordSender, RecordReader recordReader,
                       TableSchema tableSchema, Configuration sliceConfig,
                       List<OdpsType> tableOriginalColumnTypeList) {
        this.recordSender = recordSender;
        this.recordReader = recordReader;
        this.recordReader = recordReader;
        this.tableSchema = tableSchema;
        this.sliceConfig = sliceConfig;
        this.tableOriginalColumnTypeList = tableOriginalColumnTypeList;
    }

    public void doRead() {
        List<String> allColumnParsedWithConstant = this.sliceConfig.getList(
                Constant.ALL_COLUMN_PARSED_WITH_CONSTANT, String.class);

        List<Integer> columnPositions = this.sliceConfig.getList(
                Constant.COLUMN_POSITION, Integer.class);

        try {
            Record odpsRecord;
            String constantColumn = null;
            int originalColumnSize = this.tableSchema.getColumns().size();
            while ((odpsRecord = recordReader.read()) != null) {
                com.alibaba.datax.common.element.Record dataXRecord = recordSender
                        .createRecord();

                for (int i : columnPositions) {
                    if (i >= originalColumnSize) {
                        constantColumn = allColumnParsedWithConstant.get(i);
                        dataXRecord.addColumn(new StringColumn(constantColumn));
                    } else {
                        odpsColumnToDataXField(odpsRecord, dataXRecord,
                                this.tableOriginalColumnTypeList, i);
                    }
                }

                recordSender.sendToWriter(dataXRecord);
            }
            recordReader.close();
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsReaderErrorCode.READ_DATA_FAIL, e);
        }
    }

    //TODO  odpsRecord 的 String 可能获取出来的是 binary
    private void odpsColumnToDataXField(Record odpsRecord,
                                        com.alibaba.datax.common.element.Record dataXRecord,
                                        List<OdpsType> tableOriginalColumnTypeList, int i) {
        OdpsType type = tableOriginalColumnTypeList.get(i);
        switch (type) {
            case BIGINT: {
                dataXRecord.addColumn(new LongColumn(odpsRecord.getBigint(i)));
                break;
            }
            case BOOLEAN: {
                dataXRecord.addColumn(new BoolColumn(odpsRecord.getBoolean(i)));
                break;
            }
            case DATETIME: {
                dataXRecord.addColumn(new DateColumn(odpsRecord.getDatetime(i)));
                break;
            }
            case DOUBLE: {
                dataXRecord.addColumn(new DoubleColumn(odpsRecord.getDouble(i)));
                break;
            }
            case STRING: {
                dataXRecord.addColumn(new StringColumn(odpsRecord.getString(i)));
                break;
            }
            default:
                throw DataXException.asDataXException(OdpsReaderErrorCode.ILLEGAL_VALUE,
                        String.format("DataX 抽取 ODPS 数据不支持字段类型为:[%s]. 目前支持抽取的字段类型有：bigint, boolean, datetime, double, string. " +
                                        "您可以选择不抽取 DataX 不支持的字段或者联系 ODPS 管理员寻求帮助.",
                                type));
        }
    }

}
