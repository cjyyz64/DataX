package com.alibaba.datax.plugin.reader.odpsreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

public class ReaderProxy {
    private RecordSender recordSender;
    private RecordReader recordReader;
    private Map<String, OdpsType> columnTypeMap;
    private List<Pair<String, ColumnType>> parsedColumns;
    private String partition;

    public ReaderProxy(RecordSender recordSender, RecordReader recordReader,
            Map<String, OdpsType> columnTypeMap,
            List<Pair<String, ColumnType>> parsedColumns, String partition) {
        this.recordSender = recordSender;
        this.recordReader = recordReader;
        this.columnTypeMap = columnTypeMap;
        this.parsedColumns = parsedColumns;
        this.partition = partition;
    }

    public void doRead() {
        try {
            Record odpsRecord;
            Map<String, String> partitionMap = new HashMap<String, String>();
            String[] splitedPartition = this.partition.split(",");
            for (String eachPartition : splitedPartition) {
                String[] partitionDetail = eachPartition.split("=");
                // warn: check partition like partition=1
                if (2 != partitionDetail.length) {
                    throw DataXException
                            .asDataXException(
                                    OdpsReaderErrorCode.ILLEGAL_VALUE,
                                    String.format(
                                            "您的分区 [%s] 解析出现错误,解析后正确的配置方式类似为 [ pt=1,dt=1 ].",
                                            eachPartition));
                }
                partitionMap.put(partitionDetail[0], partitionDetail[1]);
            }
            while ((odpsRecord = recordReader.read()) != null) {
                com.alibaba.datax.common.element.Record dataXRecord = recordSender
                        .createRecord();
                // warn: for PARTITION||NORMAL columnTypeMap's key(columnName)
                // is big than parsedColumns's left(columnName)
                for (Pair<String, ColumnType> pair : this.parsedColumns) {
                    switch (pair.getRight()) {
                    case PARTITION:
                    case NORMAL:
                        this.odpsColumnToDataXField(odpsRecord, dataXRecord,
                                columnTypeMap.get(pair.getLeft()),
                                pair.getLeft());
                        break;
                    case CONSTANT:
                        dataXRecord.addColumn(new StringColumn(pair.getLeft()));
                        break;
                    default:
                        break;
                    }
                }
                recordSender.sendToWriter(dataXRecord);
            }
            recordReader.close();
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    OdpsReaderErrorCode.READ_DATA_FAIL, e);
        }
    }

    // TODO warn: odpsRecord 的 String 可能获取出来的是 binary
    private void odpsColumnToDataXField(Record odpsRecord,
            com.alibaba.datax.common.element.Record dataXRecord, OdpsType type,
            String columnName) {
        switch (type) {
        case BIGINT: {
            dataXRecord.addColumn(new LongColumn(odpsRecord
                    .getBigint(columnName)));
            break;
        }
        case BOOLEAN: {
            dataXRecord.addColumn(new BoolColumn(odpsRecord
                    .getBoolean(columnName)));
            break;
        }
        case DATETIME: {
            dataXRecord.addColumn(new DateColumn(odpsRecord
                    .getDatetime(columnName)));
            break;
        }
        case DOUBLE: {
            dataXRecord.addColumn(new DoubleColumn(odpsRecord
                    .getDouble(columnName)));
            break;
        }
        case STRING: {
            dataXRecord.addColumn(new StringColumn(odpsRecord
                    .getString(columnName)));
            break;
        }
        default:
            throw DataXException
                    .asDataXException(
                            OdpsReaderErrorCode.ILLEGAL_VALUE,
                            String.format(
                                    "DataX 抽取 ODPS 数据不支持字段类型为:[%s]. 目前支持抽取的字段类型有：bigint, boolean, datetime, double, string. "
                                            + "您可以选择不抽取 DataX 不支持的字段或者联系 ODPS 管理员寻求帮助.",
                                    type));
        }
    }

}
