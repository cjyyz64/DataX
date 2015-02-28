package com.alibaba.datax.plugin.reader.odpsreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;

import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReaderProxy {
    private static final Logger LOG = LoggerFactory
            .getLogger(ReaderProxy.class);

    private RecordSender recordSender;
    private RecordReader recordReader;
    private Map<String, OdpsType> columnTypeMap;
    private List<Pair<String, ColumnType>> parsedColumns;
    private String partition;
    private boolean isPartitionTable;

    public ReaderProxy(RecordSender recordSender, RecordReader recordReader,
            Map<String, OdpsType> columnTypeMap,
            List<Pair<String, ColumnType>> parsedColumns, String partition,
            boolean isPartitionTable) {
        this.recordSender = recordSender;
        this.recordReader = recordReader;
        this.columnTypeMap = columnTypeMap;
        this.parsedColumns = parsedColumns;
        this.partition = partition;
        this.isPartitionTable = isPartitionTable;
    }

    public void doRead() {
        try {
            Record odpsRecord;
            Map<String, String> partitionMap = this
                    .parseCurrentPartitionValue();
            while ((odpsRecord = recordReader.read()) != null) {
                com.alibaba.datax.common.element.Record dataXRecord = recordSender
                        .createRecord();
                // warn: for PARTITION||NORMAL columnTypeMap's key(columnName)
                // is big than parsedColumns's left(columnName)
                for (Pair<String, ColumnType> pair : this.parsedColumns) {
                    String columnName = pair.getLeft();
                    switch (pair.getRight()) {
                    case PARTITION:
                        this.odpsColumnToDataXField(odpsRecord, dataXRecord,
                                columnTypeMap.get(columnName),
                                partitionMap.get(columnName), true);
                        break;
                    case NORMAL:
                        this.odpsColumnToDataXField(odpsRecord, dataXRecord,
                                columnTypeMap.get(columnName), columnName,
                                false);
                        break;
                    case CONSTANT:
                        dataXRecord.addColumn(new StringColumn(columnName));
                        break;
                    default:
                        break;
                    }
                }
                recordSender.sendToWriter(dataXRecord);
            }
            recordReader.close();
        } catch (Exception e) {
            // warn: if dirty
            throw DataXException.asDataXException(
                    OdpsReaderErrorCode.READ_DATA_FAIL, e);
        }
    }

    private Map<String, String> parseCurrentPartitionValue() {
        Map<String, String> partitionMap = new HashMap<String, String>();
        if (this.isPartitionTable) {
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
        }
        return partitionMap;
    }

    /**
     * TODO warn: odpsRecord 的 String 可能获取出来的是 binary
     * 
     * warn: there is no dirty data in reader plugin, so do not handle dirty
     * data with TaskPluginCollector
     * 
     * warn: odps only support BIGINT && String partition column actually
     * 
     * @param odpsRecord
     *            every line record of odps table
     * @param dataXRecord
     *            every datax record, to be send to writer
     * @param type
     *            odps column type
     * @param columnNameValue
     *            for partition column it's column value, for normal column it's
     *            column name
     * @param isPartitionColumn
     *            true means partition column and false means normal column
     * */
    private void odpsColumnToDataXField(Record odpsRecord,
            com.alibaba.datax.common.element.Record dataXRecord, OdpsType type,
            String columnNameValue, boolean isPartitionColumn) {
        switch (type) {
        case BIGINT: {
            if (isPartitionColumn) {
                dataXRecord.addColumn(new LongColumn(columnNameValue));
            } else {
                dataXRecord.addColumn(new LongColumn(odpsRecord
                        .getBigint(columnNameValue)));
            }
            break;
        }
        case BOOLEAN: {
            if (isPartitionColumn) {
                dataXRecord.addColumn(new BoolColumn(columnNameValue));
            } else {
                dataXRecord.addColumn(new BoolColumn(odpsRecord
                        .getBoolean(columnNameValue)));
            }
            break;
        }
        case DATETIME: {
            if (isPartitionColumn) {
                try {
                    dataXRecord.addColumn(new DateColumn(ColumnCast
                            .string2Date(new StringColumn(columnNameValue))));
                } catch (ParseException e) {
                    LOG.error(String.format("", this.partition));
                    String errMessage = String.format(
                            "您读取分区 [%s] 出现日期转换异常, 日期的字符串表示为 [%s].",
                            this.partition, columnNameValue);
                    LOG.error(errMessage);
                    throw DataXException.asDataXException(
                            OdpsReaderErrorCode.READ_DATA_FAIL, errMessage, e);
                }
            } else {
                dataXRecord.addColumn(new DateColumn(odpsRecord
                        .getDatetime(columnNameValue)));
            }

            break;
        }
        case DOUBLE: {
            if (isPartitionColumn) {
                dataXRecord.addColumn(new DoubleColumn(columnNameValue));
            } else {
                dataXRecord.addColumn(new DoubleColumn(odpsRecord
                        .getDouble(columnNameValue)));
            }
            break;
        }
        case STRING: {
            if (isPartitionColumn) {
                dataXRecord.addColumn(new StringColumn(columnNameValue));
            } else {
                dataXRecord.addColumn(new StringColumn(odpsRecord
                        .getString(columnNameValue)));
            }
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
