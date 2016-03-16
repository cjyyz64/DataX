package com.alibaba.datax.plugin.writer.hbase11xwriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

public class NormalTask extends HbaseAbstractTask {
    public NormalTask(Configuration configuration) {
        super(configuration);
    }

    @Override
    public Put convertRecordToPut(Record record){
        byte[] rowkey = getRowkey(record);
        Put put = null;
        if(this.versionColumn == null){
            put = new Put(rowkey);
            if(!super.walFlag){
                //等价与0.94 put.setWriteToWAL(super.walFlag);
                put.setDurability(Durability.SKIP_WAL);
            }
        }else {
            long timestamp = getVersion(record);
            put = new Put(rowkey,timestamp);
        }
        for (Configuration aColumn : columns) {
            Integer index = aColumn.getInt(Key.INDEX);
            String type = aColumn.getString(Key.TYPE);
            ColumnType columnType = ColumnType.getByTypeName(type);
            String name = aColumn.getString(Key.NAME);
            String promptInfo = "Hbasewriter 中，column 的列配置格式应该是：列族:列名. 您配置的列错误：" + name;
            String[] cfAndQualifier = name.split(":");
            Validate.isTrue(cfAndQualifier != null && cfAndQualifier.length == 2
                    && StringUtils.isNotBlank(cfAndQualifier[0])
                    && StringUtils.isNotBlank(cfAndQualifier[1]), promptInfo);
            if(index >= record.getColumnNumber()){
                throw DataXException.asDataXException(Hbase11xWriterErrorCode.ILLEGAL_VALUE, String.format("您的column配置项中中index值超出范围,根据reader端配置,index的值小于%s,而您配置的值为%s，请检查并修改.",record.getColumnNumber(),index));
            }
            byte[] columnBytes = getColumnByte(columnType,record.getColumn(index));
            //columnBytes 为null忽略这列
            if(null != columnBytes){
                put.addColumn(Bytes.toBytes(
                        cfAndQualifier[0]),
                        Bytes.toBytes(cfAndQualifier[1]),
                        columnBytes);
            }else{
                continue;
            }
        }
        return put;
    }

    public byte[] getRowkey(Record record){
        byte[] rowkeyBuffer  = {};
        for (Configuration aRowkeyColumn : rowkeyColumn) {
            Integer index = aRowkeyColumn.getInt(Key.INDEX);
            String type = aRowkeyColumn.getString(Key.TYPE);
            ColumnType columnType = ColumnType.getByTypeName(type);
            if(index == -1){
                String value = aRowkeyColumn.getString(Key.VALUE);
                rowkeyBuffer = Bytes.add(rowkeyBuffer,getValueByte(columnType,value));
            }else{
                if(index >= record.getColumnNumber()){
                    throw DataXException.asDataXException(Hbase11xWriterErrorCode.CONSTRUCT_ROWKEY_ERROR, String.format("您的rowkeyColumn配置项中中index值超出范围,根据reader端配置,index的值小于%s,而您配置的值为%s，请检查并修改.",record.getColumnNumber(),index));
                }
                byte[] value = getColumnByte(columnType,record.getColumn(index));
                rowkeyBuffer = Bytes.add(rowkeyBuffer, value);
            }
        }
        return rowkeyBuffer;
    }

    public long getVersion(Record record){
        int index = versionColumn.getInt(Key.INDEX);
        String format = versionColumn.getString(Key.FORMAT,Constant.DEFAULT_DATA_FORMAT);
        if(index == -1){
            String value = versionColumn.getString(Key.VALUE);
            if(StringUtils.isBlank(value)){
                throw DataXException.asDataXException(Hbase11xWriterErrorCode.CONSTRUCT_VERSION_ERROR, "您指定的版本为空!");
            }
            long timestamp = 0;
            try {
                timestamp = DateUtils.parseDate(value, new String[]{format}).getTime();
            } catch (Exception e) {
                throw DataXException.asDataXException(Hbase11xWriterErrorCode.CONSTRUCT_VERSION_ERROR, e);
            }
            return timestamp ;
        }else{
            if(index >= record.getColumnNumber()){
                throw DataXException.asDataXException(Hbase11xWriterErrorCode.CONSTRUCT_VERSION_ERROR, String.format("您的versionColumn配置项中中index值超出范围,根据reader端配置,index的值小于%s,而您配置的值为%s，请检查并修改.",record.getColumnNumber(),index));
            }
            if(record.getColumn(index).getRawData()  == null){
                throw DataXException.asDataXException(Hbase11xWriterErrorCode.CONSTRUCT_VERSION_ERROR, "您指定的版本为空!");
            }
            return record.getColumn(index).asLong();
        }
    }
}