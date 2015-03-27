package com.alibaba.datax.plugin.writer.hbasebulkwriter.column;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.writer.hbasebulkwriter.BulkWriterError;
import com.alibaba.datax.plugin.writer.hbasebulkwriter.column.HBaseColumn.HBaseDataType;
import org.apache.hadoop.hbase.util.Bytes;

public class DataTypeConvertor {
  public static byte[] toBoolBytes(BoolColumn dataxBool, HBaseDataType hbaseDataType) {
    if(dataxBool.getRawData() == null) {
      throw DataXException.asDataXException(BulkWriterError.RUNTIME_CAST, "input BoolColumn is null");
    }
    if(hbaseDataType == HBaseDataType.BOOLEAN) {
      return Bytes.toBytes(dataxBool.asBoolean());
    } else {
      throw DataXException.asDataXException(BulkWriterError.RUNTIME_CAST, String.format("can not convert datax BoolColumn to hbase %s" , hbaseDataType));
    }
  }
  
  public static byte[] toRawBytes(BytesColumn dataxBytes, HBaseDataType hbaseDataType) {
    if(dataxBytes.getRawData() == null) {
      throw DataXException.asDataXException(BulkWriterError.RUNTIME_CAST, "input BytesColumn is null");
    }
    if(hbaseDataType == HBaseDataType.BYTES) {
      return dataxBytes.asBytes();
    } else {
      throw DataXException.asDataXException(BulkWriterError.RUNTIME_CAST, String.format("can not convert datax Bytes to hbase %s" , hbaseDataType));
    }
  }
  
  public static byte[] toDateBytes(DateColumn dataxDate, HBaseDataType hbaseDataType) {
    if(dataxDate.getRawData() == null) {
      throw DataXException.asDataXException(BulkWriterError.RUNTIME_CAST, "input DateColumn is null");
    }
    if(hbaseDataType == HBaseDataType.LONG) {
      return Bytes.toBytes(dataxDate.asLong());
    } else {
      throw DataXException.asDataXException(BulkWriterError.RUNTIME_CAST, String.format("can not convert datax Date to hbase %s" , hbaseDataType));
    }
  }
  
  public static byte[] toDoubleOrFloatBytes(DoubleColumn dataxDouble, HBaseDataType hbaseDataType) {
    if(dataxDouble.getRawData() == null) {
      throw DataXException.asDataXException(BulkWriterError.RUNTIME_CAST, "input DoubleColumn is null");
    }
    if(hbaseDataType == HBaseDataType.DOUBLE) {
      return Bytes.toBytes(dataxDouble.asDouble());
    } else if(hbaseDataType == HBaseDataType.FLOAT) {
      return Bytes.toBytes(dataxDouble.asDouble().floatValue());
    } else {
      throw DataXException.asDataXException(BulkWriterError.RUNTIME_CAST, String.format("can not convert datax Double to hbase %s" , hbaseDataType));
    }  
  }
  
  public static byte[] toLongOrIntBytes(LongColumn dataxLong, HBaseDataType hbaseDataType) {
    if(dataxLong.getRawData() == null) {
      throw DataXException.asDataXException(BulkWriterError.RUNTIME_CAST, "input LongColumn is null");
    }
    if(hbaseDataType == HBaseDataType.LONG) {
      return Bytes.toBytes(dataxLong.asLong());
    } else if(hbaseDataType == HBaseDataType.INT) {
      return Bytes.toBytes(dataxLong.asLong().intValue());
    } else {
      throw DataXException.asDataXException(BulkWriterError.RUNTIME_CAST, String.format("can not convert datax Long to hbase %s" , hbaseDataType));
    }
  }
  
  public static byte[] toStringBytes(StringColumn dataxString, HBaseDataType hbaseDataType, String encoding) {
    if(dataxString.getRawData() == null) {
      throw DataXException.asDataXException(BulkWriterError.RUNTIME_CAST, "input StringColumn is null");
    }
    return HBaseColumn.convertStrToBytes(dataxString.asString(), hbaseDataType, encoding);
  }
  
//  public static byte[] toNullBytes(NullColumn dataxDouble, HBaseDataType hbaseDataType) {
//    throw DataXException.asDataXException(BulkWriterError.RUNTIME_CAST, String.format("can not convert datax NULL to hbase %s" , hbaseDataType));
//  }
  
  public static byte[] toBytes(Column dataxColumn, HBaseDataType hbaseDataType) {
    return toBytes(dataxColumn, hbaseDataType, "utf-8");
  }
  
  public static byte[] toBytes(Column dataxColumn, HBaseDataType hbaseDataType, String encoding) {
    if(dataxColumn instanceof BoolColumn) {
      return toBoolBytes((BoolColumn)dataxColumn, hbaseDataType);
    } else if (dataxColumn instanceof BytesColumn) {
      return toRawBytes((BytesColumn)dataxColumn, hbaseDataType);
    } else if (dataxColumn instanceof DateColumn) {
      return toDateBytes((DateColumn)dataxColumn, hbaseDataType);
    } else if (dataxColumn instanceof DoubleColumn) {
      return toDoubleOrFloatBytes((DoubleColumn)dataxColumn, hbaseDataType);
    } else if (dataxColumn instanceof LongColumn) {
      return toLongOrIntBytes((LongColumn)dataxColumn, hbaseDataType);
    } else if (dataxColumn instanceof StringColumn) {
      return toStringBytes((StringColumn)dataxColumn, hbaseDataType,encoding);
//    } else if (dataxColumn instanceof NullColumn) {
//      return toNullBytes((NullColumn)dataxColumn, hbaseDataType);
    } else {
      throw DataXException.asDataXException(BulkWriterError.RUNTIME_CAST, String.format("unsupported datax data type" , dataxColumn.getClass().getName()));
    }
  }
}
