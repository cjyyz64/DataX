package com.alibaba.datax.plugin.writer.hbasebulkwriter2.column;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.writer.hbasebulkwriter2.BulkWriterError;
import com.alibaba.datax.plugin.writer.hbasebulkwriter2.column.HBaseColumn.HBaseDataType;
import com.alibaba.datax.plugin.writer.hbasebulkwriter2.util.PhoenixEncoder;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.util.Bytes;

import java.sql.SQLException;

public class DataTypeConvertor {
    public static byte[] toBoolBytes(BoolColumn dataxBool, HBaseDataType hbaseDataType) {
        if (dataxBool.getRawData() == null) {
            throw DataXException.asDataXException(BulkWriterError.RUNTIME_CAST, "input BoolColumn is null");
        }
        if (hbaseDataType == HBaseDataType.BOOLEAN) {
            return Bytes.toBytes(dataxBool.asBoolean());
        } else {
            throw DataXException.asDataXException(BulkWriterError.RUNTIME_CAST, String.format("can not convert datax BoolColumn to hbase %s", hbaseDataType));
        }
    }

    public static byte[] toRawBytes(BytesColumn dataxBytes, HBaseDataType hbaseDataType) {
        if (dataxBytes.getRawData() == null) {
            throw DataXException.asDataXException(BulkWriterError.RUNTIME_CAST, "input BytesColumn is null");
        }
        if (hbaseDataType == HBaseDataType.BYTES) {
            return dataxBytes.asBytes();
        } else {
            throw DataXException.asDataXException(BulkWriterError.RUNTIME_CAST, String.format("can not convert datax Bytes to hbase %s", hbaseDataType));
        }
    }

    public static byte[] toDateBytes(DateColumn dataxDate, HBaseDataType hbaseDataType) {
        if (dataxDate.getRawData() == null) {
            throw DataXException.asDataXException(BulkWriterError.RUNTIME_CAST, "input DateColumn is null");
        }
        if (hbaseDataType == HBaseDataType.LONG) {
            return Bytes.toBytes(dataxDate.asLong());
        } else {
            throw DataXException.asDataXException(BulkWriterError.RUNTIME_CAST, String.format("can not convert datax Date to hbase %s", hbaseDataType));
        }
    }

    public static byte[] toDoubleOrFloatBytes(DoubleColumn dataxDouble, HBaseDataType hbaseDataType) {
        if (dataxDouble.getRawData() == null) {
            throw DataXException.asDataXException(BulkWriterError.RUNTIME_CAST, "input DoubleColumn is null");
        }
        if (hbaseDataType == HBaseDataType.DOUBLE) {
            return Bytes.toBytes(dataxDouble.asDouble());
        } else if (hbaseDataType == HBaseDataType.FLOAT) {
            return Bytes.toBytes(dataxDouble.asDouble().floatValue());
        } else {
            throw DataXException.asDataXException(BulkWriterError.RUNTIME_CAST, String.format("can not convert datax Double to hbase %s", hbaseDataType));
        }
    }

    public static byte[] toLongOrIntBytes(LongColumn dataxLong, HBaseDataType hbaseDataType) {
        if (dataxLong.getRawData() == null) {
            throw DataXException.asDataXException(BulkWriterError.RUNTIME_CAST, "input LongColumn is null");
        }
        if (hbaseDataType == HBaseDataType.LONG) {
            return Bytes.toBytes(dataxLong.asLong());
        } else if (hbaseDataType == HBaseDataType.INT) {
            return Bytes.toBytes(dataxLong.asLong().intValue());
        } else {
            throw DataXException.asDataXException(BulkWriterError.RUNTIME_CAST, String.format("can not convert datax Long to hbase %s", hbaseDataType));
        }
    }

    public static byte[] toStringBytes(StringColumn dataxString, HBaseDataType hbaseDataType, String encoding) {
        if (dataxString.getRawData() == null) {
            throw DataXException.asDataXException(BulkWriterError.RUNTIME_CAST, "input StringColumn is null");
        }
        return HBaseColumn.convertStrToBytes(dataxString.asString(), hbaseDataType, encoding);
    }

//  public static byte[] toNullBytes(NullColumn dataxDouble, HBaseDataType hbaseDataType) {
//    throw DataXException.asDataXException(BulkWriterError.RUNTIME_CAST, String.format("can not convert datax NULL to hbase %s" , hbaseDataType));
//  }

    public static byte[] toBytes(Column dataxColumn, HBaseDataType hbaseDataType) throws SQLException {
        return toBytes(dataxColumn, hbaseDataType, "utf-8");
    }

    public static byte[] toBytes(Column dataxColumn, HBaseDataType hbaseDataType, String encoding) throws SQLException {

        if (dataxColumn.getRawData() == null) {
            throw new SQLException("input dataxColumn is null");
        }
        byte[] bytes = null;

        switch (hbaseDataType) {
            case INT:
                bytes = Bytes.toBytes(dataxColumn.asLong().intValue());
                break;
            case LONG:
                bytes = Bytes.toBytes(dataxColumn.asLong());
                break;
            case DOUBLE:
                bytes = Bytes.toBytes(dataxColumn.asDouble());
                break;
            case FLOAT:
                bytes = Bytes.toBytes(dataxColumn.asDouble().floatValue());
                break;
            case SHORT:
                bytes = Bytes.toBytes(dataxColumn.asLong().shortValue());
                break;
            case BOOLEAN:
                bytes = Bytes.toBytes(dataxColumn.asBoolean());
                break;
            case STRING:
            case GBK_STRING:
                bytes = HBaseColumn.convertStrToBytes(dataxColumn.asString(), hbaseDataType, encoding);
                break;
            case BYTE:
                bytes = new byte[]{Byte.parseByte(dataxColumn.asString())};
                break;
            case BYTES:
                //备注，逻辑上应该dataxColumn.asBytes()就可以了，但是之前的处理逻辑实际是下面这一段。
                //bytes = dataxColumn.asBytes();
                try {
                    bytes = Hex.decodeHex(dataxColumn.asString().toCharArray());
                } catch (DecoderException e) {
                    throw DataXException.asDataXException(BulkWriterError.RUNTIME_CAST, "bytes String should encode by org.apache.commons.codec.binary.Hex.", e);
                }
                break;
            case PH_INT:
                bytes = PhoenixEncoder.encodeInt(dataxColumn.asLong().intValue());
                break;
            case PH_LONG:
                bytes = PhoenixEncoder.encodeLong(dataxColumn.asLong());
                break;
            case PH_DOUBLE:
                bytes = PhoenixEncoder.encodeDouble(dataxColumn.asDouble());
                break;
            case PH_FLOAT:
                bytes = PhoenixEncoder.encodeFloat(dataxColumn.asDouble().floatValue());
                break;
            case PH_SHORT:
                bytes = PhoenixEncoder.encodeShort(dataxColumn.asLong().shortValue());
                break;
            case UNSUPPORT:
                throw new IllegalArgumentException("unsupported data type " + hbaseDataType);
        }
        return bytes;
    }
}
