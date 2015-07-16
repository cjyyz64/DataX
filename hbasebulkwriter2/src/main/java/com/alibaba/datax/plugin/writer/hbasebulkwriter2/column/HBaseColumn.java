package com.alibaba.datax.plugin.writer.hbasebulkwriter2.column;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.datax.plugin.writer.hbasebulkwriter2.BulkWriterError;
import com.alibaba.datax.plugin.writer.hbasebulkwriter2.util.PhoenixEncoder;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Callable;

public class HBaseColumn {

    public static final String COLUMN_DELIMITER = "[|:,]";
    public static final String ROWKEY_DELIMITER = "[|,]";

    public byte[] family;

    /**
     * Make sure the column families are created in the target table.
     *
     * @param htable
     * @param columnList
     * @throws java.io.IOException
     */
    public static void checkColumnList(final HTable htable, List<? extends HBaseColumn> columnList, int retryTimes) {
        final String tableName = Bytes.toString(htable.getTableName());
        @SuppressWarnings("deprecation")
        HTableDescriptor desc;

        retryTimes = retryTimes < 1 ? 1 : retryTimes;

        try {
            desc = RetryUtil.executeWithRetry(new Callable<HTableDescriptor>() {
                @Override
                public HTableDescriptor call() throws Exception {
                    return htable.getConnection().getHTableDescriptors(Collections.singletonList(tableName))[0];
                }
            }, retryTimes, 1, true);
        } catch (Exception e) {
            throw DataXException.asDataXException(BulkWriterError.IO, "请联系hbase同学检查hbase集群", e);
        }

        if (desc == null) {
            throw DataXException.asDataXException(BulkWriterError.IO, "获取HTableDescriptor失败，请联系hbase同学检查hbase集群");
        }

        boolean isErr = false;
        HashSet<String> unexistFamlies = new HashSet<String>();
        for (HBaseColumn column : columnList) {
            if (!desc.hasFamily(column.family)) {
                isErr = true;
                unexistFamlies.add(Bytes.toString(column.family));
            }
        }

        if (isErr) {
            throw DataXException.asDataXException(BulkWriterError.RUNTIME,
                    "Column Families " + unexistFamlies + " don't exist on table " + tableName);
        }
    }

    public static byte[] convertStrToBytes(String str, HBaseColumn.HBaseDataType type) {
        byte[] result = convertStrToBytes(str, type, HConstants.UTF8_ENCODING);
        if (result == null) {
            result = HConstants.EMPTY_BYTE_ARRAY;
        }
        return result;
    }

    public static byte[] convertStrToBytes(String str, HBaseColumn.HBaseDataType type, String encoding) {
        byte[] bytes = null;

        if (str == null) {
            return null;
        }

        switch (type) {
            case INT:
                bytes = Bytes.toBytes(Integer.parseInt(str));
                break;
            case LONG:
                bytes = Bytes.toBytes(Long.parseLong(str));
                break;
            case DOUBLE:
                bytes = Bytes.toBytes(Double.parseDouble(str));
                break;
            case FLOAT:
                bytes = Bytes.toBytes(Float.parseFloat(str));
                break;
            case SHORT:
                bytes = Bytes.toBytes(Short.parseShort(str));
                break;
            case BOOLEAN:
                bytes = Bytes.toBytes(Boolean.parseBoolean(str));
                break;
            case STRING:
                bytes = str.getBytes(Charset.forName(encoding));
                break;
            case GBK_STRING:
                bytes = str.getBytes(Charset.forName("GBK"));
                break;
            case BYTE:
                bytes = new byte[]{Byte.parseByte(str)};
                break;
            case BYTES:
                try {
                    bytes = Hex.decodeHex(str.toCharArray());
                } catch (DecoderException e) {
                    throw DataXException.asDataXException(BulkWriterError.RUNTIME_CAST, "bytes String should encode by org.apache.commons.codec.binary.Hex.", e);
                }
                break;
            case PH_INT:
                bytes = PhoenixEncoder.encodeInt(Integer.parseInt(str));
                break;
            case PH_LONG:
                bytes = PhoenixEncoder.encodeLong(Long.parseLong(str));
                break;
            case PH_DOUBLE:
                bytes = PhoenixEncoder.encodeDouble(Double.parseDouble(str));
                break;
            case PH_FLOAT:
                bytes = PhoenixEncoder.encodeFloat(Float.parseFloat(str));
                break;
            case PH_SHORT:
                bytes = PhoenixEncoder.encodeShort(Short.parseShort(str));
                break;
            case UNSUPPORT:
                throw new IllegalArgumentException("unsupported data type " + type);
        }

        return bytes;
    }

    /**
     * The datetime in ODPS would be convert to string in HBase.
     */
    public enum HBaseDataType {
        BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, PH_SHORT, PH_INT, PH_LONG, PH_FLOAT, PH_DOUBLE, STRING, GBK_STRING, BYTES, UNSUPPORT;

        public static HBaseDataType parseStr(String typeStr) {
            return parseStr(typeStr, false);
        }

        public static HBaseDataType parseStr(String typeStr, boolean isPhoenixStyle) {
            HBaseDataType type = null;
            if (!isPhoenixStyle) {
                if (typeStr.equalsIgnoreCase("int")) {
                    type = INT;
                } else if (typeStr.equalsIgnoreCase("long") || typeStr.equalsIgnoreCase(
                        "bigint")) {
                    type = LONG;
                } else if (typeStr.equalsIgnoreCase("double")) {
                    type = DOUBLE;
                } else if (typeStr.equalsIgnoreCase("float")) {
                    type = FLOAT;
                } else if (typeStr.equalsIgnoreCase("short")) {
                    type = SHORT;
                } else if (typeStr.equalsIgnoreCase("ph_int")) {
                    type = PH_INT;
                } else if (typeStr.equalsIgnoreCase(
                        "ph_bigint") || typeStr.equalsIgnoreCase("ph_long")) {
                    type = PH_LONG;
                } else if (typeStr.equalsIgnoreCase("ph_double")) {
                    type = PH_DOUBLE;
                } else if (typeStr.equalsIgnoreCase("ph_float")) {
                    type = PH_FLOAT;
                } else if (typeStr.equalsIgnoreCase("ph_short")) {
                    type = PH_SHORT;
                }
            } else {
                if (typeStr.equalsIgnoreCase("int") || typeStr.equalsIgnoreCase(
                        "ph_int")) {
                    type = PH_INT;
                } else if (typeStr.equalsIgnoreCase("long") || typeStr.equalsIgnoreCase(
                        "bigint") || typeStr.equalsIgnoreCase(
                        "ph_long") || typeStr.equalsIgnoreCase("ph_bigint")) {
                    type = PH_LONG;
                } else if (typeStr.equalsIgnoreCase(
                        "double") || typeStr.equalsIgnoreCase("ph_double")) {
                    type = PH_DOUBLE;
                } else if (typeStr.equalsIgnoreCase(
                        "float") || typeStr.equalsIgnoreCase("ph_float")) {
                    type = PH_FLOAT;
                } else if (typeStr.equalsIgnoreCase(
                        "short") || typeStr.equalsIgnoreCase("ph_short")) {
                    type = PH_SHORT;
                }
            }
            if (type == null) {
                if (typeStr.equalsIgnoreCase("string") || typeStr.equalsIgnoreCase(
                        "datetime")) {
                    type = STRING;
                } else if (typeStr.equalsIgnoreCase("gbk_string")) {
                    type = GBK_STRING;
                } else if (typeStr.equalsIgnoreCase("boolean")) {
                    type = BOOLEAN;
                } else if (typeStr.equalsIgnoreCase("byte")) {
                    type = BYTE;
                } else if (typeStr.equalsIgnoreCase("bytes")) {
                    type = BYTES;
                } else {
                    type = UNSUPPORT;
                }
            }
            return type;
        }

        @Override
        public String toString() {
            String typeStr = null;
            switch (this) {
                case INT:
                    typeStr = "int";
                    break;
                case LONG:
                    typeStr = "long";
                    break;
                case DOUBLE:
                    typeStr = "double";
                    break;
                case FLOAT:
                    typeStr = "float";
                    break;
                case SHORT:
                    typeStr = "short";
                    break;
                case STRING:
                    typeStr = "string";
                    break;
                case GBK_STRING:
                    typeStr = "gbk_string";
                    break;
                case BYTE:
                    typeStr = "byte";
                    break;
                case BYTES:
                    typeStr = "bytes";
                    break;
                case PH_INT:
                    typeStr = "ph_int";
                    break;
                case PH_LONG:
                    typeStr = "ph_bigint";
                    break;
                case PH_DOUBLE:
                    typeStr = "ph_double";
                    break;
                case PH_FLOAT:
                    typeStr = "ph_float";
                    break;
                case PH_SHORT:
                    typeStr = "ph_short";
                    break;
                case BOOLEAN:
                    typeStr = "boolean";
                    break;
                case UNSUPPORT:
                default:
                    typeStr = "unsupport";
                    break;
            }
            return typeStr;
        }
    }
}
