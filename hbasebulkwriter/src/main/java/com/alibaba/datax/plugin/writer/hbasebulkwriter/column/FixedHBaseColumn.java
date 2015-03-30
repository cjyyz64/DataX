package com.alibaba.datax.plugin.writer.hbasebulkwriter.column;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.hbasebulkwriter.BulkWriterError;
import com.alibaba.datax.plugin.writer.hbasebulkwriter.HBaseConsts;
import com.alibaba.datax.plugin.writer.hbasebulkwriter.util.PhoenixEncoder;
import com.alibaba.fastjson.JSONObject;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;

/**
 * This class contains some methods related to the HBase Column operation.
 * Because the column may be combined into the rowkey, this class could also
 * denote the field which would be contained in the rowkey.
 */
public class FixedHBaseColumn extends HBaseColumn {

  public static final Comparator<FixedHBaseColumn> indexComparator =
      new Comparator<FixedHBaseColumn>() {
        @Override
        public int compare(FixedHBaseColumn column, FixedHBaseColumn otherColumn) {
          return column.index - otherColumn.index;
        }
      };
  public int index;
  public byte[] qualifier;
  public byte[] fixed;
  public HBaseDataType type;
  public int order;

  /**
   * 
   * @param index position of the field in line
   * @param family
   * @param qualifier
   * @param type
   * @param order ranking of the column name in HFile
   */
  public FixedHBaseColumn(int index, byte[] family, byte[] qualifier, byte[] fixed, HBaseDataType type,
                          int order) {
    this.index = index;
    this.family = family;
    this.qualifier = qualifier;
    this.fixed = fixed;
    this.type = type;
    this.order = order;
  }

  public static byte[] toRow(ArrayList<Column> line, int bucketNum,
                             List<FixedHBaseColumn> rowkeyList) {
    byte[] rowkey = {};
    for (FixedHBaseColumn column : rowkeyList) {
      try {
        if (column.index != -1) {
          Column field = line.get(column.index);
          if (field.getRawData() == null) {
            throw DataXException.asDataXException(BulkWriterError.RUNTIME,
                "The value of field which combined into rowkey is not allowed to null.");
          }
          byte[] value = DataTypeConvertor.toBytes(field, column.type);
          rowkey = Bytes.add(rowkey, value);
        } else {
          rowkey = Bytes.add(rowkey, column.fixed);
        }
      } catch (Throwable e) {
        throw DataXException.asDataXException(BulkWriterError.RUNTIME, String.format("fail parse row on %s column",
            column.index), e);
      }
    }
    if (bucketNum != -1) {
      rowkey = PhoenixEncoder.getSaltedKey(rowkey, bucketNum);
    }
    return rowkey;
  }

  public static KeyValue[] toKVs(ArrayList<Column> line, byte[] rowkey,
      List<FixedHBaseColumn> columnList, String encoding, long timestamp,
      String nullMode) {
    KeyValue[] kvs = new KeyValue[columnList.size()];
    nullMode = nullMode.toUpperCase();
    for (FixedHBaseColumn hbaseColumn : columnList) {
      try {
        Column field = line.get(hbaseColumn.index);        
        if (field.getRawData() != null) {
          byte[] value = DataTypeConvertor.toBytes(field, hbaseColumn.type, encoding);
          kvs[hbaseColumn.order] = new KeyValue(rowkey, hbaseColumn.family,
              hbaseColumn.qualifier, timestamp, value);
        } else {
          if (nullMode.equals(HBaseConsts.NULL_MODE_EMPTY_BYTES)) {
            kvs[hbaseColumn.order] = new KeyValue(rowkey, hbaseColumn.family,
                hbaseColumn.qualifier, timestamp, HConstants.EMPTY_BYTE_ARRAY);
          } else if (nullMode.equals(HBaseConsts.NULL_MODE_SKIP)) {
            kvs[hbaseColumn.order] = null;
          } else if (nullMode.equals(HBaseConsts.NULL_MODE_DELETE)) {
            kvs[hbaseColumn.order] = new KeyValue(rowkey, hbaseColumn.family,
                hbaseColumn.qualifier, timestamp,
                org.apache.hadoop.hbase.KeyValue.Type.DeleteColumn);
          } else {
            throw new IllegalArgumentException("unkown null-mode : " + nullMode);
          }
        }
      } catch (Throwable e) {
        throw DataXException.asDataXException(BulkWriterError.RUNTIME, String.format(
            "fail parse column on %s column", hbaseColumn.index), e);
      }
    }
    return kvs;
  }

  @Override
  public String toString() {
    return toJson().toString();
  }

  public JSONObject toJson() {
    JSONObject json = new JSONObject();
    json.put("index", index);
    if (family != null) {
      json.put("family", Hex.encodeHexString(family));
    }
    if (qualifier != null) {
      json.put("qualifier", Hex.encodeHexString(qualifier));
    }
    json.put("type", type.toString());
    json.put("order", order);
    return json;
  }

  @Override
  public boolean equals(Object obj) {
    FixedHBaseColumn excepted = (FixedHBaseColumn) obj;
    boolean result = true;

    byte[] family = this.family == null ? new byte[0] : this.family;
    byte[] exceptedFamily =
        excepted.family == null ? new byte[0] : excepted.family;
    byte[] qualifier = this.qualifier == null ? new byte[0] : this.qualifier;
    byte[] exceptedQualifier =
        this.qualifier == null ? new byte[0] : this.qualifier;
    byte[] fixed = this.fixed == null ? new byte[0] : this.fixed;
    byte[] excepetedFixed = excepted.fixed == null ? new byte[0] : excepted.fixed;

    if (Bytes.compareTo(family, exceptedFamily) != 0
        || Bytes.compareTo(qualifier, exceptedQualifier) != 0
        || Bytes.compareTo(fixed, excepetedFixed) != 0
        || type != excepted.type || order != excepted.order) {
      result = false;
    }
    return result;
  }

  public static List<FixedHBaseColumn> parseRowkeySchema(
      List<Configuration> configurations) {
    if(configurations == null || configurations.isEmpty()) {
      throw DataXException.asDataXException(BulkWriterError.CONFIG_MISSING, "missing hbase_rowkey");
    }
    List<FixedHBaseColumn> rowkeyColumns = new ArrayList<FixedHBaseColumn>();
    for(Configuration conf : configurations) {
      int index = conf.getInt("index");
      HBaseDataType dataType = HBaseDataType.parseStr(conf.getString("htype"));
      if(dataType.equals(HBaseDataType.UNSUPPORT)) {
        throw DataXException.asDataXException(BulkWriterError.CONFIG_ILLEGAL, "illegal htype " + conf.getString("htype"));
      }
      byte[] constant = null;
      if(index == -1) {
        constant = convertStrToBytes(conf.getString("constant"), dataType);
      }
      FixedHBaseColumn column = new FixedHBaseColumn(index,null,null,constant,dataType,-1);
      rowkeyColumns.add(column);
    }
    return rowkeyColumns;
  }

  public static List<FixedHBaseColumn> parseColumnSchema(
      List<Configuration> configurations) {
    if(configurations == null || configurations.isEmpty()) {
      throw DataXException.asDataXException(BulkWriterError.CONFIG_MISSING, "missing hbase_column");
    }
    List<FixedHBaseColumn> columns = new ArrayList<FixedHBaseColumn>();
    List<String> qualifierList = new LinkedList<String>();
    for(Configuration conf : configurations) {
      int index = conf.getInt("index");
      String hname = conf.getString("hname");
      String[] namepair = hname.split(":");
      if(namepair.length != 2){
        throw DataXException.asDataXException(BulkWriterError.CONFIG_ILLEGAL, "column name illegal " + hname + ", should be like cf:col");
      }
      byte[] family = Bytes.toBytes(namepair[0]);
      byte[] qualifier = Bytes.toBytes(namepair[1]);
      qualifierList.add(namepair[0] + namepair[1]);
      HBaseDataType dataType = HBaseDataType.parseStr(conf.getString("htype"));
      if(dataType.equals(HBaseDataType.UNSUPPORT)) {
        throw DataXException.asDataXException(BulkWriterError.CONFIG_ILLEGAL, "illegal htype " + conf.getString("htype"));
      }
      FixedHBaseColumn column = new FixedHBaseColumn(index, family, qualifier, null, dataType, 0);
      columns.add(column);
    }
    Collections.sort(qualifierList);

    for (FixedHBaseColumn column : columns) {
      column.order =
          qualifierList.indexOf(Bytes.toString(column.family)
              + Bytes.toString(column.qualifier));
    }

    Collections.sort(columns, indexComparator);
    return columns;
  }

}
