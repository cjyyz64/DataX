package com.alibaba.datax.plugin.writer.hbasebulkwriter2.column;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.hbasebulkwriter2.BulkWriterError;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DynamicHBaseColumn extends HBaseColumn {

  public String columnStr;
  public HBaseDataType type;

  public DynamicHBaseColumn(String columnStr, String familyStr, HBaseDataType type) {
    this.columnStr = columnStr;
    this.type = type;
    this.family = Bytes.toBytes(familyStr);
  }
  
  /**
   * convert <rowkey,column_name,timestamp,value> to KeyValue
   * if $value is null, delete all kvs before $timestamp
   * @param line
   * @param rowkeyType
   * @param columnList
   * @return
   */
  public static KeyValue toKV(ArrayList<Column> line,
      HBaseColumn.HBaseDataType rowkeyType, List<DynamicHBaseColumn> columnList) throws SQLException {
    final int ROWKEY_INDEX = 0;
    final int COLUMN_INDEX = 1;
    final int TIMESTAMP_INDEX = 2;
    final int VALUE_INDEX = 3;
    
    byte[] rowkey = DataTypeConvertor.toBytes(line.get(ROWKEY_INDEX), rowkeyType);
    String columnStr = line.get(COLUMN_INDEX).asString();
    String[] splits = columnStr.split(COLUMN_DELIMITER);
    byte[] family = convertStrToBytes(splits[0], HBaseDataType.STRING);
    byte[] qualifier = convertStrToBytes(splits[1], HBaseDataType.STRING);
    long timestamp = line.get(TIMESTAMP_INDEX).asLong();
    Column value = line.get(VALUE_INDEX);
    
    KeyValue kv = null;
    if (value.getRawData() != null) {
      for (DynamicHBaseColumn column : columnList) {
        if (columnStr.startsWith(column.columnStr)) {
          byte[] val = DataTypeConvertor.toBytes(value, column.type);
          kv = new KeyValue(rowkey, family, qualifier, timestamp, val);
          break;
        }
      }
      if (kv == null) {
          throw new SQLException(String.format("Couldn't find column(%s) type.", columnStr));
      }
    } else {
      kv = new KeyValue(rowkey, family, qualifier, timestamp, KeyValue.Type.DeleteColumn);
    }
    return kv;
  }

  public static List<? extends HBaseColumn> parseColumnStr(
      Configuration conf) {
    if(conf == null) {
      throw DataXException.asDataXException(BulkWriterError.CONFIG_MISSING, "miss hbase_column");
    }
    String matchType = conf.getString("type","prefix");
    if(matchType == null || !matchType.equals("prefix")) {
      throw DataXException.asDataXException(BulkWriterError.CONFIG_ILLEGAL, "unsupported dynamic column match type " + matchType);
    }
    List<DynamicHBaseColumn> columnList = new ArrayList<DynamicHBaseColumn>();
    List<Configuration> rules = conf.getListConfiguration("rules");
    for(Configuration rule : rules) {
      String pattern = rule.getString("pattern");
      HBaseDataType dataType = HBaseDataType.parseStr(rule.getString("htype"));
      if(dataType.equals(HBaseDataType.UNSUPPORT)) {
        throw DataXException.asDataXException(BulkWriterError.CONFIG_ILLEGAL, "illegal htype " + rule.getString("htype"));
      }
      int index = pattern.indexOf(":");
      if(index == -1) {
        throw DataXException.asDataXException(BulkWriterError.CONFIG_ILLEGAL, "prefix pattern must contains full family name");
      }
      String family = pattern.substring(0,index);
      DynamicHBaseColumn column = new DynamicHBaseColumn(pattern, family, dataType);
      columnList.add(column);
    }
    return columnList;
  }
}
