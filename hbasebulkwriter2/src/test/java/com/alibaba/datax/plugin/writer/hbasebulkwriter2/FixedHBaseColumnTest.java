package com.alibaba.datax.plugin.writer.hbasebulkwriter2;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.writer.hbasebulkwriter2.ConfigurationBuilders.FixedColumnSchemaBuilder;
import com.alibaba.datax.plugin.writer.hbasebulkwriter2.ConfigurationBuilders.RowkeySchemaBuilder;
import com.alibaba.datax.plugin.writer.hbasebulkwriter2.column.FixedHBaseColumn;
import com.alibaba.datax.plugin.writer.hbasebulkwriter2.util.PhoenixEncoder;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class FixedHBaseColumnTest {

  private static final Logger LOG = LoggerFactory
          .getLogger(FixedHBaseColumnTest.class);

  @Before
  public void setUp() {}

  @After
  public void tearDown() {}

  @Test
  public void testParseRowkeyStr() throws JSONException {
    
    RowkeySchemaBuilder rowkeySchemaBuilder = new RowkeySchemaBuilder();
    rowkeySchemaBuilder.addColumn(0, "long");
    rowkeySchemaBuilder.addConstant("byte", "2");
    rowkeySchemaBuilder.addColumn(1, "string");
    List<FixedHBaseColumn> actualRowkeyList = FixedHBaseColumn.parseRowkeySchema(rowkeySchemaBuilder.build().getListConfiguration("hbase_rowkey"));

    assertEquals(actualRowkeyList.size(), 3);

    List<FixedHBaseColumn> exceptedRowkeyList = new ArrayList<FixedHBaseColumn>();
    exceptedRowkeyList.add(new FixedHBaseColumn(0, null, null, null, FixedHBaseColumn.HBaseDataType.LONG, -1));
    exceptedRowkeyList.add(new FixedHBaseColumn(-1, null, null, new byte[]{2}, FixedHBaseColumn.HBaseDataType.BYTE, -1));
    exceptedRowkeyList.add(new FixedHBaseColumn(1, null, null, null, FixedHBaseColumn.HBaseDataType.STRING, -1));

    int i = 0;
    for (FixedHBaseColumn column : actualRowkeyList) {
      assertEquals(exceptedRowkeyList.get(i), column);
      i++;
    }
  }

  @Test
  public void testParseColumnsStr() throws JSONException {
    FixedColumnSchemaBuilder fixedColumnSchemaBuilder = new FixedColumnSchemaBuilder();
    fixedColumnSchemaBuilder.addColumn(1, "string", "cf", "name");
    fixedColumnSchemaBuilder.addColumn(2, "long", "cf", "value");
    List<FixedHBaseColumn> actualColumnList = FixedHBaseColumn.parseColumnSchema(fixedColumnSchemaBuilder.build().getListConfiguration("hbase_column"));

    assertEquals(actualColumnList.size(), 2);

    List<FixedHBaseColumn> exceptedColumnList = new ArrayList<FixedHBaseColumn>();
    exceptedColumnList.add(new FixedHBaseColumn(1, Bytes.toBytes("cf"), Bytes.toBytes("name"), null, FixedHBaseColumn.HBaseDataType.STRING, 0));
    exceptedColumnList.add(new FixedHBaseColumn(2, Bytes.toBytes("cf"), Bytes.toBytes("value"), null, FixedHBaseColumn.HBaseDataType.LONG, 1));

    int i = 0;
    for (FixedHBaseColumn column : actualColumnList) {
      assertEquals(exceptedColumnList.get(i), column);
      i++;
    }
  }

  @Test
  public void testTypeParseStr() {
    FixedHBaseColumn.HBaseDataType type = FixedHBaseColumn.HBaseDataType.parseStr("bigint");
    assertEquals(type, FixedHBaseColumn.HBaseDataType.LONG);
  }

  @Test
  public void testConvertStrToBytes() {
    byte[] actual = FixedHBaseColumn.convertStrToBytes("1", FixedHBaseColumn.HBaseDataType.INT);
    byte[] expected = Bytes.toBytes(1);
    for (int i = 0, l = actual.length; i < l; i++) {
      assertEquals(actual[i], expected[i]);
    }
  }

  @Test
  public void testToKVs() throws JSONException,SQLException {
    RowkeySchemaBuilder rowkeySchemaBuilder = new RowkeySchemaBuilder();
    rowkeySchemaBuilder.addColumn(0, "ph_long");
    rowkeySchemaBuilder.addConstant("byte", "2");
    rowkeySchemaBuilder.addColumn(1, "string");
    List<FixedHBaseColumn> rowkeyList = FixedHBaseColumn.parseRowkeySchema(rowkeySchemaBuilder.build().getListConfiguration("hbase_rowkey"));

    FixedColumnSchemaBuilder fixedColumnSchemaBuilder = new FixedColumnSchemaBuilder();
    fixedColumnSchemaBuilder.addColumn(1, "string", "cf", "name");
    fixedColumnSchemaBuilder.addColumn(2, "ph_long", "cf", "value");
    List<FixedHBaseColumn> columnList = FixedHBaseColumn.parseColumnSchema(fixedColumnSchemaBuilder.build().getListConfiguration("hbase_column"));

    ArrayList<Column> line = new ArrayList<Column>();
    LOG.info(rowkeyList + "");
    line.add(new StringColumn("1"));
    line.add(new StringColumn("hello"));
    line.add(new StringColumn("-1"));
    byte[] rowkey = FixedHBaseColumn.toRow(line, 5, rowkeyList);
    KeyValue[] kvs = FixedHBaseColumn.toKVs(line, rowkey, columnList, "utf-8",System.currentTimeMillis(),HBaseConsts.NULL_MODE_DEFAULT);
    LOG.info(rowkeyList + "");
    assertNotNull(kvs);

    Result result = new Result(kvs);
    byte[] rowkeyBytes = result.getRow();
    LOG.info(Hex.encodeHexString(rowkeyBytes));
    byte[] excepetedRowkey = Bytes.add(PhoenixEncoder.encodeLong(1l), new byte[]{2}, Bytes.toBytes("hello"));
    excepetedRowkey = PhoenixEncoder.getSaltedKey(excepetedRowkey, 5);
    LOG.info(Hex.encodeHexString(excepetedRowkey));
    assertEquals(Bytes.compareTo(excepetedRowkey, rowkeyBytes), 0);

    byte[] nameBytes = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("name"));
    byte[] valueBytes = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("value"));
    assertEquals("hello", Bytes.toString(nameBytes));
    assertTrue(Bytes.equals(PhoenixEncoder.encodeLong(-1), valueBytes));
  }
  
  @Test
  public void testConfigMissingTable() throws JSONException {
    JSONObject fixedcolumn = new JSONObject();
    // add hbase_rowkey
    JSONObject rowkeyElement = new JSONObject();
    rowkeyElement.put("index", "0");
    rowkeyElement.put("htype", "long");
    JSONArray rowkeyArray = new JSONArray();
    rowkeyArray.put(rowkeyElement);
    fixedcolumn.put("hbase_rowkey", rowkeyArray);
    // add hbase_column
    JSONObject columnElement = new JSONObject();
    columnElement.put("index", "3");
    columnElement.put("hname", "cf:col1");
    columnElement.put("htype", "long");
    JSONArray columnArray = new JSONArray();
    columnArray.put(columnElement);
    fixedcolumn.put("hbase_column", columnArray);
    fixedcolumn.put("hbase_output", "/hbasebulkwriter2");
    fixedcolumn.put("hbase_config", "hbase-site.xml");
    fixedcolumn.put("hdfs_config", "hdfs-site.xml");
    System.out.println(fixedcolumn.toString());
    
    JSONObject jsonConf = new JSONObject();
    jsonConf.put("fixedcolumn", fixedcolumn);
    HBaseBulker bulker = new HBaseBulker();
    try {
      bulker.loadFixedColumnConfig(com.alibaba.datax.common.util.Configuration.from(jsonConf.toString()));
      fail();    
    } catch (DataXException e) {
      assertEquals("Code:[10001], Description:[config missing].  - Missing config hbase_table.", e.getMessage());
    }
  }
  
  @Test
  public void testConfigMissingRowkey() throws JSONException {
    JSONObject fixedcolumn = new JSONObject();
    fixedcolumn.put("hbase_table", "bulkwriter");
    // add hbase_column
    JSONObject columnElement = new JSONObject();
    columnElement.put("index", "3");
    columnElement.put("hname", "cf:col1");
    columnElement.put("htype", "long");
    JSONArray columnArray = new JSONArray();
    columnArray.put(columnElement);
    fixedcolumn.put("hbase_column", columnArray);
    fixedcolumn.put("hbase_output", "/hbasebulkwriter2");
    fixedcolumn.put("hbase_config", "hbase-site.xml");
    fixedcolumn.put("hdfs_config", "hdfs-site.xml");
    System.out.println(fixedcolumn.toString());
    
    JSONObject jsonConf = new JSONObject();
    jsonConf.put("fixedcolumn", fixedcolumn);
    HBaseBulker bulker = new HBaseBulker();
    try {
      bulker.loadFixedColumnConfig(com.alibaba.datax.common.util.Configuration.from(jsonConf.toString()));
      fail();    
    } catch (DataXException e) {
      assertEquals("Code:[10001], Description:[config missing].  - missing hbase_rowkey", e.getMessage());
    }
  }
  
  @Test
  public void testSpecifyUnkownTypeOnRowkey() throws JSONException {
    JSONObject fixedcolumn = new JSONObject();
    fixedcolumn.put("hbase_table", "bulkwriter");
    // add hbase_rowkey
    JSONObject rowkeyElement = new JSONObject();
    rowkeyElement.put("index", "0");
    rowkeyElement.put("htype", "unkown");
    JSONArray rowkeyArray = new JSONArray();
    rowkeyArray.put(rowkeyElement);
    fixedcolumn.put("hbase_rowkey", rowkeyArray);
    com.alibaba.datax.common.util.Configuration conf = com.alibaba.datax.common.util.Configuration.from(fixedcolumn.toString());
    try {
      FixedHBaseColumn.parseRowkeySchema(conf.getListConfiguration("hbase_rowkey"));
    } catch(DataXException e){
      assertEquals("Code:[10002], Description:[config illegal].  - illegal htype unkown", e.getMessage());
    }
  }
  
  @Test
  public void testConfigMissingColumn() throws JSONException {
    JSONObject fixedcolumn = new JSONObject();
    fixedcolumn.put("hbase_table", "bulkwriter");
    // add hbase_rowkey
    JSONObject rowkeyElement = new JSONObject();
    rowkeyElement.put("index", "0");
    rowkeyElement.put("htype", "long");
    JSONArray rowkeyArray = new JSONArray();
    rowkeyArray.put(rowkeyElement);
    fixedcolumn.put("hbase_rowkey", rowkeyArray);

    fixedcolumn.put("hbase_output", "/hbasebulkwriter2");
    fixedcolumn.put("hbase_config", "hbase-site.xml");
    fixedcolumn.put("hdfs_config", "hdfs-site.xml");
    JSONObject option = new JSONObject();
    option.put("start_ts", "1234");
    option.put("time_col", "1");
    fixedcolumn.put("optional", option);
    System.out.println(fixedcolumn.toString());
    
    JSONObject jsonConf = new JSONObject();
    jsonConf.put("fixedcolumn", fixedcolumn);
    HBaseBulker bulker = new HBaseBulker();
    try {
      bulker.loadFixedColumnConfig(com.alibaba.datax.common.util.Configuration.from(jsonConf.toString()));
      fail();    
    } catch (DataXException e) {
      assertEquals("Code:[10001], Description:[config missing].  - missing hbase_column", e.getMessage());
    }
  }
  
  @Test
  public void testSpecifyUnkownTypeOnColumn() throws JSONException {
    JSONObject fixedcolumn = new JSONObject();
    JSONObject columnElement = new JSONObject();
    columnElement.put("index", "3");
    columnElement.put("hname", "cf:col");
    columnElement.put("htype", "unkown");
    JSONArray columnArray = new JSONArray();
    columnArray.put(columnElement);
    fixedcolumn.put("hbase_column", columnArray);
    com.alibaba.datax.common.util.Configuration conf = com.alibaba.datax.common.util.Configuration.from(fixedcolumn.toString());
    try {
      FixedHBaseColumn.parseColumnSchema(conf.getListConfiguration("hbase_column"));
      fail();
    } catch (DataXException e) {
      assertEquals("Code:[10002], Description:[config illegal].  - illegal htype unkown", e.getMessage());
    }
  }
  
  @Test
  public void testConfigMissingHbasePath() throws JSONException {
    JSONObject fixedcolumn = new JSONObject();
    fixedcolumn.put("hbase_table", "bulkwriter");
    // add hbase_rowkey
    JSONObject rowkeyElement = new JSONObject();
    rowkeyElement.put("index", "0");
    rowkeyElement.put("htype", "long");
    JSONArray rowkeyArray = new JSONArray();
    rowkeyArray.put(rowkeyElement);
    fixedcolumn.put("hbase_rowkey", rowkeyArray);
    // add hbase_column
    JSONObject columnElement = new JSONObject();
    columnElement.put("index", "3");
    columnElement.put("hname", "cf:col1");
    columnElement.put("htype", "long");
    JSONArray columnArray = new JSONArray();
    columnArray.put(columnElement);
    fixedcolumn.put("hbase_column", columnArray);
    fixedcolumn.put("hbase_output", "/hbasebulkwriter2");

    fixedcolumn.put("hdfs_config", "hdfs-site.xml");
    
    JSONObject jsonConf = new JSONObject();
    jsonConf.put("fixedcolumn", fixedcolumn);
    HBaseBulker bulker = new HBaseBulker();
    try {
      bulker.loadFixedColumnConfig(com.alibaba.datax.common.util.Configuration.from(jsonConf.toString()));
      fail();    
    } catch (DataXException e) {
      assertEquals("Code:[10001], Description:[config missing].  - Missing config hbase_config.", e.getMessage());
    }
  }
  
  @Test
  public void testConfigMissingHdfsPath() throws JSONException {
    JSONObject fixedcolumn = new JSONObject();
    fixedcolumn.put("hbase_table", "bulkwriter");
    // add hbase_rowkey
    JSONObject rowkeyElement = new JSONObject();
    rowkeyElement.put("index", "0");
    rowkeyElement.put("htype", "long");
    JSONArray rowkeyArray = new JSONArray();
    rowkeyArray.put(rowkeyElement);
    fixedcolumn.put("hbase_rowkey", rowkeyArray);
    // add hbase_column
    JSONObject columnElement = new JSONObject();
    columnElement.put("index", "3");
    columnElement.put("hname", "cf:col1");
    columnElement.put("htype", "long");
    JSONArray columnArray = new JSONArray();
    columnArray.put(columnElement);
    fixedcolumn.put("hbase_column", columnArray);
    fixedcolumn.put("hbase_output", "/hbasebulkwriter2");
    fixedcolumn.put("hbase_config", "hbase-site.xml");
    System.out.println(fixedcolumn.toString());
    
    JSONObject jsonConf = new JSONObject();
    jsonConf.put("fixedcolumn", fixedcolumn);
    HBaseBulker bulker = new HBaseBulker();
    try {
      bulker.loadFixedColumnConfig(com.alibaba.datax.common.util.Configuration.from(jsonConf.toString()));
      fail();    
    } catch (DataXException e) {
      assertEquals("Code:[10001], Description:[config missing].  - Missing config hdfs_config.", e.getMessage());
    }
  }
  
  @Test
  public void testConfigMissingOptionalIsOK() throws JSONException {
    JSONObject fixedcolumn = new JSONObject();
    fixedcolumn.put("hbase_table", "bulkwriter");
    // add hbase_rowkey
    JSONObject rowkeyElement = new JSONObject();
    rowkeyElement.put("index", "0");
    rowkeyElement.put("htype", "long");
    JSONArray rowkeyArray = new JSONArray();
    rowkeyArray.put(rowkeyElement);
    fixedcolumn.put("hbase_rowkey", rowkeyArray);
    // add hbase_column
    JSONObject columnElement = new JSONObject();
    columnElement.put("index", "3");
    columnElement.put("hname", "cf:col1");
    columnElement.put("htype", "long");
    JSONArray columnArray = new JSONArray();
    columnArray.put(columnElement);
    fixedcolumn.put("hbase_column", columnArray);
    fixedcolumn.put("hbase_output", "/hbasebulkwriter2");
    fixedcolumn.put("hbase_config", "hbase-site.xml");
    fixedcolumn.put("hdfs_config", "hdfs-site.xml");
    System.out.println(fixedcolumn.toString());
    
    JSONObject jsonConf = new JSONObject();
    jsonConf.put("fixedcolumn", fixedcolumn);
    HBaseBulker bulker = new HBaseBulker();
    try {
      bulker.loadFixedColumnConfig(com.alibaba.datax.common.util.Configuration.from(jsonConf.toString()));         
    } catch (DataXException e) {
      fail();
    }
  }
  
  @Test
  public void testConfigInvalidHname() throws JSONException {
    JSONObject fixedcolumn = new JSONObject();
    JSONObject columnElement = new JSONObject();
    columnElement.put("index", "3");
    columnElement.put("hname", "col");
    columnElement.put("htype", "long");
    JSONArray columnArray = new JSONArray();
    columnArray.put(columnElement);
    fixedcolumn.put("hbase_column", columnArray);
    com.alibaba.datax.common.util.Configuration conf = com.alibaba.datax.common.util.Configuration.from(fixedcolumn.toString());
    try {
      FixedHBaseColumn.parseColumnSchema(conf.getListConfiguration("hbase_column"));
      fail();
    } catch (DataXException e) {
      assertEquals("Code:[10002], Description:[config illegal].  - column name illegal col, should be like cf:col", e.getMessage());
    }
  }
  
  @Test
  public void testSetStartTsAndTimeColAtSameTime() throws JSONException {
    JSONObject fixedcolumn = new JSONObject();
    fixedcolumn.put("hbase_table", "bulkwriter");
    // add hbase_rowkey
    JSONObject rowkeyElement = new JSONObject();
    rowkeyElement.put("index", "0");
    rowkeyElement.put("htype", "long");
    JSONArray rowkeyArray = new JSONArray();
    rowkeyArray.put(rowkeyElement);
    fixedcolumn.put("hbase_rowkey", rowkeyArray);
    // add hbase_column
    JSONObject columnElement = new JSONObject();
    columnElement.put("index", "3");
    columnElement.put("hname", "cf:col1");
    columnElement.put("htype", "long");
    JSONArray columnArray = new JSONArray();
    columnArray.put(columnElement);
    fixedcolumn.put("hbase_column", columnArray);
    fixedcolumn.put("hbase_output", "/hbasebulkwriter2");
    fixedcolumn.put("hbase_config", "hbase-site.xml");
    fixedcolumn.put("hdfs_config", "hdfs-site.xml");
    JSONObject option = new JSONObject();
    option.put("start_ts", "1234");
    option.put("time_col", "1");
    fixedcolumn.put("optional", option);
    System.out.println(fixedcolumn.toString());
    
    JSONObject jsonConf = new JSONObject();
    jsonConf.put("fixedcolumn", fixedcolumn);
    HBaseBulker bulker = new HBaseBulker();
    try {
      bulker.loadFixedColumnConfig(com.alibaba.datax.common.util.Configuration.from(jsonConf.toString()));
      fail();    
    } catch (DataXException e) {
      assertEquals("Code:[10002], Description:[config illegal].  - can not set time_col and start_ts at the same time.", e.getMessage());
    }
  }
  
  @Test
  public void testMissingHdfsoutputPath() throws JSONException {
    JSONObject fixedcolumn = new JSONObject();
    fixedcolumn.put("hbase_table", "bulkwriter");
    // add hbase_rowkey
    JSONObject rowkeyElement = new JSONObject();
    rowkeyElement.put("index", "0");
    rowkeyElement.put("htype", "long");
    JSONArray rowkeyArray = new JSONArray();
    rowkeyArray.put(rowkeyElement);
    fixedcolumn.put("hbase_rowkey", rowkeyArray);
    // add hbase_column
    JSONObject columnElement = new JSONObject();
    columnElement.put("index", "3");
    columnElement.put("hname", "cf:col1");
    columnElement.put("htype", "long");
    JSONArray columnArray = new JSONArray();
    columnArray.put(columnElement);
    fixedcolumn.put("hbase_column", columnArray);
    fixedcolumn.put("hbase_config", "hbase-site.xml");
    fixedcolumn.put("hdfs_config", "hdfs-site.xml");
    fixedcolumn.put("optional", "");
    System.out.println(fixedcolumn.toString());
    
    JSONObject jsonConf = new JSONObject();
    jsonConf.put("fixedcolumn", fixedcolumn);
    HBaseBulker bulker = new HBaseBulker();
    try {
      bulker.loadFixedColumnConfig(com.alibaba.datax.common.util.Configuration.from(jsonConf.toString()));
      fail();
    } catch (DataXException e) {
      assertEquals("Code:[10001], Description:[config missing].  - Missing config hbase_output.", e.getMessage());
    }
  }
}
