package com.alibaba.datax.plugin.writer.hbasebulkwriter;

import static org.junit.Assert.*;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.writer.hbasebulkwriter.column.DynamicHBaseColumn;

public class DynamicHBaseColumnTest {
  
  @Test
  public void configMissingTable() throws JSONException {
    JSONObject dynamicColumn = new JSONObject();
    dynamicColumn.put("rowkey_type", "string");
    // add hbase_column
    JSONObject columnElement = new JSONObject();
    columnElement.put("pattern", "cf:col1");
    columnElement.put("htype", "long");
    JSONArray columnArray = new JSONArray();
    columnArray.put(columnElement);
    JSONObject rule = new JSONObject();
    rule.put("type", "prefix");
    rule.put("rules", columnArray);
    dynamicColumn.put("hbase_column", rule);
    dynamicColumn.put("hbase_output", "/hbasebulkwriter");
    dynamicColumn.put("hbase_config", "hbase-site.xml");
    dynamicColumn.put("hdfs_config", "hdfs-site.xml");
    System.out.println(dynamicColumn.toString());
    
    JSONObject jsonConf = new JSONObject();
    jsonConf.put("dynamiccolumn", dynamicColumn);
    HBaseBulker bulker = new HBaseBulker();
    try {
      bulker.loadDynamicColumnConfig(com.alibaba.datax.common.util.Configuration.from(jsonConf.toString()));
      fail();    
    } catch (DataXException e) {
      assertEquals("Code:[10001], Description:[config missing].  - Missing config hbase_table.", e.getMessage());
    }
  }

  @Test
  public void configMissingRowkeyType() throws JSONException {
    JSONObject dynamicColumn = new JSONObject();
    dynamicColumn.put("hbase_table", "bulkwriter");
    // add hbase_column
    JSONObject columnElement = new JSONObject();
    columnElement.put("pattern", "cf:col1");
    columnElement.put("htype", "long");
    JSONArray columnArray = new JSONArray();
    columnArray.put(columnElement);
    JSONObject rule = new JSONObject();
    rule.put("type", "prefix");
    rule.put("rules", columnArray);
    dynamicColumn.put("hbase_column", rule);
    dynamicColumn.put("hbase_output", "/hbasebulkwriter");
    dynamicColumn.put("hbase_config", "hbase-site.xml");
    dynamicColumn.put("hdfs_config", "hdfs-site.xml");
    System.out.println(dynamicColumn.toString());
    
    JSONObject jsonConf = new JSONObject();
    jsonConf.put("dynamiccolumn", dynamicColumn);
    HBaseBulker bulker = new HBaseBulker();
    try {
      bulker.loadDynamicColumnConfig(com.alibaba.datax.common.util.Configuration.from(jsonConf.toString()));
      fail();    
    } catch (DataXException e) {
      assertEquals("Code:[10001], Description:[config missing].  - Missing conif rowkey_type", e.getMessage());
    }
  }
  
  @Test
  public void configIllegalHtype() throws JSONException {
    JSONObject columnElement = new JSONObject();
    columnElement.put("pattern", "cf:col1");
    columnElement.put("htype", "unkown");
    JSONArray columnArray = new JSONArray();
    columnArray.put(columnElement);
    JSONObject rule = new JSONObject();
    rule.put("type", "prefix");
    rule.put("rules", columnArray);    

    try {
      DynamicHBaseColumn.parseColumnStr(com.alibaba.datax.common.util.Configuration.from(rule.toString()));
      fail();
    } catch (DataXException e) {
      assertEquals("Code:[10002], Description:[config illegal].  - illegal htype unkown", e.getMessage());
    }
  }
  
  @Test
  public void configMissingFamily() throws JSONException {
    JSONObject columnElement = new JSONObject();
    columnElement.put("pattern", "col1");
    columnElement.put("htype", "long");
    JSONArray columnArray = new JSONArray();
    columnArray.put(columnElement);
    JSONObject rule = new JSONObject();
    rule.put("type", "prefix");
    rule.put("rules", columnArray);    

    try {
      DynamicHBaseColumn.parseColumnStr(com.alibaba.datax.common.util.Configuration.from(rule.toString()));
      fail();
    } catch (DataXException e) {
      assertEquals("Code:[10002], Description:[config illegal].  - prefix pattern must contains full family name", e.getMessage());
    }
  }
  
  @Test
  public void configMissingHbaseconfigPath() throws JSONException {
    JSONObject dynamicColumn = new JSONObject();
    dynamicColumn.put("hbase_table", "bulkwriter");
    dynamicColumn.put("rowkey_type", "string");
    // add hbase_column
    JSONObject columnElement = new JSONObject();
    columnElement.put("pattern", "cf:col1");
    columnElement.put("htype", "long");
    JSONArray columnArray = new JSONArray();
    columnArray.put(columnElement);
    JSONObject rule = new JSONObject();
    rule.put("type", "prefix");
    rule.put("rules", columnArray);
    dynamicColumn.put("hbase_column", rule);
    dynamicColumn.put("hbase_output", "/hbasebulkwriter");
    dynamicColumn.put("hdfs_config", "hdfs-site.xml");
    System.out.println(dynamicColumn.toString());
    
    JSONObject jsonConf = new JSONObject();
    jsonConf.put("dynamiccolumn", dynamicColumn);
    HBaseBulker bulker = new HBaseBulker();
    try {
      bulker.loadDynamicColumnConfig(com.alibaba.datax.common.util.Configuration.from(jsonConf.toString()));
      fail();    
    } catch (DataXException e) {
      assertEquals("Code:[10001], Description:[config missing].  - Missing config hbase_config.", e.getMessage());
    }
  }
  
  @Test
  public void configMissingHdfsconfigPath() throws JSONException {
    JSONObject dynamicColumn = new JSONObject();
    dynamicColumn.put("hbase_table", "bulkwriter");
    dynamicColumn.put("rowkey_type", "string");
    // add hbase_column
    JSONObject columnElement = new JSONObject();
    columnElement.put("pattern", "cf:col1");
    columnElement.put("htype", "long");
    JSONArray columnArray = new JSONArray();
    columnArray.put(columnElement);
    JSONObject rule = new JSONObject();
    rule.put("type", "prefix");
    rule.put("rules", columnArray);
    dynamicColumn.put("hbase_column", rule);
    dynamicColumn.put("hbase_output", "/hbasebulkwriter");
    dynamicColumn.put("hbase_config", "hbase-site.xml");
    System.out.println(dynamicColumn.toString());
    
    JSONObject jsonConf = new JSONObject();
    jsonConf.put("dynamiccolumn", dynamicColumn);
    HBaseBulker bulker = new HBaseBulker();
    try {
      bulker.loadDynamicColumnConfig(com.alibaba.datax.common.util.Configuration.from(jsonConf.toString()));
      fail();    
    } catch (DataXException e) {
      assertEquals("Code:[10001], Description:[config missing].  - Missing config hdfs_config.", e.getMessage());
    }
  }
}
