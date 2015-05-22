package com.alibaba.datax.plugin.writer.hbasebulkwriter2;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.alibaba.datax.common.util.Configuration;


public class ConfigurationBuilders {
  /**
   * {
   *  "hbase_rowkey" : [
   *    {"index" : 0 , "htype" : "long"},
   *    {"index" : 1 , "htype" : "string"},
   *    {"index" : -1 , "htype" : "string", "constant" : "0"}
   *  ]
   * }
   *
   */
  public static class RowkeySchemaBuilder {
    private JSONObject conf;
    
    public RowkeySchemaBuilder() {
      this.conf = new JSONObject();
    }
    public void addColumn(int index,String type) throws JSONException {
      JSONObject column = new JSONObject();
      column.put("index", index);
      column.put("htype", type);
      conf.append("hbase_rowkey", column);
    }
    public void addConstant(String type, String value) throws JSONException {
      JSONObject column = new JSONObject();
      column.put("index", -1);
      column.put("htype", type);
      column.put("constant", value);
      conf.append("hbase_rowkey", column);
    }
    
    protected JSONObject getJSON() {
      return this.conf;
    }
    
    public Configuration build() {
      return Configuration.from(this.conf.toString());
    }
  }
  
  /**
   * {
   *  "hbase_column" : [
   *    {"index" : 3 , "hname" : "cf:col1", "htype" : "long"},
   *    {"index" : 4 , "hname" : "cf:col2", "htype" : "string"}
   *  ]
   * }
   *
   */
  public static class FixedColumnSchemaBuilder {
    private JSONObject conf;
    
    public FixedColumnSchemaBuilder() {
      this.conf = new JSONObject();
    }
    public void addColumn(int index, String type, String family, String qualifier) throws JSONException {
      JSONObject column = new JSONObject();
      column.put("index", index);
      column.put("hname", family + ":" + qualifier);
      column.put("htype", type);
      this.conf.append("hbase_column", column);
    }
    
    protected JSONObject getJSON() {
      return this.conf;
    }
    
    public Configuration build() {
      return Configuration.from(this.conf.toString());
    }
  }
  
  public static class FixedSchemaBuilder {
    private JSONObject fixed;
    private JSONObject optional;
    
    public FixedSchemaBuilder() {
      this.fixed = new JSONObject();
      this.optional = new JSONObject();
    }
    
    public void addTable(String tableName) throws JSONException {
      this.fixed.put("hbase_table", tableName);
    }
    
    public void addRowkeySchema(JSONArray rowkeyShema) throws JSONException {
      this.fixed.put("hbase_rowkey", rowkeyShema);
    }
    
    public void addColumnSchema(JSONArray columnScheam) throws JSONException {
      this.fixed.put("hbase_column", columnScheam);
    }
    
    public void addHbaseConfig(String path) throws JSONException {
      this.fixed.put("hbase_conifg", path);
    }
    
    public void addHdfsConfig(String path) throws JSONException {
      this.fixed.put("hdfs_config", path);
    }
    
    public void addHdfsOutput(String path) throws JSONException {
      this.fixed.put("hbase_output", path);
    }
    
    public void addClusterId(String id) throws JSONException {
      this.fixed.put("cluster_id", id);
    }
    
    public void addNullMode(String mode) throws JSONException {
      this.optional.put("null_mode", mode);
    }
    
    public void addBucketNum(int num) throws JSONException {
      this.optional.put("bucket_num", num);
    }
    
    public void addStartTs(long ts) throws JSONException {
      this.optional.put("start_ts", ts);
    }
    
    public void addTimeCol(int index) throws JSONException {
      this.optional.put("time_col", index);
    }
    
    public void addTruncateTable(String value) throws JSONException {
      this.optional.put("truncate_table", value);
    }
    
    public void addEncoding(String encoding) throws JSONException {
      this.optional.put("encoding", encoding);
    }
    
    public Configuration build() throws JSONException {
      JSONObject conf = new JSONObject();
      this.fixed.put("optional", this.optional);
      conf.put("fixedcolumn", this.fixed);
      return Configuration.from(conf.toString());
    }
  }
  
  /**
   * {
   *  "type" : "prefix",
   *  "rules" : [
   *    {"pattern" : "cf:col_", "htype" : "long"},
   *    {"pattern" : "cf:mal_", "htype" : "string}
   *  ]
   * }
   *
   */
  public static class DynamicColumnSchemaBuiler {
    private JSONObject conf;
    
    public DynamicColumnSchemaBuiler() throws JSONException {
      this.conf = new JSONObject();
      this.conf.put("type", "prefix");
    }
    
    public void addColumn(String type, String family, String prefix) throws JSONException {
      JSONObject rule = new JSONObject();
      rule.put("pattern", family + ":" + prefix);
      rule.put("htype", type);
      this.conf.append("rules", rule);
    }
    
    protected JSONObject getJSON() {
      return this.conf;
    }
    
    public Configuration build() throws JSONException {
      return Configuration.from(this.conf.toString());
    }
  }
  
  public static void main(String[] args) throws JSONException {
    DynamicColumnSchemaBuiler dynamicColumnBuilder = new DynamicColumnSchemaBuiler();
    dynamicColumnBuilder.addColumn("long", "cf", "col");
    dynamicColumnBuilder.addColumn("string", "xb", "ll");
    Configuration dynamicColumnConf = dynamicColumnBuilder.build();
    System.out.println(dynamicColumnConf);
    
    DynamicSchemaBuilder dynamicSchemaBuilder = new DynamicSchemaBuilder();
    dynamicSchemaBuilder.addTable("t1");
    dynamicSchemaBuilder.addRowkeyType("string");
    dynamicSchemaBuilder.addColumnSchema(dynamicColumnBuilder.getJSON());
    dynamicSchemaBuilder.addHbaseConfig("/home/taobao/hbase-site.xml");
    dynamicSchemaBuilder.addHdfsConfig("/home/taobao/hdfs-site.xml");
    dynamicSchemaBuilder.addHdfsOutput("hdfs://1234");
    dynamicSchemaBuilder.addClusterId("23");
    dynamicSchemaBuilder.addTruncateTable("true");
    dynamicSchemaBuilder.addEncoding("GBK");
    System.out.println(dynamicSchemaBuilder.build());
    
    FixedColumnSchemaBuilder fixedColumnBuilder = new FixedColumnSchemaBuilder();
    fixedColumnBuilder.addColumn(3, "long", "cf", "col1");
    fixedColumnBuilder.addColumn(4, "string", "cf", "col2");
    Configuration fixedColumnConf = fixedColumnBuilder.build();
    System.out.println(fixedColumnConf);
    
    RowkeySchemaBuilder rowkey = new RowkeySchemaBuilder();
    rowkey.addColumn(0, "long");
    rowkey.addColumn(1, "string");
    rowkey.addConstant("byte", "0");
    Configuration rowkeyConf = rowkey.build();
    System.out.println(rowkeyConf);
    
    FixedSchemaBuilder fixedSchemaBuilder = new FixedSchemaBuilder();
    fixedSchemaBuilder.addTable("t1");
    fixedSchemaBuilder.addRowkeySchema(rowkey.getJSON().getJSONArray("hbase_rowkey"));
    fixedSchemaBuilder.addColumnSchema(fixedColumnBuilder.getJSON().getJSONArray("hbase_column"));
    fixedSchemaBuilder.addHbaseConfig("/home/taobao/hbase-site.xml");
    fixedSchemaBuilder.addHdfsConfig("/home/taobao/hdfs-site.xml");
    fixedSchemaBuilder.addHdfsOutput("hdfs://1234");
    fixedSchemaBuilder.addClusterId("23");
    fixedSchemaBuilder.addNullMode("DELETE");
    fixedSchemaBuilder.addBucketNum(4);
    fixedSchemaBuilder.addStartTs(34324235425425l);
    fixedSchemaBuilder.addTimeCol(1);
    fixedSchemaBuilder.addTruncateTable("true");
    fixedSchemaBuilder.addEncoding("GBK");
    
    System.out.println(fixedSchemaBuilder.build());
  }
  
  public static class DynamicSchemaBuilder {
    private JSONObject dynamic;
    private JSONObject optional;    
    public DynamicSchemaBuilder() {
      this.dynamic = new JSONObject();
      this.optional = new JSONObject();
    }
    
    public void addTable(String tableName) throws JSONException {
      this.dynamic.put("hbase_table", tableName);
    }
    
    public void addRowkeyType(String type) throws JSONException {
      this.dynamic.put("rowkey_type", type);
    }
    
    public void addColumnSchema(JSONObject columnScheam) throws JSONException {
      this.dynamic.put("hbase_column", columnScheam);
    }
    
    public void addHbaseConfig(String path) throws JSONException {
      this.dynamic.put("hbase_conifg", path);
    }
    
    public void addHdfsConfig(String path) throws JSONException {
      this.dynamic.put("hdfs_config", path);
    }
    
    public void addHdfsOutput(String path) throws JSONException {
      this.dynamic.put("hbase_output", path);
    }
    
    public void addClusterId(String id) throws JSONException {
      this.dynamic.put("cluster_id", id);
    }
    
    public void addTruncateTable(String value) throws JSONException {
      this.optional.put("truncate_table", value);
    }
    
    public void addEncoding(String encoding) throws JSONException {
      this.optional.put("encoding", encoding);
    }
    
    public Configuration build() throws JSONException {
      JSONObject conf = new JSONObject();
      this.dynamic.put("optional", this.optional);
      conf.put("dynamiccolumn", this.dynamic);
      return Configuration.from(conf.toString());
    }
  }
}
