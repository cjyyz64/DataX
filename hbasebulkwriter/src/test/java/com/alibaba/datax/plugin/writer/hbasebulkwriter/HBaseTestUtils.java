package com.alibaba.datax.plugin.writer.hbasebulkwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.plugin.writer.hbasebulkwriter.HBaseBulker;
import com.alibaba.datax.plugin.writer.hbasebulkwriter.HBaseConsts;
import com.alibaba.datax.plugin.writer.hbasebulkwriter.HBaseLineReceiver;
import com.alibaba.datax.plugin.writer.hbasebulkwriter.column.FixedHBaseColumn;
import com.aliyun.odps.Record;
import com.aliyun.odps.io.*;
import com.aliyun.odps.mapreduce.JobClient;
import com.aliyun.odps.mapreduce.JobConf;
import com.aliyun.odps.mapreduce.MapContext;
import com.aliyun.odps.mapreduce.Mapper;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.datax.plugin.writer.hbasebulkwriter.ConfigurationBuilders.RowkeySchemaBuilder;
import com.alibaba.datax.plugin.writer.hbasebulkwriter.ConfigurationBuilders.FixedColumnSchemaBuilder;

public class HBaseTestUtils {

  public static final String tmpHBaseSite = System.getenv("TEST_HBASE_CONFIG_HOME") + "/hbase-site.xml";
  public static final String tmpHdfsSite = System.getenv("TEST_HBASE_CONFIG_HOME") + "/hdfs-site.xml";
  public static final String endpoint =
      "http://service.odps.aliyun-inc.com/api";
  public static final String accessId = "IZCzCSCDl9SsJeXs";
  public static final String accessKey = "FGR3vdUaRQI7c5bK4JPudfpIVlblG4";
  public static final String projectName = "cdo_zyd_no1";
  public static final String targetTable = "datax_hbasebulkwriter_target";
  public static final String sourceTable = "datax_hbasebulkwriter_source";
  public static final String tmpOutputDir = "/hbasebulkwriter";
  public static final String pyPath = System.getProperty("user.dir")
      + "/datax-hbasebulkwriter/src/main/python/datax_odps_hbase_cdh4_sort.py";
  public static final String jarPath =
      System.getProperty("user.dir")
          + "/datax-hbasebulkwriter/target/datax-odps-hbasebulkwriter-udf-1.0.0-SNAPSHOT.jar";
  public static final Configuration conf = new Configuration();
  public static final ArrayList<Column> line = new ArrayList<Column>();
  private static final Logger LOG = LoggerFactory
      .getLogger(HBaseTestUtils.class);
  static {
    try {
      initFields();
    } catch (JSONException e) {
      e.printStackTrace();
    }
  }
  public static List<FixedHBaseColumn> rowkeyList;
  public static List<FixedHBaseColumn> columnList;
  public static FileSystem fs;

  public static void initFields() throws JSONException {
    conf.addResource(new Path(tmpHBaseSite));
    conf.addResource(new Path(tmpHdfsSite));

    line.add(new StringColumn("1"));
    line.add(new StringColumn("hello"));
    line.add(new StringColumn("-1"));

    RowkeySchemaBuilder rowkeySchemaBuilder = new RowkeySchemaBuilder();
    rowkeySchemaBuilder.addColumn(0, "long");
    rowkeySchemaBuilder.addColumn(1, "string");
    rowkeyList = FixedHBaseColumn.parseRowkeySchema(rowkeySchemaBuilder.build().getListConfiguration("hbase_rowkey"));

    FixedColumnSchemaBuilder fixedColumnSchemaBuilder = new FixedColumnSchemaBuilder();
    fixedColumnSchemaBuilder.addColumn(1, "string", "cf", "name");
    fixedColumnSchemaBuilder.addColumn(2, "long", "cf", "value");
    columnList = FixedHBaseColumn.parseColumnSchema(fixedColumnSchemaBuilder.build().getListConfiguration("hbase_column"));

    try {
      fs = FileSystem.get(conf);
    } catch (IOException e) {
      LOG.error("Create filesystem failed.", fs);
    }
  }

  public static HBaseBulker initWriter(HBaseBulker writer,
      List<FixedHBaseColumn> rowkeyList, List<FixedHBaseColumn> columnList) {
    try {
      writer.init(targetTable, conf, rowkeyList, columnList);
    } catch (Exception e) {
      LOG.error("Init HBaseBulker failed.", e);
    }
    return writer;
  }

  public static void prepareTargetTable(String targetTable, int num, long count) throws IOException {
    HBaseAdmin admin = new HBaseAdmin(HBaseTestUtils.conf);
    //int num = 100;
    //long count = 2000000000;
    long base = 2000000000000000l;
    long delta = count / num;
    byte[][] splitKeys = new byte[num - 1][];
    for (int i = 1; i < num; i++) {
      splitKeys[i - 1] =
          Bytes.add(Bytes.toBytes(base + (delta * i) + ""),
              Bytes.toBytes(base + (delta * i) + ""));
    }
    HTableDescriptor desc = new HTableDescriptor(targetTable);
    desc.addFamily(
            new HColumnDescriptor("cf")
                    .setDataBlockEncoding(DataBlockEncoding.DIFF)
                    .setEncodeOnDisk(true)
                    .setCompressionType(Compression.Algorithm.LZO));
    admin.createTable(desc, splitKeys);
    admin.close();
  }

  public static void prepareSourceTable(String sourceTable, int num, long count) throws IOException {
    JobConf job = new JobConf();
    job.set("count", count / num + "");
    job.setAllowNoInput(true);
    job.setMapperClass(OdpsDataGenerator.class);
    job.setNumMapTasks(num);
    TableOutputFormat.addOutput(new TableInfo(sourceTable), false, job);
    job.setNumReduceTasks(0);
    JobClient.runJob(job);
  }
  
  public static void prepareSourceTableWithNull(String sourceTable, int num, long count) throws IOException {
	    JobConf job = new JobConf();
	    job.set("count", count / num + "");
	    job.setAllowNoInput(true);
	    job.setMapperClass(OdpsDataGeneratorWithNull.class);
	    job.setNumMapTasks(num);
	    TableOutputFormat.addOutput(new TableInfo(sourceTable), false, job);
	    job.setNumReduceTasks(0);
	    JobClient.runJob(job);
	  }

  public static void prepareTestData(String path, long count) throws IOException {
    FileWriter writer = new FileWriter(path);
    long base = 2000000000000000l;
    String type = "000001";
    for (int i = 0; i < count; i++) {
      StringBuilder sb = new StringBuilder();
      long userId = base + i;
      sb.append(userId);
      sb.append(',');
      sb.append(userId);
      sb.append(',');
      sb.append(DigestUtils.md5Hex(userId + "").substring(0, 4));
      sb.append(userId);
      sb.append('#');
      sb.append(userId);
      sb.append(',');
      sb.append(type);
      sb.append(',');
      sb.append(146.6147);
      sb.append(',');
      sb.append(0);
      sb.append('\n');
      writer.append(sb);
    }
    writer.close();
  }

  public static void exportTargetTable(String targetTable, String hbaseSitePath) throws IOException {
    Configuration conf = new Configuration();
    conf.addResource(new Path(hbaseSitePath));
    Scan scan = new Scan();
    scan.setMaxVersions(1);
    scan.setCaching(2000);
    HTable table = new HTable(conf, targetTable);
    ResultScanner scanner = table.getScanner(scan);

  }

  public static void main(String[] args) throws IOException {
    int choice = Integer.parseInt(args[0]);
    if (choice == 0) {
      String sourceTable = args[1];
      int num = Integer.parseInt(args[2]);
      long count = Long.parseLong(args[3]);
      prepareSourceTable(sourceTable, num, count);
    } else if (choice == 1) {
      String targetTable = args[1];
      int num = Integer.parseInt(args[2]);
      long count = Long.parseLong(args[3]);
      prepareTargetTable(targetTable, num, count);
    } else if (choice == 2) {
      String path = args[1];
      long count = Long.parseLong(args[2]);
      prepareTestData(path, count);
    } else if (choice == 3) {
    	String sourceTable = args[1];
        int num = Integer.parseInt(args[2]);
        long count = Long.parseLong(args[3]);
        prepareSourceTableWithNull(sourceTable, num, count);
    }
  }

  @Test
  public void performanceTest() throws JSONException {
    long start = System.currentTimeMillis();
    int l = 16;
    PerformanceThread[] threads = new PerformanceThread[l];
    for (int i = 0; i < l; i++) {
      PerformanceThread thread = new PerformanceThread(i);
      thread.start();
      threads[i] = thread;
    }
    for (PerformanceThread thread : threads) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        LOG.error("Error", e);
      }
    }
    long end = System.currentTimeMillis();
    long spent = end - start;
    long size = 1024l * 1024l * 1024l * 16l;
    LOG.info("Total spent: {}s, Value speed: {} KB", spent / 1000, size / spent);
  }

  public static class OdpsDataGenerator extends Mapper {
    @Override
    public void run(MapContext context) throws IOException,
        InterruptedException {
      long count = Long.parseLong(context.getConfiguration().get("count"));
      int taskId = context.getTaskAttemptID().getTaskId();
      long base = 2000000000000000l + count * taskId;
      for (int i = 0; i < count; i++) {
        Record record = context.createOutputRecord();
        String userId = base + i + "";
        String hash = DigestUtils.md5Hex(userId).substring(0, 4);
        record.set(0, new Text(hash + userId + "#" + userId));
        record.set(1, new Text(userId));
        record.set(2, new Text(userId));
        record.set(3, new Text("000001"));
        record.set(4, new DoubleWritable(146.6147));
        record.set(5, new LongWritable(0));
        context.write(record);
      }
    }
  }
  
  public static class OdpsDataGeneratorWithNull extends Mapper {
	    @Override
	    public void run(MapContext context) throws IOException,
	        InterruptedException {
	      long count = Long.parseLong(context.getConfiguration().get("count"));
	      int taskId = context.getTaskAttemptID().getTaskId();
	      long base = 2000000000000000l + count * taskId;
	      for (int i = 0; i < count; i++) {
	        Record record = context.createOutputRecord();
	        String userId = base + i + "";
	        String hash = DigestUtils.md5Hex(userId).substring(0, 4);
	        record.set(0, new Text(hash + userId + "#" + userId));
	        record.set(1, new Text(userId));
	        record.set(2, new Text(userId));
	        record.set(3, new Text("000001"));
	        record.set(4, new DoubleWritable(146.6147));
	        record.set(5, new LongWritable(0));
	        record.set(6,NullWritable.get());
	        context.write(record);
	      }
	    }
	  }

  public static class PerformanceThread extends Thread {
    final long size = 1024 * 1024 * 1024;
    final long base = 2000000000000000l;
    final ArrayList<Column> line = new ArrayList<Column>();
    final List<FixedHBaseColumn> rowkeyList;
    final List<FixedHBaseColumn> columnList;
    int id = 0;
    HBaseLineReceiver receiver = new HBaseLineReceiver(null) {
      private long index = 0;
      private long count = -1;

      public ArrayList<Column> read() {
        if (count == -1) {
          byte[] rowkey = FixedHBaseColumn.toRow(line, -1, rowkeyList);
          KeyValue[] kvs = FixedHBaseColumn.toKVs(line, rowkey, columnList, "utf-8", System.currentTimeMillis(), HBaseConsts.NULL_MODE_DEFAULT);
          int valueLen = 0;
          for (KeyValue kv : kvs) {
            valueLen += kv.getValueLength();
          }
          count = size / valueLen;
        }

        if (index < count) {
          line.add(0, new StringColumn(base + index + ""));
          line.add(1, new StringColumn(base + index + ""));
          index++;
          return line;
        } else {
          return null;
        }
      }

    };

    public PerformanceThread(int id) throws JSONException {
      RowkeySchemaBuilder rowkeySchemaBuilder = new RowkeySchemaBuilder();
      rowkeySchemaBuilder.addColumn(0, "string");
      rowkeySchemaBuilder.addColumn(1, "string");
      this.rowkeyList = FixedHBaseColumn.parseRowkeySchema(rowkeySchemaBuilder.build().getListConfiguration("hbase_rowkey"));
      FixedColumnSchemaBuilder fixedColumnSchemaBuilder = new FixedColumnSchemaBuilder();
      fixedColumnSchemaBuilder.addColumn(2, "string", "cf", "un");
      fixedColumnSchemaBuilder.addColumn(3, "string", "cf", "on");
      fixedColumnSchemaBuilder.addColumn(4, "string", "cf", "t");
      fixedColumnSchemaBuilder.addColumn(5, "string", "cf", "sub");
      fixedColumnSchemaBuilder.addColumn(6, "double", "cf", "cti");
      fixedColumnSchemaBuilder.addColumn(7, "string", "cf", "fir");
      fixedColumnSchemaBuilder.addColumn(8, "string", "cf", "las");
      this.columnList = FixedHBaseColumn.parseColumnSchema(fixedColumnSchemaBuilder.build().getListConfiguration("hbase_column"));
      
      this.id = id;
      line.add(new StringColumn(base + ""));
      line.add(new StringColumn(base + ""));
      line.add(new StringColumn("黄浩松"));
      line.add(new StringColumn("黄浩松"));
      line.add(new StringColumn("4"));
      line.add(new StringColumn("吃饭时付款"));
      line.add(new StringColumn("0.9"));
      line.add(new StringColumn("2013-12-12 12:12:12"));
      line.add(new StringColumn("2013-12-12 12:12:12"));
    }

    @Override
    public void run() {
      HBaseBulker writer =
          HBaseTestUtils.initWriter(new HBaseBulker(), rowkeyList, columnList);
      long start = System.currentTimeMillis();
      writer.startWrite(receiver);
      long end = System.currentTimeMillis();
      writer.finish();
      long spent = end - start;
      LOG.info("KBytes pre second: {}, Spent time: {}", size / spent, spent);
    }
  }
}
