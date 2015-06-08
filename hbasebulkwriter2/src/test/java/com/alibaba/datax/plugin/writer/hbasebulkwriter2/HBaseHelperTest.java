package com.alibaba.datax.plugin.writer.hbasebulkwriter2;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.plugin.writer.hbasebulkwriter2.column.FixedHBaseColumn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class HBaseHelperTest {

  private static final Logger LOG = LoggerFactory
          .getLogger(HBaseHelperTest.class);
  //private static final String tmpHBaseSite = HBaseTestUtils.tmpHBaseSite;
  private static final String tmpOutputDir = HBaseTestUtils.tmpOutputDir;
  //private static final String targetTable= HBaseTestUtils.targetTable;
  private static final Configuration conf = HBaseTestUtils.conf;
  private static final ArrayList<Column> line = HBaseTestUtils.line;
  private static final List<FixedHBaseColumn> rowkeyList = HBaseTestUtils.rowkeyList;
  private static final List<FixedHBaseColumn> columnList = HBaseTestUtils.columnList;
  //private static final FileSystem fs = HBaseTestUtils.fs;

  @Before
  public void setUp() {}

  @After
  public void tearDown() {
    HBaseHelper.clearTmpOutputDir(conf, tmpOutputDir);
  }

  @Test
  public void testCreateRecordWriter() {
    try {
      HBaseHelper.checkTmpOutputDir(conf, tmpOutputDir,1);
      HFileWriter writer = HBaseHelper.createHFileWriter(new HTable(conf, ""), conf, tmpOutputDir);
      assertNotNull(writer);

      byte[] rowkey = FixedHBaseColumn.toRow(line, -1, rowkeyList);
      KeyValue[] kvs = FixedHBaseColumn.toKVs(line, rowkey, columnList, "utf-8", System.currentTimeMillis(),HBaseConsts.NULL_MODE_DEFAULT);
      for (KeyValue kv : kvs) {
        writer.write(null, kv);
      }
      writer.close();
    } catch (Exception e) {
      LOG.error("Create RecordWriter Error.", e);
      assertTrue(false);
    }
  }

  @Test
  public void testCheckTmpOutputDir() {
      HBaseHelper.checkTmpOutputDir(conf, tmpOutputDir,1);

  }

    @Test
    public void testGetConf() throws Exception {
        String configstr ="{\"dfs.client.failover.proxy.provider.hdfscluster-perf\":\"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider\",\"dfs.ha.namenodes.hdfscluster-perf\":\"nn1,nn2\",\"dfs.namenode.rpc-address.hdfscluster-perf.nn1\":\"10.101.88.59:8020\",\"dfs.namenode.rpc-address.hdfscluster-perf.nn2\":\"10.101.85.53:8020\",\"dfs.nameservices\":\"hdfscluster-perf\",\"fs.defaultFS\":\"hdfs://hdfscluster-perf\",\"hbase.zookeeper.quorum\":\"10.101.88.59,10.101.85.53,10.101.87.52\",\"zookeeper.znode.parent\":\"/hbase-datax3\"}";
        Configuration conf=HBaseHelper.getConfiguration(null,null,configstr,null);
        Assert.assertEquals(conf.get("fs.defaultFS"),"hdfs://hdfscluster-perf");
        Assert.assertEquals(conf.get("fs.default.name"),"hdfs://hdfscluster-perf");
    }


}
