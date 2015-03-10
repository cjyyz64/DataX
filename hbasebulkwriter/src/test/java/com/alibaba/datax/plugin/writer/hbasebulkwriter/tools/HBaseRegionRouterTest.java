package com.alibaba.datax.plugin.writer.hbasebulkwriter.tools;

import com.alibaba.datax.plugin.writer.hbasebulkwriter.HBaseTestUtils;
import com.alibaba.datax.plugin.writer.hbasebulkwriter.tools.HBaseRegionRouter;
import com.aliyun.openservices.odps.ODPSConnection;
import com.aliyun.openservices.odps.Project;
import com.aliyun.openservices.odps.helper.InstanceRunner;
import com.aliyun.openservices.odps.jobs.*;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONException;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class HBaseRegionRouterTest {

  private static final String tmpHBaseSite = HBaseTestUtils.tmpHBaseSite;
  private static final String startKeyStr = "0000000000000000ffffffffffffffff";
  private static final String endKeyStr = "0000000000000010ffffffffffffffff";
  private static final String endpoint = HBaseTestUtils.endpoint;
  private static final String accessId = HBaseTestUtils.accessId;
  private static final String accessKey = HBaseTestUtils.accessKey;
  private static final String projectName = HBaseTestUtils.projectName;
  private static final String targetTable = HBaseTestUtils.targetTable;
  private static final String sourceTable = HBaseTestUtils.sourceTable;
  private static final String pyPath = HBaseTestUtils.pyPath;
  private static final String jarPath = HBaseTestUtils.jarPath;
  private static final Logger LOG = LoggerFactory
      .getLogger(HBaseRegionRouterTest.class);
  private static Configuration conf = HBaseTestUtils.conf;
  private static HBaseAdmin hadmin;
  private static ODPSConnection odpsConn;
  private static Project project;

  //@BeforeClass
  public static void setUpClass() {
    try {
      initTargetTable();
      initSourceTable();
    } catch (Exception e) {
      LOG.error("Setup Error", e);
      assertTrue(false);
    }
  }

  //@AfterClass
  public static void tearDownClass() {
    try {
      // FIXME
      //destroyTargetTable();
      //destroySourceTable();
    } catch (Exception e) {
      LOG.error("Teardown Error", e);
      assertTrue(false);
    }
  }

  public static void initTargetTable() throws Exception {
    hadmin = new HBaseAdmin(conf);

    if (hadmin.tableExists(targetTable)) {
      return;
    }
    HTableDescriptor desc = new HTableDescriptor(targetTable);
    desc.addFamily(new HColumnDescriptor("cf"));
    hadmin.createTable(desc, Hex.decodeHex(startKeyStr.toCharArray()),
            Hex.decodeHex(endKeyStr.toCharArray()), 10);
  }

  public static void destroyTargetTable() throws Exception {
    hadmin.disableTable(targetTable);
    hadmin.deleteTable(targetTable);
  }

  public static void initSourceTable() throws Exception {
    odpsConn = new ODPSConnection(endpoint, accessId, accessKey);
    project = new Project(odpsConn, projectName);
    project.load();

    try {
      runOdpsSql(String.format(
          "CREATE TABLE %s(part bigint, name string, value bigint);",
          sourceTable));
    } catch (Exception e) {
      return;
    }

    String sql =
        "INSERT INTO TABLE %s SELECT '%d', '%s', '%d' from (select count(0) from %s) %s;";
    for (int i = 0, l = 5; i < l; i++) {
      runOdpsSql(String.format(sql, sourceTable, i, sourceTable,
          0xffffffffffffffffl, sourceTable, sourceTable));
    }
  }

  public static void destroySourceTable() throws Exception {
    runOdpsSql(String.format("DROP TABLE %s;", sourceTable));
  }

  public static String runOdpsSql(String sql) throws Exception {
    Task task = new SqlTask("SqlTask", sql);
    InstanceRunner runner = new InstanceRunner(project, task, null, null);
    String result = runner.waitForCompletion();

    return result;
  }

  public static String runShell(String cmd) throws Exception {
    Process process = Runtime.getRuntime().exec(cmd);
    process.waitFor();
    String output = IOUtils.toString(process.getInputStream());
    return output;
  }

  @Test
  public void testFindRegionNum() throws JSONException {
    HBaseRegionRouter router = new HBaseRegionRouter();
    JSONArray regionsJson =
        HBaseRegionRouter.getRegionsJson(targetTable, tmpHBaseSite, null);
    byte[][] keysArr = router.toKeysArr(regionsJson);
    byte[] rowkey = Bytes.add(Bytes.toBytes(4l), Bytes.toBytes(-1l));
    int index = router.findRegionNum(keysArr, rowkey);
    LOG.info("Index: {}, Rowkey: {}", index, Hex.encodeHexString(rowkey));
    assertEquals(index, 2);
  }

  @Test
  public void testGetRegionsJson() {
    JSONArray regionsJson =
        HBaseRegionRouter.getRegionsJson(targetTable, tmpHBaseSite, null);
    assertEquals(regionsJson.length(), 10);
  }

  @Test
  public void testToPhoenixStyleRowkey() {
    HBaseRegionRouter router = new HBaseRegionRouter();
    byte[] rowkey = router.toPhoenixStyleRowkey("5", "1","byte","2","byte");
    LOG.warn(Hex.encodeHexString(rowkey));
  }

  @Test
  public void testOdpsHBaseSortPy() {
    String cmd =
        String.format("python %s -s %s -c %s -t %s -g %s -u %s", pyPath, sourceTable,
            "part,value", targetTable, tmpHBaseSite, jarPath);
    LOG.info("OdpsHBaseSortPy Cmd: {}", cmd);

    try {
      String output = runShell(cmd);
      LOG.info("OdpsHBaseSortPy Output: {}", output);
      String result =
          runOdpsSql(String.format(
              "SHOW PARTITIONS t_datax_odps2hbase_table_%s;", sourceTable));
      LOG.info("OdpsHBaseSortPy Result: {}", result);
      assertEquals(result.split("\n").length, 3);
    } catch (Exception e) {
      LOG.error("Run odps_hbase_sort.py Error.", e);
      assertTrue(false);
    }
  }
}
