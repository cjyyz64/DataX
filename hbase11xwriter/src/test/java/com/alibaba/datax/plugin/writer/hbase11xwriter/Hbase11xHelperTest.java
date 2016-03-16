package com.alibaba.datax.plugin.writer.hbase11xwriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by shf on 16/4/6.
 */
public class Hbase11xHelperTest {

    @Test
    public void testGetHbaseConnection() throws Exception {
        //hbaseConfig为blank
        org.apache.hadoop.hbase.client.Connection hConnection;
        String hbaseConfig = " ";
        try {
            hConnection= Hbase11xHelper.getHbaseConnection(hbaseConfig);
        } catch (Exception e) {
            assertTrue(e instanceof DataXException);
            assertTrue(e.getMessage().contains("读 Hbase 时需要配置hbaseConfig，其内容为 Hbase 连接信息，请联系 Hbase PE 获取该信息."));
        }

        //hbaseConfig为空
        hbaseConfig = "{}";
        try {
            hConnection= Hbase11xHelper.getHbaseConnection(hbaseConfig);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("hbaseConfig不能为空Map结构!"));
        }
        //hbaseConfig非法
        hbaseConfig = "{@@@@@@@$$$$$}";
        try {
            hConnection= Hbase11xHelper.getHbaseConnection(hbaseConfig);
        } catch (Exception e) {
            assertTrue(e instanceof DataXException);
            assertTrue(e.getMessage().contains("获取Hbase连接时出错"));
        }

        //hbaseConfig正常
        hbaseConfig= "{\"hbase.rootdir\":\"hdfs://10.101.85.161:9000/hbase\"," +
                "\"hbase.cluster.distributed\":\"true\"," +
                "\"hbase.zookeeper.quorum\":\"v101085161.sqa.zmf\"}";
        hConnection= Hbase11xHelper.getHbaseConnection(hbaseConfig);
        assertNotNull(hConnection);

    }

    @Test
    public void testGetTable() throws Exception {
        //table正常
        com.alibaba.datax.common.util.Configuration configuration = Configuration.newDefault();
        String hbaseConfig= "{\"hbase.rootdir\":\"hdfs://10.101.85.161:9000/hbase\"," +
                "\"hbase.cluster.distributed\":\"true\"," +
                "\"hbase.zookeeper.quorum\":\"v101085161.sqa.zmf\"}";
        configuration.set("hbaseConfig",hbaseConfig);
        configuration.set("table","users");

        org.apache.hadoop.hbase.client.Table hTable = null;
        hTable = Hbase11xHelper.getTable(configuration);
        assertNotNull(hTable);

        //table 不存在
        configuration.set("table","usersXXXXXXXXXXXXX");
        try {
            hTable = Hbase11xHelper.getTable(configuration);
        } catch (Exception e) {
            assertTrue(e instanceof DataXException);
            assertTrue(e.getMessage().contains("不存在, 请检查您的配置 或者 联系 Hbase 管理员"));
        }

        configuration.set("table","disable");
        try {
            hTable = Hbase11xHelper.getTable(configuration);
        } catch (Exception e) {
            assertTrue(e instanceof DataXException);
            assertTrue(e.getMessage().contains("is disabled, 请检查您的配置 或者 联系 Hbase 管理员."));
        }
    }

}