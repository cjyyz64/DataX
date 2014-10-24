package com.alibaba.datax.plugin.writer.otswriter.sample;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.otswriter.OtsWriterMasterProxy;
import com.alibaba.datax.plugin.writer.otswriter.common.BaseTest;
import com.alibaba.datax.plugin.writer.otswriter.common.Utils;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.aliyun.openservices.ots.OTSClient;
import com.aliyun.openservices.ots.model.PrimaryKeyType;
import com.aliyun.openservices.ots.model.TableMeta;

public class TestWriter {
    private String tableName = "ots_reader_test_writer";
    private BaseTest base = new BaseTest(tableName);
    private Configuration p = Utils.loadConf();
    
    public void setBeforeClass() {
        OTSClient ots = new OTSClient(p.getString("endpoint"), p.getString("accessid"), p.getString("accesskey"), p.getString("instance-name"));
        
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("Uid", PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn("Pid", PrimaryKeyType.INTEGER);
        tableMeta.addPrimaryKeyColumn("Mid", PrimaryKeyType.INTEGER);
        tableMeta.addPrimaryKeyColumn("Gid", PrimaryKeyType.STRING);
        try {
            Utils.createTable(ots, tableName, tableMeta);
        } catch (Exception e) {
            e.printStackTrace();
        } 
    }
    
    public void test() throws Exception {
        
        //setBeforeClass();
        
        OtsWriterMasterProxy proxy = new OtsWriterMasterProxy();
        String json = 
                         "{\"accessId\":\""+ p.getString("accessid") +"\","
                        + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                        + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                        + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                        + "\"table\":\""+ tableName +"\","
                        + "\"primaryKey\":[],"
                        + "\"column\":[],"
                        + "\"conflict\":\"\","
                        + "}";
        Configuration p = Configuration.from(json);
        proxy.init(p);
        OTSConf conf = proxy.getOTSConf();
        System.out.println(conf.getEndpoint());
        base.getOts().shutdown();
    }

    public static void main(String[] args) throws Exception {
        TestWriter t = new TestWriter();
        t.test();
    }

}
