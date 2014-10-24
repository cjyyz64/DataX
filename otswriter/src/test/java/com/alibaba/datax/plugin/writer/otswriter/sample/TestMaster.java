package com.alibaba.datax.plugin.writer.otswriter.sample;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.otswriter.OtsWriterMasterProxy;
import com.alibaba.datax.plugin.writer.otswriter.common.BaseTest;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConst;
import com.alibaba.datax.plugin.writer.otswriter.utils.GsonParser;
import com.aliyun.openservices.ots.model.PrimaryKeyType;

public class TestMaster {
    
    private static String tableName = "ots_writer_test_master";
    private static BaseTest base = new BaseTest(tableName);
    
    public static void prepare() {
        List<PrimaryKeyType> pk = new ArrayList<PrimaryKeyType>();
        pk.add(PrimaryKeyType.STRING);
        
        base.prepareData(pk, -100, 0, 0.5);
    }

    public static Configuration loadConf() {
        String path = "src/test/resources/sample.json";
        InputStream f;
        try {
            f = new FileInputStream(path);
            Configuration p = Configuration.from(f);
            return p;
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    } 

    public static void main(String[] args) throws Exception {
        TestMaster.prepare();
        
        Configuration configuration = TestMaster.loadConf();

        OtsWriterMasterProxy master = new OtsWriterMasterProxy();
        
        master.init(configuration);
        
        master.close();
        
        List<Configuration> conf = master.split(1);
        for (Configuration c : conf) {
            OTSConf con = GsonParser.jsonToConf(c.getString(OTSConst.OTS_CONF));
            System.out.println(con.getEndpoint());
        }
    }

}
