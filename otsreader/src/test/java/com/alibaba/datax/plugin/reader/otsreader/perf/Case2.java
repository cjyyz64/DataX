package com.alibaba.datax.plugin.reader.otsreader.perf;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.OtsReaderMasterProxy;
import com.alibaba.datax.plugin.reader.otsreader.OtsReaderSlaveProxy;
import com.alibaba.datax.plugin.reader.otsreader.common.ReaderConf;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.test.simulator.util.RecordSenderForTest;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;


 /**
  * OTS -> SDK -> Reader
  * 直接调用Reader
  * @author redchen
  *
  */

class NewThread extends Thread {
    PrintWriter p = new PrintWriter(System.out);
    RecordSender sender = new RecordSenderForTest(p, null);
    OtsReaderSlaveProxy slave = new OtsReaderSlaveProxy();
    Configuration c = null;
    
    public NewThread(Configuration c) {
        this.c = c;
    }
    
    @Override
    public void run() {
        try {
            slave.read(sender, c);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
}

public class Case2 {

    private String endpoint = "http://10.101.16.13";
    private String accessId = "OTSMultiUser001_accessid";
    private String accesskey = "OTSMultiUser001_accesskey";
    private String instanceName = "TestInstance001";
    private String tableName = "ots_datax_perf";
    
    public void run(int threadNum) throws Exception {
        OTSConf conf = new OTSConf();
        
        conf.setEndpoint(endpoint);
        conf.setAccessId(accessId);
        conf.setAccessKey(accesskey);
        conf.setInstanceName(instanceName);
        conf.setTableName(tableName);
        
        List<OTSColumn> columns = new ArrayList<OTSColumn>();
        columns.add(OTSColumn.fromNormalColumn("userid"));
        columns.add(OTSColumn.fromNormalColumn("groupid"));
        
        columns.add(OTSColumn.fromNormalColumn("col_string_1"));
        columns.add(OTSColumn.fromNormalColumn("col_string_2"));
        columns.add(OTSColumn.fromNormalColumn("col_string_3"));
        columns.add(OTSColumn.fromNormalColumn("col_string_4"));
        columns.add(OTSColumn.fromNormalColumn("col_string_5"));
        columns.add(OTSColumn.fromNormalColumn("col_string_6"));
        columns.add(OTSColumn.fromNormalColumn("col_string_7"));
        columns.add(OTSColumn.fromNormalColumn("col_string_8"));
        
        columns.add(OTSColumn.fromNormalColumn("col_string_N1"));
        columns.add(OTSColumn.fromNormalColumn("col_string_N2"));
        columns.add(OTSColumn.fromNormalColumn("col_string_N3"));
        columns.add(OTSColumn.fromNormalColumn("col_string_N4"));
        columns.add(OTSColumn.fromNormalColumn("col_string_N5"));
        columns.add(OTSColumn.fromNormalColumn("col_string_N6"));
        
        conf.setColumns(columns);
        
        List<PrimaryKeyValue> rangeBegin = new ArrayList<PrimaryKeyValue>();
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        rangeBegin.add(PrimaryKeyValue.INF_MIN);
        
        List<PrimaryKeyValue> rangeEnd = new ArrayList<PrimaryKeyValue>();
        rangeEnd.add(PrimaryKeyValue.INF_MAX);
        rangeEnd.add(PrimaryKeyValue.INF_MAX);

        conf.setRangeBegin(rangeBegin);
        conf.setRangeEnd(rangeEnd);
        
        ReaderConf readerConf = new ReaderConf();
        readerConf.setConf(conf);

        Configuration p = Configuration.from(readerConf.toString());
        OtsReaderMasterProxy master = new OtsReaderMasterProxy();
        
        master.init(p);
        List<Configuration> cs = master.split(threadNum);
        
        for (Configuration c: cs) {
            NewThread n = new NewThread(c);
            n.start();
        }
    }

    public static void main(String[] args) throws Exception {
        for (String s: args) {
            System.out.println(s);
        }
        int threadNum = 1;
        if (args.length >= 1) {
            threadNum = Integer.parseInt(args[0]);
        }
        Case2 sdk = new Case2();
        sdk.run(threadNum);
    }

}
