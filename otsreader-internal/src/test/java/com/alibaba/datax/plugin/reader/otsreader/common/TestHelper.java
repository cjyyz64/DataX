package com.alibaba.datax.plugin.reader.otsreader.common;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.codec.binary.Base64;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.Constant;
import com.alibaba.datax.plugin.reader.otsreader.OtsReaderMasterProxy;
import com.alibaba.datax.plugin.reader.otsreader.OtsReaderMultiVersionSlaveProxy;
import com.alibaba.datax.plugin.reader.otsreader.OtsReaderNormalSlaveProxy;
import com.alibaba.datax.plugin.reader.otsreader.OtsReaderSlaveProxy;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSMode;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSRange;
import com.alibaba.datax.plugin.reader.otsreader.utils.GsonParser;
import com.alibaba.datax.test.simulator.util.RecordSenderForTest;
import com.aliyun.openservices.ots.internal.model.TableMeta;

public class TestHelper {
    
    public static String getBase64(String input) {
        return Base64.encodeBase64String(input.getBytes());
    }
    
    public static long test(Configuration param) throws Exception {
        // master
        OtsReaderMasterProxy master = new OtsReaderMasterProxy();
        master.init(param);
        List<Configuration> configs = master.split(1);

        // slave
        List<Record> noteRecordForTest = new LinkedList<Record>();
        RecordSender sender = new RecordSenderForTest(null, noteRecordForTest);
        for (Configuration c : configs) {
            OtsReaderSlaveProxy slave = null;
            OTSConf conf   = GsonParser.jsonToConf((String) c.get(Constant.ConfigKey.CONF));
            OTSRange range = GsonParser.jsonToRange((String) c.get(Constant.ConfigKey.RANGE));
            TableMeta meta = GsonParser.jsonToMeta((String) c.get(Constant.ConfigKey.META));
            
            if (conf.getMode() == OTSMode.MULTI_VERSION) {
                slave = new OtsReaderMultiVersionSlaveProxy();
            } else {
                slave = new OtsReaderNormalSlaveProxy();
            }
            slave.init(conf, range, meta);
            slave.startRead(sender);
            slave.close();
        }
        AssertHelper.assertRecords(master.getOts(), master.getMeta(), master.getConf(), noteRecordForTest);
        master.close();
        
        return noteRecordForTest.size();
    }
}
