package com.alibaba.datax.plugin.reader.otsstreamreader.internal.core;

import com.alibaba.datax.plugin.reader.otsstreamreader.internal.common.AssertHelper;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.common.RecordSenderForTest;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.common.Utils;
import com.aliyun.openservices.ots.internal.model.*;
import org.junit.Test;

public class TestOTSStreamRecordSender {

    @Test
    public void testSendToDatax() {
        RecordSenderForTest recordSenderForTest = new RecordSenderForTest();
        for (int isExportSeq = 0; isExportSeq < 2; isExportSeq++) {
            OTSStreamRecordSender recordSender = new OTSStreamRecordSender(recordSenderForTest, "shardId", isExportSeq == 1);
            for (int pkNum = 1; pkNum < 5; pkNum++) {
                for (int colNum = 0; colNum < 1000; colNum++) {
                    StreamRecord streamRecord = new StreamRecord();
                    streamRecord.setRecordType(Utils.getRandomRecordType());
                    streamRecord.setPrimaryKey(Utils.getPrimaryKey(pkNum));
                    streamRecord.setColumns(Utils.getRecordColumns(colNum, streamRecord.getRecordType()));
                    streamRecord.setSequenceInfo(new RecordSequenceInfo(0, System.currentTimeMillis(), 0));
                    recordSender.sendToDatax(streamRecord);
                    AssertHelper.check(recordSenderForTest.getRecords(), streamRecord, isExportSeq == 1, "shardId");
                    recordSenderForTest.flush();
                }
            }
        }
    }
}
