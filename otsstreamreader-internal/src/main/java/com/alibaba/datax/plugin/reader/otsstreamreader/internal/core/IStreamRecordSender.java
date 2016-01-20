package com.alibaba.datax.plugin.reader.otsstreamreader.internal.core;

import com.aliyun.openservices.ots.internal.model.StreamRecord;

public interface IStreamRecordSender {

    void sendToDatax(StreamRecord streamRecord);

}
