package com.alibaba.datax.plugin.reader.otsreader.utils;

import com.alibaba.datax.plugin.reader.otsreader.callable.GetTableMetaCallable;

import com.aliyun.openservices.ots.internal.OTSClient;
import com.aliyun.openservices.ots.internal.model.TableMeta;

public class OtsHelper {
    
    public static TableMeta getTableMeta(OTSClient ots, String tableName, int retry, int sleepInMilliSecond) throws Exception {
        return RetryHelper.executeWithRetry(
                new GetTableMetaCallable(ots, tableName),
                retry,
                sleepInMilliSecond
                );
    }
}
