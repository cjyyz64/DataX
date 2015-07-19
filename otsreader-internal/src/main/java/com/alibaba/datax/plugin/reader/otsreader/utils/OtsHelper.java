package com.alibaba.datax.plugin.reader.otsreader.utils;

import com.alibaba.datax.plugin.reader.otsreader.callable.GetRangeCallable;
import com.alibaba.datax.plugin.reader.otsreader.callable.GetTableMetaCallable;
import com.alibaba.datax.plugin.reader.otsreader.model.DefaultNoRetry;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.aliyun.openservices.ots.internal.ClientConfiguration;
import com.aliyun.openservices.ots.internal.OTS;
import com.aliyun.openservices.ots.internal.OTSClient;
import com.aliyun.openservices.ots.internal.model.GetRangeResult;
import com.aliyun.openservices.ots.internal.model.RangeRowQueryCriteria;
import com.aliyun.openservices.ots.internal.model.TableMeta;

public class OtsHelper {
    
    public static OTS getOTSInstance(OTSConf conf) {
        ClientConfiguration clientConfigure = new ClientConfiguration();
        clientConfigure.setIoThreadCount(conf.getIoThreadCount());
        clientConfigure.setMaxConnections(conf.getMaxConnectCount());
        clientConfigure.setSocketTimeoutInMillisecond(conf.getSocketTimeoutInMillisecond());
        clientConfigure.setConnectionTimeoutInMillisecond(conf.getConnectTimeoutInMillisecond());
        clientConfigure.setRetryStrategy(new DefaultNoRetry());

        OTS ots = new OTSClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstanceName(),
                clientConfigure);
        return ots;
    }
    
    public static TableMeta getTableMeta(OTS ots, String tableName, int retry, int sleepInMillisecond) throws Exception {
        return RetryHelper.executeWithRetry(
                new GetTableMetaCallable(ots, tableName),
                retry,
                sleepInMillisecond
                );
    }
    
    public static GetRangeResult getRange(OTS ots, RangeRowQueryCriteria rangeRowQueryCriteria, int retry, int sleepInMillisecond) throws Exception {
        return RetryHelper.executeWithRetry(
                new GetRangeCallable(ots, rangeRowQueryCriteria),
                retry,
                sleepInMillisecond
                );
    }
}
