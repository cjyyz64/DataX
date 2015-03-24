package com.alibaba.datax.plugin.writer.otswriter.callable;

import java.util.concurrent.Callable;

import com.aliyun.openservices.ots.internal.OTS;
import com.aliyun.openservices.ots.internal.model.BatchWriteRowRequest;
import com.aliyun.openservices.ots.internal.model.BatchWriteRowResult;

public class BatchWriteRowCallable implements Callable<BatchWriteRowResult>{
    
    private OTS ots = null;
    private BatchWriteRowRequest batchWriteRowRequest = null;

    public BatchWriteRowCallable(OTS ots,  BatchWriteRowRequest batchWriteRowRequest) {
        this.ots = ots;
        this.batchWriteRowRequest = batchWriteRowRequest;

    }
    
    @Override
    public BatchWriteRowResult call() throws Exception {
        return ots.batchWriteRow(batchWriteRowRequest);
    }

}