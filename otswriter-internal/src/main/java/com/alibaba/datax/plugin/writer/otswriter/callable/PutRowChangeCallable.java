package com.alibaba.datax.plugin.writer.otswriter.callable;

import java.util.concurrent.Callable;

import com.aliyun.openservices.ots.internal.OTS;
import com.aliyun.openservices.ots.internal.model.PutRowRequest;
import com.aliyun.openservices.ots.internal.model.PutRowResult;

public class PutRowChangeCallable implements Callable<PutRowResult>{
    
    private OTS ots = null;
    private PutRowRequest putRowRequest = null;

    public PutRowChangeCallable(OTS ots, PutRowRequest putRowRequest) {
        this.ots = ots;
        this.putRowRequest = putRowRequest;
    }
    
    @Override
    public PutRowResult call() throws Exception {
        return ots.putRow(putRowRequest);
    }

}