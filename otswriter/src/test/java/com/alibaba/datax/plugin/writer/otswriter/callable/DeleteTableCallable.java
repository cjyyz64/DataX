package com.alibaba.datax.plugin.writer.otswriter.callable;

import java.util.concurrent.Callable;

import com.aliyun.openservices.ots.OTSClient;
import com.aliyun.openservices.ots.model.DeleteTableRequest;
import com.aliyun.openservices.ots.model.DeleteTableResult;

public class DeleteTableCallable implements Callable<DeleteTableResult>{

    private OTSClient ots = null;
    private DeleteTableRequest deleteTableRequest = null;
    
    public DeleteTableCallable(OTSClient ots, DeleteTableRequest deleteTableRequest) {
        this.ots = ots;
        this.deleteTableRequest = deleteTableRequest;
    }
    
    @Override
    public DeleteTableResult call() throws Exception {
        return ots.deleteTable(deleteTableRequest);
    }
}
