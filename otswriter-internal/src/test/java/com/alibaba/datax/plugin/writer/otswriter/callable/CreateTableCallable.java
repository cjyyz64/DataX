package com.alibaba.datax.plugin.writer.otswriter.callable;

import java.util.concurrent.Callable;

import com.aliyun.openservices.ots.internal.OTSClient;
import com.aliyun.openservices.ots.internal.model.CreateTableRequest;
import com.aliyun.openservices.ots.internal.model.CreateTableResult;

public class CreateTableCallable implements Callable<CreateTableResult>{

    private OTSClient ots = null;
    private CreateTableRequest createTableRequest = null;
    
    public CreateTableCallable(OTSClient ots, CreateTableRequest createTableRequest) {
        this.ots = ots;
        this.createTableRequest = createTableRequest;
    }
    
    @Override
    public CreateTableResult call() throws Exception {
        return ots.createTable(createTableRequest);
    }
}
