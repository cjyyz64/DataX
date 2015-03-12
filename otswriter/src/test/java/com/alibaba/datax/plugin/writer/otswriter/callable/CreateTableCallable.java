package com.alibaba.datax.plugin.writer.otswriter.callable;

import java.util.concurrent.Callable;

import com.aliyun.openservices.ots.OTSClient;
import com.aliyun.openservices.ots.model.CreateTableRequest;
import com.aliyun.openservices.ots.model.CreateTableResult;

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
