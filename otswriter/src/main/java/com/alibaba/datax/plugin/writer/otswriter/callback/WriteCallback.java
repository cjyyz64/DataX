package com.alibaba.datax.plugin.writer.otswriter.callback;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.plugin.writer.otswriter.OtsWriterSlaveProxy;
import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.OTSException;
import com.aliyun.openservices.ots.internal.OTSCallback;
import com.aliyun.openservices.ots.model.BatchWriteRowRequest;
import com.aliyun.openservices.ots.model.BatchWriteRowResult;
import com.aliyun.openservices.ots.model.OTSContext;

public class WriteCallback implements OTSCallback<BatchWriteRowRequest, BatchWriteRowResult>{
    
    private static final Logger LOG = LoggerFactory.getLogger(OtsWriterSlaveProxy.class);
    
    private Lock lock = null;
    private Condition condition = null;

    public WriteCallback(
            Lock lock,
            Condition condition) {

        this.lock = lock;
        this.condition = condition;

    }

    @Override
    public void onCompleted(
            OTSContext<BatchWriteRowRequest, BatchWriteRowResult> otsContext) {
        LOG.debug("onCompleted begin.");

        lock.lock();
        condition.signal();
        lock.unlock();
        
        LOG.debug("onCompleted end.");
    }

    @Override
    public void onFailed(
            OTSContext<BatchWriteRowRequest, BatchWriteRowResult> otsContext,
            OTSException ex) {
        LOG.debug("onFailed.OTSException begin.");
        LOG.error(ex.getMessage());
        
        lock.lock();
        condition.signal();
        lock.unlock();
        
        LOG.debug("onFailed.OTSException end.");
    }

    @Override
    public void onFailed(
            OTSContext<BatchWriteRowRequest, BatchWriteRowResult> otsContext,
            ClientException ex) {
        LOG.debug("onFailed.ClientException begin.");
        LOG.warn(ex.getMessage());
        
        lock.lock();
        condition.signal();
        lock.unlock();
        
        LOG.debug("onFailed.ClientException end.");
    }

}
