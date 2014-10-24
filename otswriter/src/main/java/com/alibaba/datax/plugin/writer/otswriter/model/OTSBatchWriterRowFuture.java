package com.alibaba.datax.plugin.writer.otswriter.model;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.plugin.writer.otswriter.callback.WriteCallback;
import com.aliyun.openservices.ots.OTSClientAsync;
import com.aliyun.openservices.ots.model.BatchWriteRowRequest;
import com.aliyun.openservices.ots.model.BatchWriteRowResult;
import com.aliyun.openservices.ots.model.BatchWriteRowResult.RowStatus;
import com.aliyun.openservices.ots.model.OTSFuture;
import com.aliyun.openservices.ots.model.Error;

/**
 * 这个类主要是的对OTSFuture的封装，该类有如下特点：
 * 1.当前的Future运行完成之后，主动通知上层应用
 * 2.保存原始请求和Record，当该请求失败时，上层可以获得原始请求、Record和错误的消息
 * 3.如果上层决定重试该Future，系统会自动剔除已经成功写入的Record，且依据sleep规则，延后发送请求
 */
public class OTSBatchWriterRowFuture {
    private int hadRetryTimes = 0;
    private long retrySleepTime = 0;
    
    private Lock lock = null;
    private Condition condition = null;
    private OTSClientAsync ots = null;
    private OTSConf conf = null;
    private BatchWriteRowRequest req = null;
    private OTSFuture<BatchWriteRowResult> resp = null;
    
    private List<Record> records = new ArrayList<Record>();
    
    // isRunning 表示Future是否在运行状态
    private volatile boolean isRunning = false;
    
    private static final Logger LOG = LoggerFactory.getLogger(OTSBatchWriterRowFuture.class);
    
    /**
     * 
     * @param ots
     * @param conf
     * @param lock
     * @param condition
     * @param records 当前请求正在发送的Record链表
     * @param req
     * @param delaySendMillinSeconds 延时发送请求
     */
    public OTSBatchWriterRowFuture(
            OTSClientAsync ots,
            OTSConf conf, 
            Lock lock,
            Condition condition,
            List<Record> records,
            BatchWriteRowRequest req,
            int delaySendMillinSeconds
            ) {
        // init
        this.ots = ots;
        this.conf = conf;
        this.lock = lock;
        this.condition = condition;
        this.records.addAll(records);
        this.req = req;
        
        this.retrySleepTime = conf.getSleepInMilliSecond();
        this.hadRetryTimes = 0;
        
        // send
        delaySend(req, delaySendMillinSeconds);
    }

    public int getHadRetryTimes() {
        return hadRetryTimes;
    }
    
    public List<Record> getRecords() {
        return records;
    }
    
    public BatchWriteRowRequest getRequest() {
        return req;
    }
    
    public OTSFuture<BatchWriteRowResult> getResultFuture() {
        return resp;
    }
    
    /**
     * 注意：检查OTSBatchWriterRowFuture是否已经完成，而不是请求是否已经完成
     * @return
     */
    public boolean isDone() {
        if (isRunning && resp.isDone()) {
            return true;
        }
        return false;
    }
    
    public class RecordAndError {
        private Record record;
        private Error error;
        
        public RecordAndError(Record record, Error error) {
            this.record = record;
            this.error = error;
        }

        public Record getRecord() {
            return record;
        }

        public Error getError() {
            return error;
        }
    }
    
    public List<RecordAndError> getRecordAndError() {
        List<RecordAndError> errors = new ArrayList<RecordAndError>();
        BatchWriteRowResult result = resp.get();
        
        switch(conf.getOperation()) {
        case PUT_ROW:
            List<RowStatus> putStatus = result.getPutRowStatus(conf.getTableName());
            for (int i = 0; i < putStatus.size(); i++) {
                if (!putStatus.get(i).isSucceed()) {
                    errors.add(new RecordAndError(records.get(i), putStatus.get(i).getError()));
                }
            }
            break;
        case UPDATE_ROW:
            List<RowStatus> updateStatus = result.getUpdateRowStatus(conf.getTableName());
            for (int i = 0; i < updateStatus.size(); i++) {
                if (!updateStatus.get(i).isSucceed()) {
                    errors.add(new RecordAndError(records.get(i), updateStatus.get(i).getError()));
                }
            }
            break;
        }
        return errors;
    }
    
    /**
     * 该类主要是延时发送写请求，并修改Future状态
     */
    class DelaySendRequest extends Thread{
        private BatchWriteRowRequest newRequst = null;
        private long sleepTime = -1;
        
        public DelaySendRequest(BatchWriteRowRequest newRequst, long sleepTime) {
            this.newRequst = newRequst;
            this.sleepTime = sleepTime;
        }

        public void run() {
            try {
                if (sleepTime > 0) {
                    Thread.sleep(sleepTime);
                }
                resp = ots.batchWriteRow(newRequst, new WriteCallback(lock,  condition));
                isRunning = true;
            } catch (InterruptedException e) {
                LOG.warn(e.getMessage());
            }
        }
    }
    
    /**
     * 该方法主要生成新的请求，并把已经成功的Record剔除掉，最后调用延时发送请求方法发送请求
     */
    public void resetForRetry() {
        LOG.info("begin resetForRetry.");
        isRunning = false;
        BatchWriteRowRequest newRequst = new BatchWriteRowRequest();
        List<Record> newRecords = new ArrayList<Record>();
        try {
            BatchWriteRowResult result = resp.get();
            
            switch(conf.getOperation()) {
            case PUT_ROW:
                List<RowStatus> putStatus = result.getPutRowStatus(conf.getTableName());
                for (int i = 0; i < putStatus.size(); i++) {
                    if (!putStatus.get(i).isSucceed()) {
                        newRequst.addRowPutChange(req.getRowPutChange(conf.getTableName(), i));
                        newRecords.add(records.get(i));
                    }
                }
                break;
            case UPDATE_ROW:
                List<RowStatus> updateStatus = result.getUpdateRowStatus(conf.getTableName());
                for (int i = 0; i < updateStatus.size(); i++) {
                    if (!updateStatus.get(i).isSucceed()) {
                        newRequst.addRowUpdateChange(req.getRowUpdateChange(conf.getTableName(), i));
                        newRecords.add(records.get(i));
                    }
                }
                break;
            }
        } catch (Exception e) {
            newRequst = req;
        }
        
        // reset
        hadRetryTimes++;
        records = newRecords;
        req = newRequst;
        
        delaySend(req,  this.retrySleepTime);
        
        this.retrySleepTime += this.retrySleepTime;
        if (this.retrySleepTime >= 30000) {
            this.retrySleepTime = 30000;
        }
        
        LOG.info("end resetForRetry.");
    }
    
    private void delaySend(BatchWriteRowRequest request, long delayMillinSeconds) {
        if (delayMillinSeconds <= 0) {
            resp = ots.batchWriteRow(request, new WriteCallback(lock,  condition));
            isRunning = true;
        } else {
            DelaySendRequest wait = new DelaySendRequest(request, delayMillinSeconds);
            wait.start();
        }
    }
}
