package com.alibaba.datax.plugin.writer.otswriter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.SlavePluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSBatchWriterRowFuture;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSBatchWriterRowFuture.RecordAndError;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConst;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSErrorMessage;
import com.alibaba.datax.plugin.writer.otswriter.utils.Common;
import com.alibaba.datax.plugin.writer.otswriter.utils.DefaultNoRetry;
import com.alibaba.datax.plugin.writer.otswriter.utils.GsonParser;
import com.alibaba.datax.plugin.writer.otswriter.utils.RetryHelper;
import com.aliyun.openservices.ots.ClientConfiguration;
import com.aliyun.openservices.ots.OTSClientAsync;
import com.aliyun.openservices.ots.OTSServiceConfiguration;
import com.aliyun.openservices.ots.model.BatchWriteRowRequest;

public class OtsWriterSlaveProxy {
    
    private Lock lock = new ReentrantLock();
    
    private Condition condition = lock.newCondition();
    
    private List<OTSBatchWriterRowFuture> futures = new LinkedList<OTSBatchWriterRowFuture>();
    
    private static final Logger LOG = LoggerFactory.getLogger(OtsWriterSlaveProxy.class);
    
    private OTSConf conf = null;
    
    private OTSClientAsync ots = null;
    
    public void init(Configuration configuration) {
        LOG.info("OTSWriter slave parameter: {}", configuration.toJSON());
        conf = GsonParser.jsonToConf(configuration.getString(OTSConst.OTS_CONF));
        
        ClientConfiguration clientConfigure = new ClientConfiguration();
        clientConfigure.setMaxConnections(conf.getConcurrencyWrite());
        clientConfigure.setIoThreadCount(conf.getConcurrencyWrite());
        
        OTSServiceConfiguration otsConfigure = new OTSServiceConfiguration();
        otsConfigure.setRetryStrategy(new DefaultNoRetry());
        
        ots = new OTSClientAsync(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstanceName(),
                clientConfigure,
                otsConfigure,
                null);
    }
    
    public void close() {
        ots.shutdown();
    }

    public void write(RecordReceiver recordReceiver, SlavePluginCollector collector) throws Exception {
        LOG.info("write begin.");

        List<Record> array = new ArrayList<Record>(conf.getBatchWriteCount());
        Record record = null;
        while ((record = recordReceiver.getFromReader()) != null) {
            array.add(record);
            if (array.size() >= conf.getBatchWriteCount()) {
                while (futures.size() >= conf.getConcurrencyWrite()) {
                    waitForAny();
                    clearExpireFutureAndRetry(ots, conf, futures, collector);
                } 
                writeToOTS(ots, conf, array, collector);
                array.clear();
            }
        }

        if (!array.isEmpty()) {
            writeToOTS(ots, conf, array, collector);
        }

        while (!futures.isEmpty()) {
            waitForAny();
            clearExpireFutureAndRetry(ots, conf, futures, collector);
        } 

        LOG.info("write end.");
    }

    // private function
    
    private void waitForAny() {
        LOG.debug("waitForAny begin.");
        
        for (OTSBatchWriterRowFuture result : futures) {
            if (result.getResultFuture().isDone()) {
                return;
            }
        }
        if (futures.isEmpty()) {
            return;
        }
        
        lock.lock();
        try {
            condition.await();
        } catch (InterruptedException e) {
            LOG.warn(e.getMessage());
        } finally {
            lock.unlock();
        }
        LOG.debug("waitForAny end.");
    }
    
    private void writeToOTS(OTSClientAsync ots, OTSConf conf, List<Record> records, SlavePluginCollector collector) {
        LOG.debug("writeToOTS begin.");
        
        BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();
        int expectColumnCount = conf.getPrimaryKeyColumn().size() + conf.getAttributeColumn().size();;
        
        boolean hasDataFlag = false;
        
        for (Record r : records) {
            int columnCount = r.getColumnNumber();
            
            if (columnCount != expectColumnCount) {
                throw new IllegalArgumentException(String.format(OTSErrorMessage.RECORD_AND_COLUMN_SIZE_ERROR, columnCount, expectColumnCount));
            }

            try {
                switch (conf.getOperation()) {
                case PUT_ROW:
                    batchWriteRowRequest.addRowPutChange(Common.recordToRowPutChange(conf, r));
                    hasDataFlag = true;
                    break;
                case UPDATE_ROW:
                    batchWriteRowRequest.addRowUpdateChange(Common.recordToRowUpdateChange(conf, r));
                    hasDataFlag = true;
                    break;
                default:
                    throw new RuntimeException(String.format(OTSErrorMessage.OPERATION_PARSE_ERROR, conf.getOperation()));
                }
            } catch (IllegalArgumentException e) {
                collector.collectDirtyRecord(r, e.getMessage());
            }
        }
        
        if (hasDataFlag) {
            futures.add(
                    new OTSBatchWriterRowFuture(
                            ots, conf, lock, condition, records, batchWriteRowRequest, 0
                    )
            );
        }
        LOG.debug("writeToOTS end.");
    }
    
    private void handleRetry(
            SlavePluginCollector collector,
            OTSBatchWriterRowFuture r, 
            Iterator<OTSBatchWriterRowFuture> iter, 
            List<RecordAndError> errors, 
            Exception e, List<Record> records) {
        
        if (r.getHadRetryTimes() < conf.getRetry()) {
            LOG.warn("HadRetryTimes:{} ", r.getHadRetryTimes());
            r.resetForRetry();
        } else {
            LOG.error("Retry times more than limition."); 
            iter.remove();
            if (e == null) {
                for (RecordAndError re : errors) {
                    collector.collectDirtyRecord(re.getRecord(), re.getError().getMessage());
                }
            } else {
                for (Record record : r.getRecords()) {
                    collector.collectDirtyRecord(record, e.getMessage());
                }
            }
        }
    }
    
    private void clearExpireFutureAndRetry(
            OTSClientAsync ots, 
            OTSConf conf, 
            List<OTSBatchWriterRowFuture> list, 
            SlavePluginCollector collector) throws Exception {
        LOG.debug("clearExpireFuture begin.");
        
        for(Iterator<OTSBatchWriterRowFuture> iter = list.iterator(); iter.hasNext();) {
            OTSBatchWriterRowFuture r = iter.next();
            if (r.isDone()) {
                try {
                    List<RecordAndError> errors = null;
                    if (!(errors = r.getRecordAndError()).isEmpty()) {
                        handleRetry(collector, r, iter, errors, null, null);
                    } else {
                        iter.remove();
                    }
                } catch (Exception e) {
                    if (!RetryHelper.canRetry(e)) {
                        throw e;
                    }
                    handleRetry(collector, r, iter, null, e, r.getRecords());
                }
            }
        }
        
        LOG.debug("clearExpireFuture end.");
    }
}
