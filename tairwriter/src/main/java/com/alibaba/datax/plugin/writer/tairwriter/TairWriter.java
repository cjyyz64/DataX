package com.alibaba.datax.plugin.writer.tairwriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.exception.ExceptionTracker;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.google.common.collect.Lists;
import com.taobao.tair.impl.mc.MultiClusterTairManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class TairWriter extends Writer {

    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);

        private Configuration originalConfig;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
        }

        @Override
        public void prepare() {
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();
            for (int i = 0; i < mandatoryNumber; i++) {
                writerSplitConfigs.add(this.originalConfig.clone());
            }
            return writerSplitConfigs;
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory
                .getLogger(Task.class);

        private TairConfig conf = null;
        //private MultiClusterTairManager tm = null;
        //private TairWriterWorker worker = null;

        private int threadNum;
        private int memorySize;
        private volatile int queueSize;

        private volatile boolean hasInit = false;
        private volatile boolean isShutdown = false;

        //初始值，会被计算的重新赋值
        private ArrayBlockingQueue<Record> recordQueue = new ArrayBlockingQueue<Record>(100);

        private TairWriterMultiWorker[] works;
        Thread dispatcher;

        private final AtomicReference<Exception> threadException = new AtomicReference<Exception>();


        @Override
        public void init() {
            //检查TairConf，以免在线程中多次重复检查
            conf = new TairConfig(getPluginJobConf());
            threadNum = getPluginJobConf().getInt(Key.TAIR_THREAD_NUM, 40);
            memorySize = getPluginJobConf().getInt(Key.TAIR_MEMORY_SIZE, 16777216);

            if (!conf.checkValid()) {
                throw DataXException.asDataXException(TairWriterErrorCode.TairInitError, conf.getErrorString());
            }
        }

        private void initWorks(int recordSize) {
            try {
                // 根据数据的大小，自动调整queue的长度。。 这里的作用是“内存保护”，基本写tair的每行都差不多多，因此采用这种简单直接的方式。
                if (recordSize >= memorySize) {
                    throw DataXException.asDataXException(TairWriterErrorCode.TairInitError,
                            String.format("tair 的内存设置[%s]太小,小于record的大小[%s]", memorySize, recordSize));
                }

                //queue的最大4096个。
                queueSize = Math.min(memorySize / recordSize, 4000);

                //根据queueSize，调整并发的个数。如果queueSize比并发还小，并发这么多是没有意思的。同时为了保护内存。
                if (queueSize < threadNum) {
                    threadNum = queueSize;
                }

                recordQueue = new ArrayBlockingQueue<Record>(queueSize);
                works = new TairWriterMultiWorker[threadNum];

                for (int i = 0; i < threadNum; i++) {
                    MultiClusterTairManager tm = TairClientManager.getInstance(conf.getConfigId(), conf.getLanguage(),
                            conf.getCompressionThreshold(), conf.getTimeout());
                    works[i] = new TairWriterMultiWorker(Thread.currentThread().getName()+"_"+i, tm, conf, this.getTaskPluginCollector(), queueSize / threadNum, threadException);
                    works[i].start();
                }
            } catch (Exception e) {
                LOG.error("Init tair mc-client failure: {}", e);
                throw DataXException.asDataXException(TairWriterErrorCode.TairInitError, "tair mc客户端 init()失败," + e.getMessage(), e);
            }

            dispatcher= new Thread(new Runnable() {
                @Override
                public void run() {
                    doDispatcher();
                }
            });

            dispatcher.start();
        }

        private void doDispatcher() {

            LOG.info("TairWriter Dispatcher start.");

            Record record = null;
            while (!isShutdown || recordQueue.size() > 0) {
                try {
                    try {
                        record = recordQueue.poll(100, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        LOG.error("recordQueue.poll has Exception: " + e.getMessage(), e);
                    }
                    if (record == null) {
                        continue;
                    }

                    String key = TairWriterMultiWorker.getKeyFromColumn(conf, record);

                    int workid = Math.abs(key.hashCode()) % threadNum;
                    works[workid].send(record);
                } catch (Exception e) {
                    this.getTaskPluginCollector().collectDirtyRecord(record, "doDispatcher this record has Exception: " + e.getMessage()
                            + " => " + ExceptionTracker.trace(e));
                }
            }

            LOG.info("TairWriter Dispatcher end.");
        }

        @Override
        public void prepare() {
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            Record record;
            long lastTime = System.currentTimeMillis();

            try {
                List<Record> tmpListForInit = Lists.newArrayList();
                int tmpSizeForInit = 0;
                //recordReceiver.getFromReader()返回null，表示reader结束
                while ((record = recordReceiver.getFromReader()) != null) {

                    if (!hasInit) {
                        tmpSizeForInit += record.getByteSize();
                        tmpListForInit.add(record);
                        if(tmpListForInit.size() >=100 || tmpSizeForInit>= 1000000) {
                            //取前100个record的平均大小，作为初始化queue和work线程的依据。保护内存！
                            initWorks(tmpSizeForInit/tmpListForInit.size());
                            hasInit = true;
                            recordQueue.addAll(tmpListForInit);
                            tmpListForInit.clear();
                        }else{
                            continue;
                        }
                    }else {
                        recordQueue.put(record);

                        checkException();

                        long currentTime = System.currentTimeMillis();
                        //每2分钟打印一次queue的size，作为调整并发的依据
                        if (currentTime > lastTime + 120000) {
                            LOG.info("TairWriter's recordQueue size=" + recordQueue.size());
                            lastTime = currentTime;
                        }
                    }
                }

                //说明总共不到100条数据，大小不到1M
                if(tmpListForInit.size()>0){
                    initWorks(tmpSizeForInit/tmpListForInit.size());
                    recordQueue.addAll(tmpListForInit);
                    tmpListForInit.clear();
                }

                //队列没有处理完，等待队列处理完
                lastTime = System.currentTimeMillis();
                while (recordQueue.size() > 0) {
                    Thread.sleep(1000);
                    checkException();
                    long currentTime = System.currentTimeMillis();
                    //每分钟打印一次queue的size，作为调整并发的依据
                    if (currentTime > lastTime + 60000) {
                        LOG.info("TairWriter wait work finish, now recordQueue size=" + recordQueue.size());
                        lastTime = currentTime;
                    }
                }

                //队列中没有record，说明dispatcher要么正在处理已经从队列中读出的record，要么结束了
                //等待dispatcher结束
                LOG.info("TairWriter wait dispatcher join start...");
                isShutdown = true;
                if(dispatcher!=null) {
                    dispatcher.join();
                }
                LOG.info("TairWriter wait dispatcher join finished...");

                stopAllWorers();

                //最后，检查所有线程退出
                LOG.info("TairWriter wait work join start...");
                if(works!=null) {
                    for (TairWriterMultiWorker worker : works) {
                        worker.join();
                        checkException();
                    }
                }
                LOG.info("TairWriter wait work join finished.");
            } catch (Exception e) {
                LOG.error("write Exception: {}",e.getMessage(), e);
                throw DataXException.asDataXException(TairWriterErrorCode.TairRuntimeError, e.getMessage(), e);
            }
        }

        private void checkException(){
            if (threadException.get() != null) {
                //有线程异常，结束线程，并抛出异常
                stopAllWorers();
                isShutdown = true;
                throw DataXException.asDataXException(TairWriterErrorCode.TairRuntimeError, threadException.get().getMessage(), threadException.get());
            }
        }

        private void stopAllWorers() {
            if(works == null){
                return;
            }
            for (TairWriterMultiWorker worker : works) {
                if (worker != null) {
                    worker.setIsShutdown(true);
                }
            }
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }
}
