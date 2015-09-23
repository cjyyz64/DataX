package com.alibaba.datax.plugin.writer.tairwriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.taobao.tair.impl.mc.MultiClusterTairManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
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

        private ArrayBlockingQueue<Record> recordQueue = new ArrayBlockingQueue<Record>(2048);

        private TairWriterMultiWorker[] works;

        private final AtomicReference<Exception> threadException = new AtomicReference<Exception>();


        @Override
        public void init() {
            //检查TairConf，以免在线程中多次重复检查
            conf = new TairConfig(getPluginJobConf());
            threadNum = getPluginJobConf().getInt(Key.TAIR_THREAD_NUM, 40);
            works = new TairWriterMultiWorker[threadNum];
            if (!conf.checkValid()) {
                throw DataXException.asDataXException(TairWriterErrorCode.TairInitError, conf.getErrorString());
            }

            try {
                initWorks();
            } catch (Exception e) {
                LOG.error("Init tair mc-client failure: {}", e);
                throw DataXException.asDataXException(TairWriterErrorCode.TairInitError, "tair mc客户端 init()失败," + e.getMessage(), e);
            }
        }

        private void initWorks() {
            for (int i = 0; i < threadNum; i++) {
                MultiClusterTairManager tm = TairClientManager.getInstance(conf.getConfigId(), conf.getLanguage(),
                        conf.getCompressionThreshold(), conf.getTimeout());
                works[i] = new TairWriterMultiWorker(i, tm, conf, this.getTaskPluginCollector(), recordQueue, threadException);
                works[i].start();
            }
        }

        @Override
        public void prepare() {
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            Record record;
            long lastTime = System.currentTimeMillis();
            try {
                //recordReceiver.getFromReader()返回null，表示reader结束
                while ((record = recordReceiver.getFromReader()) != null) {
                    recordQueue.put(record);

                    if (threadException.get() != null) {
                        //有线程异常，结束线程，并抛出异常
                        stopAllWorers();
                        throw DataXException.asDataXException(TairWriterErrorCode.TairRuntimeError, threadException.get().getMessage(), threadException.get());
                    }
                    long currentTime = System.currentTimeMillis();
                    //每分钟打印一次queue的size，作为调整并发的依据
                    if (currentTime > lastTime + 60000) {
                        LOG.info("TairWriter's recordQueue size=" + recordQueue.size());
                        lastTime = currentTime;
                    }
                }

                //队列没有处理完，等待队列处理完
                lastTime = System.currentTimeMillis();
                while (recordQueue.size() > 0) {
                    Thread.sleep(1000);
                    long currentTime = System.currentTimeMillis();
                    //每分钟打印一次queue的size，作为调整并发的依据
                    if (currentTime > lastTime + 60000) {
                        LOG.info("TairWriter wait work finish, now recordQueue size=" + recordQueue.size());
                        lastTime = currentTime;
                    }
                }

                //队列中没有record，说明work要么正在处理已经从队列中读出的record，要么结束了
                stopAllWorers();

                //最后，检查所有线程退出
                LOG.info("TairWriter wait work join start...");
                for (TairWriterMultiWorker worker : works) {
                    worker.join();
                }
                LOG.info("TairWriter wait work join finished.");
            } catch (Exception e) {
                LOG.error("write Exception: {}", e);
                throw DataXException.asDataXException(TairWriterErrorCode.TairRuntimeError, e.getMessage(), e);
            }
        }

        private void stopAllWorers() {
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
