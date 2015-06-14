package com.alibaba.datax.plugin.writer.tairwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.taobao.tair.impl.mc.MultiClusterTairManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

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
        private MultiClusterTairManager tm = null;
        private TairWriterWorker worker = null;

        @Override
        public void init() {
            conf = new TairConfig(getPluginJobConf());
            if (!conf.checkValid()) {
                  throw DataXException.asDataXException(TairWriterErrorCode.TairInitError, conf.getErrorString());
            }
            try {
                tm = TairClientManager.getInstance(conf.getConfigId(), conf.getLanguage(),
                        conf.getCompressionThreshold(), conf.getTimeout());
            } catch (Exception e) {
                LOG.error("Init tair mc-client failure: {}", e);
                throw DataXException.asDataXException(TairWriterErrorCode.TairInitError, "tair mc客户端 init()失败," + e.getMessage(), e);
            }
            worker = new TairWriterWorker(tm, conf, this.getTaskPluginCollector());
        }

        @Override
        public void prepare() {
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            try {
                worker.write(recordReceiver);
            } catch (Exception e) {
              LOG.error("write Exception: {}", e);
              throw DataXException.asDataXException(TairWriterErrorCode.TairRuntimeError, e.getMessage(), e);
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
