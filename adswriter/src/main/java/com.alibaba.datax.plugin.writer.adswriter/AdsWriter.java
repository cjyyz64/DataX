package com.alibaba.datax.plugin.writer.mysqlwriter;

import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;

import java.util.List;


//TODO writeProxy
public class AdsWriter extends Writer {

    public static class Job extends Writer.Job {
        private Configuration originalConfig = null;

        @Override
        public void init() {
        }

        // 一般来说，是需要推迟到 task 中进行pre 的执行（单表情况例外）
        @Override
        public void prepare() {
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            return null;
        }

        // 一般来说，是需要推迟到 task 中进行post 的执行（单表情况例外）
        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }

    }

    public static class Task extends Writer.Task {
        private Configuration writerSliceConfig;

        @Override
        public void init() {
        }

        @Override
        public void prepare() {
        }

        //TODO 改用连接池，确保每次获取的连接都是可用的（注意：连接可能需要每次都初始化其 session）
        public void startWrite(RecordReceiver recordReceiver) {
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }

}
