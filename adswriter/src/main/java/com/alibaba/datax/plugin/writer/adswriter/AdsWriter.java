package com.alibaba.datax.plugin.writer.adswriter;

import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.odpswriter.OdpsWriter;

import java.util.List;


//TODO writeProxy
public class AdsWriter extends Writer {


    public static class Job extends Writer.Job {

        private OdpsWriter.Job odpsWriterProxy = new OdpsWriter.Job();
        private Configuration originalConfig = null;

        @Override
        public void init() {

//            this.originalConfig = super.getPluginJobConf();

//            //创建odps表
//            Configuration newConf = generate();
//            super.setPluginConf(newConf);
            String project = null;
            String accessKey = null;
            String partition = null;
            String tunnelServer = null;
            String truncat = null;
            String odpsServer = null;
            String table = null;
            String accessId = null;
            List<String> column = null;


        }

        // 一般来说，是需要推迟到 task 中进行pre 的执行（单表情况例外）
        @Override
        public void prepare() {
            //倒数据到odps表中

            this.odpsWriterProxy.init();
            this.odpsWriterProxy.prepare();
            this.odpsWriterProxy.split(3);
            this.odpsWriterProxy.post();
            this.odpsWriterProxy.destroy();
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
