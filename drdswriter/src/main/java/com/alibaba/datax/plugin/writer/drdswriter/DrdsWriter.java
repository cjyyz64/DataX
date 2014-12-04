package com.alibaba.datax.plugin.writer.drdswriter;


import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Key;

import java.util.List;

public class DrdsWriter extends Writer {
    private static final DataBaseType DATABASE_TYPE = DataBaseType.DRDS;

    public static class Master extends Writer.Master {
        private Configuration originalConfig = null;
        private CommonRdbmsWriter.Master commonRdbmsWriterMaster;
        private String ONLY_SUPPORTED_WRITEMODE = "replace";

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            String writeMode = this.originalConfig.getString(Key.WRITE_MODE, ONLY_SUPPORTED_WRITEMODE);
            if (!ONLY_SUPPORTED_WRITEMODE.equalsIgnoreCase(writeMode)) {
                throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
                        String.format("drdswriter only support writeMode:%s, but you configured writeMode:%s",
                                ONLY_SUPPORTED_WRITEMODE, writeMode));
            }

            this.originalConfig.set(Key.WRITE_MODE, ONLY_SUPPORTED_WRITEMODE);
            this.commonRdbmsWriterMaster = new CommonRdbmsWriter.Master(DATABASE_TYPE);
            this.commonRdbmsWriterMaster.init(this.originalConfig);
        }

        // 对于 Drds 而言，只会暴露一张逻辑表，所以直接在 Master 做 pre,post 操作
        @Override
        public void prepare() {
            this.commonRdbmsWriterMaster.prepare(this.originalConfig);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            return this.commonRdbmsWriterMaster.split(this.originalConfig, mandatoryNumber);
        }

        // 一般来说，是需要推迟到 slave 中进行post 的执行（单表情况例外）
        @Override
        public void post() {
            this.commonRdbmsWriterMaster.post(this.originalConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsWriterMaster.destroy(this.originalConfig);
        }

    }

    public static class Slave extends Writer.Slave {
        private Configuration writerSliceConfig;
        private CommonRdbmsWriter.Slave commonRdbmsWriterSlave;

        @Override
        public void init() {
            this.writerSliceConfig = super.getPluginJobConf();
            this.commonRdbmsWriterSlave = new CommonRdbmsWriter.Slave();
            this.commonRdbmsWriterSlave.init(this.writerSliceConfig);
        }

        @Override
        public void prepare() {
            this.commonRdbmsWriterSlave.prepare(this.writerSliceConfig);
        }

        //TODO 改用连接池，确保每次获取的连接都是可用的（注意：连接可能需要每次都初始化其 session）
        public void startWrite(RecordReceiver recordReceiver) {
            this.commonRdbmsWriterSlave.startWrite(recordReceiver, this.writerSliceConfig,
                    super.getSlavePluginCollector());
        }

        @Override
        public void post() {
            this.commonRdbmsWriterSlave.post(this.writerSliceConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsWriterSlave.destroy(this.writerSliceConfig);
        }

    }
}
