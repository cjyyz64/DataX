package com.alibaba.datax.plugin.reader.db2reader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;

import java.util.List;

/**
 * <pre>
 *
 * IBM DB2 Universal Driver Type 4
 *
 * DRIVER CLASS: com.ibm.db2.jcc.DB2Driver
 *
 * DRIVER LOCATION: db2jcc.jar and db2jcc_license_cu.jar
 * (Both of these jars must be included)
 *
 * JDBC URL FORMAT: jdbc:db2://<host>[:<port>]/<database_name>
 *
 * JDBC URL Examples:
 *
 * jdbc:db2://127.0.0.1:50000/SAMPLE
 *
 * </pre>
 */
public class DB2Reader extends Reader {

    private static final DataBaseType DATABASE_TYPE = DataBaseType.DB2;

    public static class Job extends Reader.Job {

        private Configuration originalConfig = null;
        private CommonRdbmsReader.Job commonRdbmsReaderJob;

        @Override
        public void init() {

            this.originalConfig = super.getPluginJobConf();
            int fetchSize = this.originalConfig.getInt(com.alibaba.datax.plugin.rdbms.reader.Constant.FETCH_SIZE,
                    Constant.DEFAULT_FETCH_SIZE);
            if (fetchSize < 1) {
                throw DataXException.asDataXException(DBUtilErrorCode.REQUIRED_VALUE,
                        "fetchSize不能小于1.");
            }
            this.originalConfig.set(com.alibaba.datax.plugin.rdbms.reader.Constant.FETCH_SIZE, fetchSize);

            this.commonRdbmsReaderJob = new CommonRdbmsReader.Job(DATABASE_TYPE);
            this.commonRdbmsReaderJob.init(this.originalConfig);
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            return this.commonRdbmsReaderJob.split(this.originalConfig, adviceNumber);
        }

        @Override
        public void post() {
            this.commonRdbmsReaderJob.post(this.originalConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsReaderJob.destroy(this.originalConfig);
        }

    }

    public static class Task extends Reader.Task {

        private Configuration readerSliceConfig;
        private CommonRdbmsReader.Task commonRdbmsReaderTask;

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            this.commonRdbmsReaderTask = new CommonRdbmsReader.Task(DATABASE_TYPE);
            this.commonRdbmsReaderTask.init(this.readerSliceConfig);

        }

        @Override
        public void startRead(RecordSender recordSender) {
            int fetchSize = this.readerSliceConfig.getInt(com.alibaba.datax.plugin.rdbms.reader.Constant.FETCH_SIZE);

            this.commonRdbmsReaderTask.startRead(this.readerSliceConfig, recordSender,
                    super.getTaskPluginCollector(), fetchSize);
        }

        @Override
        public void post() {
            this.commonRdbmsReaderTask.post(this.readerSliceConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsReaderTask.destroy(this.readerSliceConfig);
        }

    }

}
