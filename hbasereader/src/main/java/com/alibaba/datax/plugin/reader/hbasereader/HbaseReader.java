package com.alibaba.datax.plugin.reader.hbasereader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.hbasereader.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class HbaseReader extends Reader {
    public static class Job extends Reader.Job {
        private static Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originalConfig;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();

            HbaseUtil.doPretreatment(this.originalConfig);

            LOG.debug("After init(), now originalConfig is:\n{}\n", this.originalConfig);
        }

        @Override
        public void prepare() {
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            return HbaseSplitUtil.split(this.originalConfig);
        }


        @Override
        public void post() {

        }

        @Override
        public void destroy() {
        }

    }

    public static class Task extends Reader.Task {
        private Configuration taskConfig;

        private HbaseAbstractReader hbaseAbstractReader;
        private String mode;

        @Override
        public void init() {
            this.taskConfig = super.getPluginJobConf();

            this.mode = this.taskConfig.getString(Key.MODE);
            ModeType modeType = ModeType.getByTypeName(this.mode);

            switch (modeType) {
                case Normal:
                    this.hbaseAbstractReader = new NormalReader(this.taskConfig);
                    break;
                case MultiVersionFixedColumn:
                    this.hbaseAbstractReader = new MultiVersionFixedColumnReader(this.taskConfig);
                    break;
                case MultiVersionDynamicColumn:
                    this.hbaseAbstractReader = new MultiVersionDynamicColumnReader(this.taskConfig);
                    break;
                default:
                    throw DataXException.asDataXException(HbaseReaderErrorCode.ILLEGAL_VALUE, "Hbasereader 不支持此类模式:" + modeType);
            }
        }

        @Override
        public void prepare() {
            try {
                this.hbaseAbstractReader.prepare();
            } catch (Exception e) {
                throw DataXException.asDataXException(HbaseReaderErrorCode.PREPAR_READ_ERROR, e);
            }
        }

        @Override
        public void startRead(RecordSender recordSender) {
            Record record = recordSender.createRecord();
            boolean fetchOK;
            while (true) {
                try {
                    fetchOK = this.hbaseAbstractReader.fetchLine(record);
                } catch (Exception e) {
                    super.getTaskPluginCollector().collectDirtyRecord(record, e);
                    continue;
                }
                if (fetchOK) {
                    recordSender.sendToWriter(record);
                    record = recordSender.createRecord();
                } else {
                    break;
                }
            }
            recordSender.flush();
        }

        @Override
        public void post() {
            super.post();
        }

        @Override
        public void destroy() {
            if (this.hbaseAbstractReader != null) {
                try {
                    this.hbaseAbstractReader.close();
                } catch (Exception e) {
                    //
                }
            }
        }


    }
}
