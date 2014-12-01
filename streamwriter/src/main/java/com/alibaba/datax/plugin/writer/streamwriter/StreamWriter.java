
package com.alibaba.datax.plugin.writer.streamwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

public class StreamWriter extends Writer {
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
                writerSplitConfigs.add(this.originalConfig);
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

        private static final String NEWLINE_FLAG = System.getProperty("line.separator", "\n");

        private Configuration writerSliceConfig;

        private String fieldDelimiter;
        private boolean print;


        @Override
        public void init() {
            this.writerSliceConfig = getPluginJobConf();

            this.fieldDelimiter = this.writerSliceConfig.getString(
                    Key.FIELD_DELIMITER, "\t");
            this.print = this.writerSliceConfig.getBool(Key.PRINT, true);
        }

        @Override
        public void prepare() {
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            try {
                BufferedWriter writer = new BufferedWriter(
                        new OutputStreamWriter(System.out, "UTF-8"));

                Record record;
                while ((record = recordReceiver.getFromReader()) != null) {
                    if (this.print) {
                        writer.write(recordToString(record));
                    } else {
                        /* do nothing */
                    }
                }
                writer.flush();
            } catch (Exception e) {
                throw DataXException.asDataXException(StreamWriterErrorCode.RUNTIME_EXCEPTION, e);
            }
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }

        private String recordToString(Record record) {
            int recordLength = record.getColumnNumber();
            if (null == record || 0 == recordLength) {
                return NEWLINE_FLAG;
            }

            Column column;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < recordLength; i++) {
                column = record.getColumn(i);
                sb.append(column.asString()).append(fieldDelimiter);
            }
            sb.setLength(sb.length() - 1);
            sb.append(NEWLINE_FLAG);

            return sb.toString();
        }
    }
}
