
package com.alibaba.datax.plugin.writer.streamwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * TODO:\n 换行符，可以考虑全局跨操作系统，从系统中获取
 */
public class StreamWriter extends Writer {
    public static class Master extends Writer.Master {
        private static final Logger LOG = LoggerFactory
                .getLogger(StreamWriter.Master.class);

        private Configuration originalConfig;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            Boolean print = this.originalConfig.getBool(Key.PRINT);
            if (null == print) {
                throw new DataXException(StreamWriterErrorCode.REQUIRED_VALUE, "Lost config print.");
            }
        }

        @Override
        public void prepare() {
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();
            for (int i = 0; i < mandatoryNumber; i++) {
                writerSplitConfigs.add(getPluginJobConf());
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

    public static class Slave extends Writer.Slave {
        private static final Logger LOG = LoggerFactory
                .getLogger(StreamWriter.Slave.class);

        private Configuration writerSliceConfig;

        private String fieldDelimiter;
        private boolean print;

        @Override
        public void init() {
            this.writerSliceConfig = getPluginJobConf();

            this.fieldDelimiter = this.writerSliceConfig.getString(
                    Key.FIELD_DELIMITER, "\t");
            this.print = this.writerSliceConfig.getBool(Key.PRINT);
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
                        writer.write(makeVisual(record));
                    } else {
                        /* do nothing */
                    }
                }
                writer.flush();
            } catch (Exception e) {
                throw new DataXException(StreamWriterErrorCode.RUNTIME_EXCEPTION, e);
            }
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }

        private String makeVisual(Record record) {
            int recordLength = record.getColumnNumber();
            if (null == record || 0 == recordLength) {
                return "\n";
            }

            Column column;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < recordLength; i++) {
                column = record.getColumn(i);
                sb.append(column.asString()).append(fieldDelimiter);
            }
            sb.setLength(sb.length() - 1);
            sb.append("\n");

            return sb.toString();
        }
    }
}
