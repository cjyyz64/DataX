
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
 * Created by jingxing on 14-9-4.
 */
public class StreamWriter extends Writer {
    public static class Master extends Writer.Master {
        private static final Logger LOG = LoggerFactory
                .getLogger(StreamWriter.Master.class);

        @Override
        public void init() {
            LOG.info("init()");
        }

        @Override
        public void prepare() {
            LOG.info("prepare()");
        }

        @Override
        public List<Configuration> split(int readerSplitNumber) {
            List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();
            for(int i=0; i<readerSplitNumber; i++) {
                writerSplitConfigs.add(getPluginJobConf());
            }

            return writerSplitConfigs;
        }

        @Override
        public void post() {
            LOG.info("post()");
        }

        @Override
        public void destroy() {
            LOG.info("destroy()");
        }
    }

    public static class Slave extends Writer.Slave {
        private static final Logger LOG = LoggerFactory
                .getLogger(StreamWriter.Slave.class);

        private String fieldDelimiter = "\t";
        private String encoding = "UTF-8";
        private boolean visible = false;

        @Override
        public void init() {
            Configuration writerSliceConfig = getPluginJobConf();

            this.fieldDelimiter = writerSliceConfig.getString(
                    Key.FIELD_DELIMITER, this.fieldDelimiter);
            this.encoding = writerSliceConfig.getString(
                    Key.ENCODING, this.encoding);
            this.visible = writerSliceConfig.getBool(
                    Key.VISIBLE, this.visible);
        }

        @Override
        public void prepare() {
            LOG.info("prepare()");
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            try {
                BufferedWriter writer = new BufferedWriter(
                        new OutputStreamWriter(System.out, this.encoding));

                Record record;
                while ((record = recordReceiver.getFromReader()) != null) {
                    if (this.visible) {
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
            LOG.info("post()");
        }

        @Override
        public void destroy() {
            LOG.info("destroy()");
        }

        private String makeVisual(Record record) {
            int recordLength = record.getColumnNumber();
            if (record == null || recordLength == 0) {
                return "\n";
            }

            Column column ;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < recordLength; i++) {
                column = record.getColumn(i);
                sb.append(column.toString());

                if (i != recordLength-1) {
                    sb.append(fieldDelimiter);
                } else {
                    sb.append('\n');
                }
            }

            return sb.toString();
        }
    }
}
