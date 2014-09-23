package com.alibaba.datax.plugin.reader.streamreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by jingxing on 14-9-4.
 */
public class StreamReader extends Reader {
    public static class Master extends Reader.Master {
        private static final Logger LOG = LoggerFactory
                .getLogger(StreamReader.Master.class);

        @Override
        public void init() {
            LOG.debug("init()");
        }

        @Override
        public void prepare() {
            LOG.debug("prepare()");
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> readerSplitConfigs = new ArrayList<Configuration>();

            for (int i = 0; i < adviceNumber; i++) {
                readerSplitConfigs.add(getPluginJobConf());
            }
            return readerSplitConfigs;
        }

        @Override
        public void post() {
            LOG.debug("post()");
        }

        @Override
        public void destroy() {
            LOG.debug("destroy()");
        }
    }

    public static class Slave extends Reader.Slave {
        private static final Logger LOG = LoggerFactory
                .getLogger(StreamReader.Slave.class);

        private List<Configuration> columns;
        private long sliceRecordCount = 100L;
        private String fieldDelimiter = ",";

        @Override
        public void init() {
            Configuration readerSliceConfig = getPluginJobConf();
            this.columns = readerSliceConfig.getListConfiguration(Key.COLUMN);
            this.fieldDelimiter = readerSliceConfig.getString(
                    Key.FIELD_DELIMITER, this.fieldDelimiter);
            this.sliceRecordCount = readerSliceConfig.getLong(
                    Key.SLICE_RECORD_COUNT, this.sliceRecordCount);
        }

        @Override
        public void prepare() {
            LOG.debug("prepare()");
        }

        @Override
        public void startRead(RecordSender recordSender) {
            Splitter fieldSplitter = Splitter.on(this.fieldDelimiter);

            /**
             * 没有配置column，标准输入
             */
            if (this.columns == null) {
                String fetchLine;
                BufferedReader reader = null;
                try {
                    reader = new BufferedReader(
                            new InputStreamReader(System.in));
                    Record record;
                    while ((fetchLine = reader.readLine()) != null) {
                        record = recordSender.createRecord();
                        Iterable<String> is = fieldSplitter.split(fetchLine);
                        for (String field : is) {
                            record.addColumn(new StringColumn(field));
                        }
                        recordSender.sendToWriter(record);
                    }
                    recordSender.flush();
                } catch (Exception e) {
                    throw new DataXException(
                            StreamReaderErrorCode.RUNTIME_EXCEPTION, e);
                } finally {
                    try {
                        reader.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            } else {
                if(this.sliceRecordCount > 0) {
                    Record record = recordSender.createRecord();
                    for(Configuration columnConfig : this.columns) {
                        Column column = generateColumn(columnConfig);
                        record.addColumn(column);
                    }

                    long lineCounter = 0l;
                    while (lineCounter++ < this.sliceRecordCount) {
                        recordSender.sendToWriter(record);
                    }
                }
            }
        }

        @Override
        public void post() {
            LOG.debug("post()");
        }

        @Override
        public void destroy() {
            LOG.debug("destroy()");
        }

        private Column generateColumn(Configuration columnConfig) {
            String columnType = columnConfig.getString(Key.TYPE);
            Object columnValue = columnConfig.get(Key.VALUE);
            if("string".equalsIgnoreCase(columnType) ||
                    "char".equalsIgnoreCase(columnType)) {
                return new StringColumn((String)columnValue);
            } else if("long".equalsIgnoreCase(columnType)) {
                return new NumberColumn((Long)columnValue);
            } else if("int".equalsIgnoreCase(columnType)) {
                return new NumberColumn((Integer)columnValue);
            } else if("float".equalsIgnoreCase(columnType)) {
                return new NumberColumn((Float)columnValue);
            } else if("double".equalsIgnoreCase(columnType)) {
                return new NumberColumn((Double)columnValue);
            } else if("bool".equalsIgnoreCase(columnType)) {
                return new BoolColumn((Boolean)columnValue);
            } else if("date".equalsIgnoreCase(columnType)) {
                if(columnValue instanceof Integer) {
                    return new DateColumn((Integer)columnValue);
                } else if(columnValue instanceof Long) {
                    return new DateColumn((Long)columnValue);
                } else {
                    throw new DataXException(StreamReaderErrorCode.CAST_VALUE_TYPE_ERROR,
                            String.format("can not cast[%s] to type[%s]", columnValue, columnType));
                }
            } else if("byte".equalsIgnoreCase(columnType)) {
                return new BytesColumn(((String)columnValue).getBytes());
            } else {
                String errorMessage = String.format("not support column type [%s]", columnType);
                LOG.error(errorMessage);
                throw new DataXException(StreamReaderErrorCode.NOT_SUPPORT_TYPE,
                        errorMessage);
            }
        }
    }
}
