package com.alibaba.datax.plugin.reader.hbasereader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.hbasereader.util.*;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
        private List<Map> column;
        private String mode;
        private List<HbaseColumnCell> hbaseColumnCells;

        @Override
        public void init() {
            this.taskConfig = super.getPluginJobConf();

            this.column = this.taskConfig.getList(Key.COLUMN, Map.class);
            this.mode = this.taskConfig.getString(Key.MODE);
            if ("normal".equalsIgnoreCase(this.mode)) {
                this.hbaseAbstractReader = new NormalReader(this.taskConfig);
            } else {
                this.hbaseAbstractReader = new MutiVersionReader(this.taskConfig);
            }
            this.hbaseColumnCells = HbaseReader.parseColumn(this.column);
        }

        @Override
        public void prepare() {
        }

        @Override
        public void startRead(RecordSender recordSender) {
            try {
                this.hbaseAbstractReader.prepare(this.hbaseColumnCells);
            } catch (Exception e) {
                throw DataXException.asDataXException(HbaseReaderErrorCode.PREPAR_READ_ERROR, e);
            }

            Record record = recordSender.createRecord();
            boolean fetchOK = true;
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

    public static List<HbaseColumnCell> parseColumn(List<Map> column) {
        List<HbaseColumnCell> hbaseColumnCells = new ArrayList<HbaseColumnCell>();

        HbaseColumnCell oneColumnCell = null;

        for (Map<String, String> aColumn : column) {
            ColumnType type = ColumnType.getByTypeName(aColumn.get("type"));
            String columnName = aColumn.get("name");
            String columnValue = aColumn.get("value");
            String dateformat = aColumn.get("format");

            if (type == ColumnType.DATE) {
                Validate.notNull(dateformat, "Hbasereader 的列配置中，如果类型为时间，则必须指定时间格式. 形如：yyyy-MM-dd HH:mm:ss");
                oneColumnCell = new HbaseColumnCell
                        .Builder(type)
                        .columnName(columnName)
                        .columnValue(columnValue)
                        .dateformat(dateformat)
                        .build();
            } else {
                Validate.isTrue(dateformat == null, "Hbasereader 的列配置中，如果类型不为时间，则不需要指定时间格式.");

                oneColumnCell = new HbaseColumnCell
                        .Builder(type)
                        .columnName(columnName)
                        .columnValue(columnValue)
                        .build();
            }

            hbaseColumnCells.add(oneColumnCell);
        }

        return hbaseColumnCells;
    }
}
