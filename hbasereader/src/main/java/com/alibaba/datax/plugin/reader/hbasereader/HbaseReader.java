package com.alibaba.datax.plugin.reader.hbasereader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.hbasereader.util.HbaseProxy;
import com.alibaba.datax.plugin.reader.hbasereader.util.HbaseSplitUtil;
import com.alibaba.datax.plugin.reader.hbasereader.util.HbaseUtil;
import com.alibaba.datax.plugin.reader.hbasereader.util.LogUtil;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;

import java.util.*;

public class HbaseReader extends Reader {
    public static class Job extends Reader.Job {
        private static Logger LOG;

        private Configuration originalConfig;
        private HbaseProxy hbaseProxy = null;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            LOG = LogUtil.ReaderLog.initLoglevel(Job.class, this.originalConfig);

            HbaseUtil.doPretreatment(this.originalConfig);

            LOG.debug("After init(), now originalConfig is:{}", this.originalConfig);
        }

        @Override
        public void prepare() {
            this.hbaseProxy = HbaseProxy.newProxy(this.originalConfig);
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            return HbaseSplitUtil.split(this.originalConfig, this.hbaseProxy);
        }


        @Override
        public void post() {

        }

        @Override
        public void destroy() {
            if (null != this.hbaseProxy) {
                try {
                    hbaseProxy.close();
                } catch (Exception e) {
                    //
                }
            }
        }

    }

    public static class Task extends Reader.Task {
        private Configuration taskConfig;

        private HbaseProxy hbaseProxy = null;

        private List<Map> column;
        private List<HbaseColumnCell> hbaseColumnCells;

        @Override
        public void init() {
            this.taskConfig = super.getPluginJobConf();

            this.column = this.taskConfig.getList(Key.COLUMN, Map.class);
            this.hbaseColumnCells = HbaseReader.parseColumn(this.column);
        }

        @Override
        public void prepare() {
            this.hbaseProxy = HbaseProxy.newProxy(this.taskConfig);
        }

        @Override
        public void startRead(RecordSender recordSender) {
            try {
                this.hbaseProxy.prepare(this.hbaseColumnCells);
            } catch (Exception e) {
                throw DataXException.asDataXException(HbaseReaderErrorCode.TEMP, e);
            }

            Record record = recordSender.createRecord();
            boolean fetchOK = true;
            while (true) {
                try {
                    fetchOK = this.hbaseProxy.fetchLine(record, this.hbaseColumnCells);
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
            if (this.hbaseProxy != null) {
                try {
                    this.hbaseProxy.close();
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
                Validate.isTrue(dateformat != null, "Hbasereader 的列配置中，如果类型为时间，则必须指定时间格式. 形如：yyyy-MM-dd HH:mm:ss");
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
