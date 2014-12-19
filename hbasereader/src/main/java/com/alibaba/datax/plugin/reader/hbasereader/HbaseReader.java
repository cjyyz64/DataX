package com.alibaba.datax.plugin.reader.hbasereader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.hbasereader.util.HbaseProxy;
import com.alibaba.datax.plugin.reader.hbasereader.util.HbaseSplitUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HbaseReader extends Reader {
    public static class Job extends Reader.Job {
        private final static Logger LOG = LoggerFactory
                .getLogger(Job.class);
        private Configuration originalConfig;

        private String hbaseConfig;
        private String mode;
        private String table;
        private List<String> columns;
        private String encoding;
        private String startRowkey;
        private String endRowKey;

        private String rowkeyType;
        private boolean isBinaryRowkey;

        private HbaseProxy proxy = null;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.hbaseConfig = this.originalConfig.getNecessaryValue(Key.HBASE_CONFIG, HbaseReaderErrorCode.TEMP);
            this.mode = this.originalConfig.getString(Key.MODE, "normal");
            if (!this.mode.equalsIgnoreCase("normal") && !this.mode.equalsIgnoreCase("multiVersion")) {
                throw DataXException.asDataXException(HbaseReaderErrorCode.TEMP, "mode 仅能配置为 normal 或者 multiVersion .");
            }
            this.originalConfig.set(Key.MODE, this.mode);

            this.table = this.originalConfig.getNecessaryValue((Key.TABLE), HbaseReaderErrorCode.TEMP);
            this.columns = this.originalConfig.getList(Key.COLUMN, String.class);
            this.encoding = this.originalConfig.getString(Key.ENCODING, "utf-8");
            this.startRowkey = this.originalConfig.getString(Key.START_ROWKEY);
            this.endRowKey = this.originalConfig.getString(Key.END_ROWKEY);

            this.rowkeyType = this.originalConfig.getString(Key.ROWKEY_TYPE);
            if (!this.rowkeyType.equalsIgnoreCase("bytes") && !this.rowkeyType.equalsIgnoreCase("string")) {
                throw DataXException.asDataXException(HbaseReaderErrorCode.TEMP, "rowkeyType 仅能配置为 bytes 或者 string .");
            }

            this.isBinaryRowkey = "bytes".equalsIgnoreCase(this.rowkeyType);
        }

        @Override
        public void prepare() {
            //TODO conn to hbase, faile fast
            try {
                proxy = HbaseProxy.newProxy(this.hbaseConfig, this.table);
                proxy.setEncode(this.encoding);
                proxy.setBinaryRowkey(this.isBinaryRowkey);
            } catch (IOException e) {
                try {
                    if (null != proxy) {
                        proxy.close();
                    }
                } catch (IOException e1) {
                }
                throw DataXException.asDataXException(HbaseReaderErrorCode.TEMP, e);
            }
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            //TODO  adviceNumber?
            return HbaseSplitUtil.split(this.originalConfig, this.proxy);
        }


        @Override
        public void post() {

        }

        @Override
        public void destroy() {
            if (null != this.proxy) {
                try {
                    proxy.close();
                } catch (Exception e) {
                    //
                }
            }
        }
    }

    public static class Task extends Reader.Task {
        private String tableName = null;
        private String columnTypeAndNames = null;
        private String hbaseConf = null;
        // private String rowkeyRange = null;
        private boolean needRowkey = false;
        private boolean isBinaryRowkey = false;
        private HbaseProxy hbaseProxy = null;
        private String[] columnTypes = null;
        private String[] columnFamilyAndQualifier = null;

        @Override
        public void init() {
            Configuration taskConfig = super.getPluginJobConf();
            this.tableName = taskConfig.getString(Key.TABLE);
            this.hbaseConf = taskConfig.getString(Key.HBASE_CONFIG);

            this.columnTypeAndNames = taskConfig.getString(Key.COLUMN);

            this.isBinaryRowkey = "bytes".equalsIgnoreCase(taskConfig.getString(Key.ROWKEY_TYPE));

            HbaseColumnConfig hbaseColumnConfig = new HbaseColumnConfig();
            parseColumn(columnTypeAndNames, hbaseColumnConfig);
            this.columnTypes = hbaseColumnConfig.columnTypes;
            this.columnFamilyAndQualifier = hbaseColumnConfig.columnFamilyAndQualifiers;

            checkColumnTypes(this.columnTypes);

            try {
                hbaseProxy = HbaseProxy.newProxy(hbaseConf, tableName);
                String encoding = taskConfig.getString(Key.ENCODING, "UTF-8");
                hbaseProxy.setEncode(encoding);
                hbaseProxy.setBinaryRowkey(this.isBinaryRowkey);
            } catch (IOException e) {
//                LOG.error(ExceptionTracker.trace(e));
                try {
                    if (null != hbaseProxy) {
                        hbaseProxy.close();
                    }
                } catch (IOException e1) {
                }
                throw DataXException.asDataXException(null, e);
            }
        }

        @Override
        public void prepare() {
            super.prepare();
        }

        @Override
        public void startRead(RecordSender recordSender) {

            //TODO
            String[] columnFamilyAndQualifier = null;

            try {
                this.hbaseProxy.prepare(columnFamilyAndQualifier);
            } catch (Exception e) {
                throw DataXException.asDataXException(HbaseReaderErrorCode.TEMP, e);
            }

            Record line = recordSender.createRecord();
            boolean fetchOK = true;
            while (true) {
                try {
                    fetchOK = hbaseProxy.fetchLine(line, this.columnTypes);
                } catch (Exception e) {
//                    LOG.warn(String.format("Bad line rowkey:[%s] for Reason:[%s]",
//                            line == null ? null : line.toString(','),
//                            e.getMessage()), e);
                    continue;
                }
                if (fetchOK) {
                    recordSender.sendToWriter(line);
                    line = recordSender.createRecord();
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
            if (null != this.hbaseProxy) {
                try {
                    this.hbaseProxy.close();
                } catch (Exception e) {
                    //
                }
            }
        }


        private void parseColumn(String columnTypeAndNames,
                                 HbaseColumnConfig hbaseColumnConfig) {
            String[] columnArray = columnTypeAndNames.split(",");
            int columnLength = columnArray.length;
            if (columnLength < 1) {
//                throw new DataXException(
//                        String.format("Configed Hbase column=[%s] is empty !",
//                                columnTypeAndNames));
            }

            hbaseColumnConfig.columnTypes = new String[columnLength];
            hbaseColumnConfig.columnFamilyAndQualifiers = new String[columnLength];

            String tempColumn = null;
            String[] tempColumnArray = null;
            for (int i = 0; i < columnLength; i++) {
                tempColumn = columnArray[i].trim();
                if (StringUtils.isBlank(tempColumn)) {
//                    throw new DataXException(String.format(
//                            "Configed Hbase column=[%s] has empty value!",
//                            columnTypeAndNames));
                }
                tempColumnArray = tempColumn.split("\\|");

                if (2 != tempColumnArray.length) {
//                    throw new DataXException(
//                            String.format(
//                                    "Wrong Format:[%s], Right Format:type|family:qualifier",
//                                    tempColumn));
                }

                hbaseColumnConfig.columnTypes[i] = tempColumnArray[0].trim()
                        .toLowerCase();

                String columnFamilyAndQualifier = tempColumnArray[1].trim();
                hbaseColumnConfig.columnFamilyAndQualifiers[i] = columnFamilyAndQualifier;

                if (!columnFamilyAndQualifier.contains(":")) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Column %s must be like 'family:qualifier'",
                                    tempColumn));
                }
            }
        }

        private void checkColumnTypes(String[] tempColumnTypes) {
            Set<String> hbaseColumnTypeSet = new HashSet<String>();
            hbaseColumnTypeSet.add("boolean");
            hbaseColumnTypeSet.add("short");
            hbaseColumnTypeSet.add("int");
            hbaseColumnTypeSet.add("long");
            hbaseColumnTypeSet.add("float");
            hbaseColumnTypeSet.add("double");
            hbaseColumnTypeSet.add("string");
            for (String type : tempColumnTypes) {
                if (!hbaseColumnTypeSet.contains(type.trim().toLowerCase())) {
//                    throw new DataXException(String.format(
//                            "Unsupported hbase type[%s], only support types:%s .",
//                            type, hbaseColumnTypeSet));
                }
            }
        }


    }
}
