package com.alibaba.datax.plugin.reader.hbasereader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.hbasereader.util.HbaseProxy;
import com.alibaba.datax.plugin.reader.hbasereader.util.HbaseSplitUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class HbaseReader extends Reader {
    public static class Job extends Reader.Job {
        private final static Logger LOG = LoggerFactory
                .getLogger(Job.class);

        private Configuration originalConfig;

        private String hbaseConfig;
        private String mode;
        private String table;
        private List<Map> column;
        private String encoding;
        private String startRowkey;
        private String endRowKey;
        private boolean isBinaryRowkey;

        private HbaseProxy hbaseProxy = null;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();

            this.hbaseConfig = this.originalConfig.getNecessaryValue(Key.HBASE_CONFIG,
                    HbaseReaderErrorCode.TEMP);

            this.mode = dealMode(this.originalConfig);
            this.originalConfig.set(Key.MODE, this.mode);

            this.table = this.originalConfig.getNecessaryValue(Key.TABLE, HbaseReaderErrorCode.TEMP);
            this.column = this.originalConfig.getList(Key.COLUMN, Map.class);
            if (this.column == null) {
                throw DataXException.asDataXException(HbaseReaderErrorCode.REQUIRED_VALUE, "必须配置 Hbasereader 的 column 配置项.");
            }

            this.checkColumn(this.column);

            this.encoding = this.originalConfig.getString(Key.ENCODING, "utf-8");
            this.originalConfig.set(Key.ENCODING, this.encoding);

            Triple<String, String, Boolean> triple = dealRowkeyRange(this.originalConfig);
            if (triple != null) {
                this.startRowkey = triple.getLeft();
                this.endRowKey = triple.getMiddle();
                this.isBinaryRowkey = triple.getRight();

                this.originalConfig.set(Constant.HAS_RANGE_CONFIG, true);
            }
        }

        private Triple<String, String, Boolean> dealRowkeyRange(Configuration originalConfig) {
            Map<String, Object> range = originalConfig.getMap(Constant.RANGE);
            if (range == null) {
                return null;
            }

            String startRowkey = (String) range.get(Key.START_ROWKEY);
            String endRowkey = (String) range.get(Key.END_ROWKEY);
            Boolean isBinaryRowkey;
            Object isBinaryRowkeyTemp = range.get(Key.IS_BINARY_ROWKEY);
            if (startRowkey != null || endRowkey != null) {
                if (isBinaryRowkeyTemp == null) {
                    throw DataXException.asDataXException(HbaseReaderErrorCode.REQUIRED_VALUE, "您配置了rowkey 的范围，则必须配置 isBinaryRowkey，本项可以配置为 true 或者 false. 分别对应于 DataX 内部调用Bytes.toBytesBinary(String rowKey) 或者Bytes.toBytes(String rowKey) 两个不同的 API.");
                }

                if ("true".equalsIgnoreCase(isBinaryRowkeyTemp.toString())) {
                    isBinaryRowkey = true;
                } else if ("false".equalsIgnoreCase(isBinaryRowkeyTemp.toString())) {
                    isBinaryRowkey = false;
                } else {
                    throw DataXException.asDataXException(HbaseReaderErrorCode.REQUIRED_VALUE, "isBinaryRowkey，只能配置为 true 或者 false. 分别对应于 DataX 内部调用Bytes.toBytesBinary(String rowKey) 或者Bytes.toBytes(String rowKey) 两个不同的 API.");
                }
            }
            return ImmutableTriple.of(startRowkey, endRowkey, isBinaryRowkey);
        }

        private String dealMode(Configuration originalConfig) {
            String mode = originalConfig.getString(Key.MODE, "normal");
            if (!mode.equalsIgnoreCase("normal") && !mode.equalsIgnoreCase("multiVersion")) {
                throw DataXException.asDataXException(HbaseReaderErrorCode.TEMP, "mode 仅能配置为 normal 或者 multiVersion .");
            }

            return mode;
        }

        @Override
        public void prepare() {
            try {
                hbaseProxy = HbaseProxy.newProxy(this.hbaseConfig, this.table);
                hbaseProxy.setEncode(this.encoding);
                hbaseProxy.setBinaryRowkey(this.isBinaryRowkey);
            } catch (IOException e) {
                try {
                    if (null != hbaseProxy) {
                        hbaseProxy.close();
                    }
                } catch (IOException e1) {
                }
                throw DataXException.asDataXException(HbaseReaderErrorCode.TEMP, e);
            }
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

        private void checkColumn(List<Map> column) {
            parseColumn(column);
        }
    }

    public static class Task extends Reader.Task {
        private String tableName = null;
        private String columnTypeAndNames = null;
        private String hbaseConf = null;
        // private String rowkeyRange = null;
        private boolean isBinaryRowkey = false;
        private HbaseProxy hbaseProxy = null;
        private String[] columnTypes = null;

        private String[] columnFamilyAndQualifier = null;

        private List<HbaseColumnCell> hbaseColumnCells

        @Override
        public void init() {
            Configuration taskConfig = super.getPluginJobConf();
            this.tableName = taskConfig.getString(Key.TABLE);
            this.hbaseConf = taskConfig.getString(Key.HBASE_CONFIG);

            this.columnTypeAndNames = taskConfig.getString(Key.COLUMN);

            this.isBinaryRowkey = taskConfig.getBool(Key.IS_BINARY_ROWKEY);

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

    public static List<HbaseColumnCell> parseColumn(List<Map> column) {
        List<HbaseColumnCell> hbaseColumnCells = new ArrayList<HbaseColumnCell>();

        for (Map<String, String> aColumn : column) {
            String type = aColumn.get("type");
            String columnName = aColumn.get("name");
            String columnValue = aColumn.get("value");
            HbaseColumnCell oneColumnCell = new HbaseColumnCell.Builder(ColumnType.valueOf(type)).columnName(columnName).columnValue(columnValue).build();
            hbaseColumnCells.add(oneColumnCell);
        }

        return hbaseColumnCells;
    }
}
