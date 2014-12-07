package com.alibaba.datax.plugin.reader.hbasereader;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.alibaba.datax.plugin.reader.hbasereader.util.HbaseProxy;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.dw.datax.common.exception.DataXException;
import com.taobao.dw.datax.common.exception.ExceptionTracker;
import com.taobao.dw.datax.common.plugin.Line;
import com.taobao.dw.datax.common.plugin.LineSender;
import com.taobao.dw.datax.common.plugin.PluginParam;
import com.taobao.dw.datax.common.plugin.PluginStatus;
import com.taobao.dw.datax.common.plugin.Reader;
import com.taobao.dw.datax.common.util.PluginKeysCompatibilityTool;

public class HBaseReader_t {
    private final static Logger LOG = LoggerFactory
            .getLogger(HBaseReader_t.class);

    private static final boolean IS_DEBUG = LOG.isDebugEnabled();

    private String tableName = null;

    private String columnTypeAndNames = null;

    private String hbaseConf = null;

    // private String rowkeyRange = null;
    private boolean needRowkey = false;

    private boolean isBinaryRowkey = false;

    private HbaseProxy proxy = null;

    private String[] columnTypes = null;

    private String[] columnFamilyAndQualifier = null;

    public List<PluginParam> split() {
        HBaseReaderSplitter splitter = new HBaseReaderSplitter();
        splitter.setPluginParam(getPluginParam());
        splitter.init();
        return splitter.split();
    }

    public int init() {
        PluginKeysCompatibilityTool.refreshPluginKeys(Thread.currentThread()
                .getContextClassLoader(), getPluginParam());
        refreshColumn(getPluginParam());

        this.tableName = getPluginParam().getValue(Key.TABLE);
        this.hbaseConf = getPluginParam().getValue(Key.HBASE_CONFIG);

        this.columnTypeAndNames = getPluginParam().getValue(Key.COLUMN);

        this.isBinaryRowkey = getPluginParam().getBoolValue(
                Key.BINARY_ROWKEY, false);
        this.needRowkey = this.getPluginParam().getBoolValue(
                Key.READ_ROWKEY, false);

        if (IS_DEBUG) {
            LOG.debug(getPluginParam().toString());
        }

        HbaseColumnConfig hbaseColumnConfig = new HbaseColumnConfig();
        parseColumn(columnTypeAndNames, hbaseColumnConfig);
        this.columnTypes = hbaseColumnConfig.columnTypes;
        this.columnFamilyAndQualifier = hbaseColumnConfig.columnFamilyAndQualifiers;

        checkColumnTypes(this.columnTypes);

        try {
            proxy = HbaseProxy.newProxy(hbaseConf, tableName);
            String encoding = getPluginParam().getValue(Key.ENCODING,
                    "UTF-8");
            proxy.setEncode(encoding);
            proxy.setBinaryRowkey(this.isBinaryRowkey);
        } catch (IOException e) {
            LOG.error(ExceptionTracker.trace(e));
            try {
                if (null != proxy) {
                    proxy.close();
                }
            } catch (IOException e1) {
            }
            throw new DataXException(e);
        }

        return PluginStatus.SUCCESS.value();
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
                throw new DataXException(String.format(
                        "Unsupported hbase type[%s], only support types:%s .",
                        type, hbaseColumnTypeSet));
            }
        }
    }

    private void parseColumn(String columnTypeAndNames,
                             HbaseColumnConfig hbaseColumnConfig) {
        String[] columnArray = columnTypeAndNames.split(",");
        int columnLength = columnArray.length;
        if (columnLength < 1) {
            throw new DataXException(
                    String.format("Configed Hbase column=[%s] is empty !",
                            columnTypeAndNames));
        }

        hbaseColumnConfig.columnTypes = new String[columnLength];
        hbaseColumnConfig.columnFamilyAndQualifiers = new String[columnLength];

        String tempColumn = null;
        String[] tempColumnArray = null;
        for (int i = 0; i < columnLength; i++) {
            tempColumn = columnArray[i].trim();
            if (StringUtils.isBlank(tempColumn)) {
                throw new DataXException(String.format(
                        "Configed Hbase column=[%s] has empty value!",
                        columnTypeAndNames));
            }
            tempColumnArray = tempColumn.split("\\|");

            if (2 != tempColumnArray.length) {
                throw new DataXException(
                        String.format(
                                "Wrong Format:[%s], Right Format:type|family:qualifier",
                                tempColumn));
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

    private void refreshColumn(PluginParam pluginParam) {
        String tempColumns = pluginParam.getValue(Key.COLUMN);
        if (!tempColumns.contains("|")) {
            tempColumns = makeNewColumn(tempColumns);
            pluginParam.putValue(Key.COLUMN, tempColumns);
        } else {
            LOG.info("Your DataX Job Hbase Config is Standard .");
        }

    }

    private String makeNewColumn(String tempColumns) {
        StringBuilder sb = new StringBuilder(tempColumns.length() * 2);
        String[] columns = tempColumns.split(",");
        for (String column : columns) {
            if (StringUtils.isBlank(column)) {
                throw new DataXException(String.format(
                        "column[%s] contains Illegal Blank Character !",
                        tempColumns));
            }
            sb.append("string").append('|').append(column.trim()).append(',');
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    public int connect() {
        LOG.info("HBaseReader start to connect to HBase .");
        String startRowkey = getPluginParam().getValue(Key.START_ROWKEY,
                "");
        String endRowkey = getPluginParam().getValue(Key.END_ROWKEY, "");

        if (StringUtils.isBlank(startRowkey) && StringUtils.isBlank(endRowkey)) {
            LOG.info("HBaseReader prepare to query all records . ");
            proxy.setStartEndRange(null, null);
        } else {
            byte[] startRowkeyByte = null;
            byte[] endRowkeyByte = null;
            if (this.isBinaryRowkey) {
                startRowkeyByte = Bytes.toBytesBinary(startRowkey);
                endRowkeyByte = Bytes.toBytesBinary(endRowkey);
            } else {
                startRowkeyByte = Bytes.toBytes(startRowkey);
                endRowkeyByte = Bytes.toBytes(endRowkey);
            }

            proxy.setStartEndRange(startRowkeyByte, endRowkeyByte);
            LOG.info(String.format(
                    "HBaseReader prepare to query records [%s, %s) .",
                    (startRowkeyByte.length == 0 ? "-infinite" : startRowkey),
                    (endRowkeyByte.length == 0 ? "+infinite" : endRowkey)));
        }

        if (needRowkey == true) {
            LOG.info("HBaseReader will extract rowkey info .");
        } else {
            LOG.info("HBaseReader will not extract rowkey info .");
        }
        proxy.setNeedRowkey(needRowkey);
        return PluginStatus.SUCCESS.value();
    }

    public int startRead(LineSender sender) {
        // for test
        if ("BAZHEN".equalsIgnoreCase(System.getenv("BAZHEN"))) {
            return PluginStatus.SUCCESS.value();
        }

        try {
            proxy.prepare(this.columnFamilyAndQualifier);
        } catch (Exception e) {
            throw new DataXException(e);
        }

        Line line = sender.createLine();
        boolean fetchOK = true;
        while (true) {
            try {
                fetchOK = proxy.fetchLine(line, this.columnTypes);
            } catch (Exception e) {
                LOG.warn(String.format("Bad line rowkey:[%s] for Reason:[%s]",
                        line == null ? null : line.toString(','),
                        e.getMessage()), e);
                continue;
            }
            if (fetchOK) {
                sender.sendToWriter(line);
                line = sender.createLine();
            } else {
                break;
            }
        }
        sender.flush();

        return PluginStatus.SUCCESS.value();
    }

    public int finish() {
        return PluginStatus.SUCCESS.value();
    }

    // TODO: 添加计数器
    public int post() {
        if (null != this.proxy) {
            try {
                proxy.close();
            } catch (Exception e) {
                //
            }
        }
        return PluginStatus.SUCCESS.value();
    }
}
