package com.alibaba.datax.plugin.reader.hbasereader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.hbasereader.util.HbaseProxy;
import com.alibaba.datax.plugin.reader.hbasereader.util.HbaseSplitUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class HbaseReader extends Reader {
    public static class Job extends Reader.Job {
        private final static Logger LOG = LoggerFactory
                .getLogger(Job.class);
        private Configuration originalConfig;

        private String hbaseConfig;
        private String mode;
        private String table;
        private List<String> columns;
        private String range;
        private String encoding;
        private HbaseProxy proxy = null;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.hbaseConfig = this.originalConfig.getNecessaryValue(Key.HBASE_CONFIG, HbaseReaderErrorCode.TEMP);
            this.mode = this.originalConfig.getString(Key.MODE, "normal");
            if (!this.mode.equalsIgnoreCase("normal") && !this.mode.equalsIgnoreCase("mutiVersion")) {
                throw DataXException.asDataXException(HbaseReaderErrorCode.TEMP, "mode 仅能配置为 normal 或者 mutiVersion .");
            }
            this.originalConfig.set(Key.MODE, this.mode);

            this.table = this.originalConfig.getNecessaryValue((Key.TABLE), HbaseReaderErrorCode.TEMP);
            this.columns = this.originalConfig.getList((Key.COLUMN);
            this.range = this.originalConfig.getString(Constant.RANGE, null);
            this.encoding = this.originalConfig.getString(Key.ENCODING, "utf-8");

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
        public void prepare() {
            LOG.info("HBaseReader start to connect to HBase .");
            //TODO parse startRowkey endRowkey from range config
            String startRowkey = null;
            String endRowkey = null;

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
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            //TODO  adviceNumber?
            return HbaseSplitUtil.split(this.originalConfig);
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
        private HbaseProxy hbaseProxy;

        @Override
        public void init() {

        }

        @Override
        public void prepare() {
            super.prepare();
        }

        @Override
        public void startRead(RecordSender recordSender) {
            try {
                this.hbaseProxy.prepare(this.columnFamilyAndQualifier);
            } catch (Exception e) {
                throw DataXException.asDataXException(HbaseReaderErrorCode.TEMP, e);
            }

            Record line = recordSender.createRecord();
            boolean fetchOK = true;
            while (true) {
                try {
                    fetchOK = hbaseProxy.fetchLine(line, this.columnTypes);
                } catch (Exception e) {
                    LOG.warn(String.format("Bad line rowkey:[%s] for Reason:[%s]",
                            line == null ? null : line.toString(','),
                            e.getMessage()), e);
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
    }
}
