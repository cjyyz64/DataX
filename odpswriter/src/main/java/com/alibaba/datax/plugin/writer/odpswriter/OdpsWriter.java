package com.alibaba.datax.plugin.writer.odpswriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.StrUtil;
import com.alibaba.odps.tunnel.*;
import com.alibaba.odps.tunnel.io.ProtobufRecordWriter;
import com.alibaba.odps.tunnel.io.Record;
import com.aliyun.openservices.odps.ODPSConnection;
import com.aliyun.openservices.odps.Project;
import com.aliyun.openservices.odps.tables.Table;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

//TODO: 换行符：System.getProperties("line.separator")方式获取
public class OdpsWriter extends Writer {
    public static class Master extends Writer.Master {
        private static final Logger LOG = LoggerFactory
                .getLogger(OdpsWriter.Master.class);
        private static final boolean IS_DEBUG = LOG.isDebugEnabled();

        private Configuration originalConfig;
        private String project;
        private String table;
        private String partition;

        private String odpsServer;
        private String tunnelServer;
        private String accountType;

        private boolean truncate;


        private ODPSConnection conn;
        private Project odpsProj;
        private DataTunnel tunnel;
        private com.alibaba.odps.tunnel.Configuration config;
        private String uploadId;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();

            this.odpsServer = this.originalConfig.getString(Key.ODPS_SERVER);
            this.tunnelServer = this.originalConfig.getString(Key.TUNNEL_SERVER);

            this.project = this.originalConfig.getString(Key.PROJECT);
            this.table = this.originalConfig.getString(Key.TABLE);

            this.partition = this.originalConfig.getString(Key.PARTITION, "").trim()
                    .replaceAll(" *= *", "=").replaceAll(" */ *", ",")
                    .replaceAll(" *, *", ",");

            this.accountType = this.originalConfig.getString(Key.ACCOUNT_TYPE,
                    "aliyun");

            this.truncate = this.originalConfig.getBool(Key.TRUNCATE);

            if (IS_DEBUG) {
                LOG.debug("init() PluginParam: [\n{}\n] .", this.originalConfig.toJSON());
            }
        }


        @Override
        public void prepare() {
            if ("aliyun".equals(this.accountType)) {
                this.originalConfig = SecurityChecker.flushAccessKeyID(this.originalConfig);
            }

            if (IS_DEBUG) {
                LOG.debug("prepare PluginParam: [{}] .", this.originalConfig.toString());
            }

            Account tunnelAccount = new Account(this.accountType,
                    this.originalConfig.getString(Key.ACCESS_ID), this.originalConfig.getString(
                    Key.ACCESS_KEY));
            if ("taobao".equalsIgnoreCase(this.accountType)) {
                tunnelAccount.setAlgorithm("rsa");
            }

            this.config = new com.alibaba.odps.tunnel.Configuration(tunnelAccount, tunnelServer);

            this.tunnel = new DataTunnel(config);

            com.aliyun.openservices.odps.Account opdsAccount = new com.aliyun.openservices.odps.Account(
                    this.accountType, this.originalConfig.getString(Key.ACCESS_ID),
                    this.originalConfig.getString(Key.ACCESS_KEY));
            if ("taobao".equalsIgnoreCase(this.accountType)) {
                opdsAccount.setAlgorithm("rsa");
            }

            this.conn = new ODPSConnection(this.odpsServer, opdsAccount);

            this.odpsProj = new Project(conn, this.project);

            if (this.truncate) {
                LOG.info("Try to clean {}:{} .", this.table, this.partition);
                if ("".equals(this.partition.trim())) {
                    truncateTable();
                } else {
                    truncatePart();
                }
            } else {
                // add part if not exists
                if (!"".equals(this.partition.trim()) && !isPartExists()) {
                    addPart();
                }
            }
        }

        private void truncateTable() {
            com.aliyun.openservices.odps.Account opdsAccount = new com.aliyun.openservices.odps.Account(
                    this.originalConfig.getString(this.accountType), this.originalConfig.getString(
                    Key.ACCESS_ID), this.originalConfig.getString(Key.ACCESS_KEY));
            if ("taobao".equalsIgnoreCase(this.accountType)) {
                opdsAccount.setAlgorithm("rsa");
            }

            ODPSConnection conn = new ODPSConnection(this.odpsServer, opdsAccount);

            Project project = new Project(conn, this.project);
            Table tab = new Table(project, this.table);
            try {
                tab.load();
            } catch (Exception e) {
                throw new DataXException(null, "Error when truncate table." + e);
            }
            String dropDdl = "drop table IF EXISTS " + this.table + ";";
            String ddl = ODPSUtils.getTableDdl(tab);

            ODPSUtils.runSqlTask(project, dropDdl);
            ODPSUtils.runSqlTask(project, ddl);

            List<com.aliyun.openservices.odps.tables.Column> partitionKeys = null;
            try {
                partitionKeys = tab.getPartitionKeys();
            } catch (Exception e) {
                throw new DataXException(null, e);
            }
            if (partitionKeys == null || partitionKeys.isEmpty()) {
                LOG.info("Table [{}] has no partition .", tab);
            } else {
                LOG.info("Table [{}] has partition [{}] .", tab,
                        partitionKeys.toString());
                String addPart = ODPSUtils.getAddPartitionDdl(tab);
                ODPSUtils.runSqlTask(project, addPart);
            }
        }

        private void truncatePart() {
            if (isPartExists()) {
                dropPart();
            }
            addPart();
        }

        private Boolean isPartExists() {
            Table table = new Table(this.odpsProj, this.table);
            // check if exist partition
            List<String> odpsParts = ODPSUtils.listOdpsPartitions(table);
            if (null == odpsParts) {
                throw new DataXException("Error when list table partitions.");
            }
            int j = 0;
            for (j = 0; j < odpsParts.size(); j++) {
                if (odpsParts.get(j).replaceAll("'", "").equals(this.partition)) {
                    break;
                }
            }
            if (j == odpsParts.size())
                return false;
            return true;
        }

        private void dropPart() {
            String partSpec = getPartSpec();
            StringBuilder dropPart = new StringBuilder();
            dropPart.append("alter table ").append(this.table)
                    .append(" drop IF EXISTS partition(").append(partSpec)
                    .append(");");
            ODPSUtils.runSqlTask(this.odpsProj, dropPart.toString());
        }

        private void addPart() {
            String partSpec = getPartSpec();
            // add if not exists partition
            StringBuilder addPart = new StringBuilder();
            addPart.append("alter table ").append(this.table)
                    .append(" add IF NOT EXISTS partition(").append(partSpec)
                    .append(");");
            ODPSUtils.runSqlTask(this.odpsProj, addPart.toString());
        }

        private String getPartSpec() {
            StringBuilder partSpec = new StringBuilder();
            String[] parts = this.partition.split(",");
            for (int i = 0; i < parts.length; i++) {
                String part = parts[i];
                String[] kv = part.split("=");
                if (kv.length != 2)
                    throw new DataXException(null, "Wrong partition Spec: " + this.partition);
                partSpec.append(kv[0]).append("=");
                partSpec.append("'").append(kv[1].replace("'", "")).append("'");
                if (i != parts.length - 1)
                    partSpec.append(",");
            }
            return partSpec.toString();
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> splittedConfigs = new ArrayList<Configuration>();

            Upload upload = createTunnelUpload("");
            String id = upload.getUploadId();
            this.uploadId = id;

            for (int i = 0; i < mandatoryNumber; i++) {
                Configuration iParam = this.originalConfig.clone();

                iParam.set(com.alibaba.datax.plugin.writer.odpswriter.Constant.UPLOAD_ID, id);
                iParam.set(com.alibaba.datax.plugin.writer.odpswriter.Constant.BLOCK_ID, String.valueOf(i));
                iParam.set("distributedConcurrencyCount", mandatoryNumber);
                splittedConfigs.add(iParam);
            }
            return splittedConfigs;
        }

        private static final int MAX_RETRY_TIME = 3;

        private Upload createTunnelUpload(String uploadId) {
            Upload upload = null;
            int count = 0;
            while (true) {
                count++;
                try {
                    LOG.info("Try to create tunnel Upload for {} times .", count);
                    if (!"".equals(uploadId)) {
                        upload = tunnel.createUpload(project, table, partition,
                                uploadId);
                    } else {
                        upload = tunnel.createUpload(project, table, partition);
                    }
                    LOG.info("Try to create tunnel Upload OK.");
                    break;
                } catch (Exception e) {
                    if (count > MAX_RETRY_TIME)
                        throw new DataXException(null, "Error when create upload." + e);
                    else {
                        try {
                            Thread.sleep(2 * count * 1000);
                        } catch (InterruptedException e1) {

                        }
                        continue;
                    }
                }
            }
            return upload;
        }


        @Override
        public void post() {
            Upload upload = createTunnelUpload(uploadId);
            List<Long> blocks = new ArrayList<Long>();

            List<String> salveWrotedBlockIds = super.getMasterPluginCollector().getMessage("slaveWrotedBlockId");

            for (String context : salveWrotedBlockIds) {

                String[] blockIdStrs = context.split(",");
                for (int i = 0, len = blockIdStrs.length; i < len; i++) {
                    blocks.add(Long.parseLong(blockIdStrs[i]));
                }
            }

            int count = 0;
            while (true) {
                count++;
                try {
                    upload.complete(blocks.toArray(new Long[0]));
                    break;
                } catch (Exception e) {
                    if (count > MAX_RETRY_TIME)
                        throw new DataXException(null, "Error when complete upload." + e);
                    else {
                        try {
                            Thread.sleep(2 * count * 1000);
                        } catch (InterruptedException e1) {
                        }
                        continue;
                    }
                }
            }
        }

        @Override
        public void destroy() {
            LOG.info("destroy()");
        }

        private void dealMaxRetryTime(Configuration originalConfig) {
            int maxRetryTime = originalConfig.getInt(Key.MAX_RETRY_TIME, MAX_RETRY_TIME);
            if (maxRetryTime < 1) {
                String bussinessMessage = "maxRetryTime should >=1.";
                String message = StrUtil.buildOriginalCauseMessage(bussinessMessage, null);

                LOG.error(message);
                throw new DataXException(OdpsWriterErrorCode.ILLEGAL_VALUE, bussinessMessage);
            }

            originalConfig.set(Key.MAX_RETRY_TIME, maxRetryTime);
        }


    }


    public static class Slave extends Writer.Slave {
        private static final Logger LOG = LoggerFactory
                .getLogger(OdpsWriter.Slave.class);

        private static final boolean IS_DEBUG = LOG.isDebugEnabled();

        private Configuration sliceConfig;
        private String accountType;
        private String tunnelServer;
        private com.alibaba.odps.tunnel.Configuration config;
        private DataTunnel tunnel;
        private RecordSchema schema;

        //TODO got their values
        private String project;
        private String table;
        private String partition;


        private ByteArrayOutputStream out = new ByteArrayOutputStream(
                70 * 1024 * 1024);
        private ProtobufRecordWriter pwriter = null;

        @Override
        public void init() {
            this.sliceConfig = super.getPluginJobConf();

            this.accountType = this.sliceConfig.getString(Key.ACCOUNT_TYPE);
            this.tunnelServer = this.sliceConfig.getString(Key.TUNNEL_SERVER);

            Account tunnelAccount = new Account(this.accountType, this.sliceConfig.getString(Key.ACCESS_ID), this.sliceConfig.getString(
                    Key.ACCESS_KEY));
            if ("taobao".equalsIgnoreCase(this.accountType)) {
                tunnelAccount.setAlgorithm("rsa");
            }

            this.config = new com.alibaba.odps.tunnel.Configuration(tunnelAccount, this.tunnelServer);

            this.tunnel = new DataTunnel(config);

            String uploadId = this.sliceConfig.getString(com.alibaba.datax.plugin.writer.odpswriter.Constant.UPLOAD_ID);
            Upload upload = createTunnelUpload(uploadId);
            this.schema = upload.getSchema();
        }

        private static final int MAX_RETRY_TIME = 3;

        private Upload createTunnelUpload(String uploadId) {
            Upload upload = null;
            int count = 0;
            while (true) {
                count++;
                try {
                    LOG.info("Try to create tunnel Upload for {} times .", count);
                    if (!"".equals(uploadId)) {
                        upload = tunnel.createUpload(project, table, partition,
                                uploadId);
                    } else {
                        upload = tunnel.createUpload(project, table, partition);
                    }
                    LOG.info("Try to create tunnel Upload OK.");
                    break;
                } catch (Exception e) {
                    if (count > MAX_RETRY_TIME)
                        throw new DataXException(null, "Error when create upload." + e);
                    else {
                        try {
                            Thread.sleep(2 * count * 1000);
                        } catch (InterruptedException e1) {

                        }
                        continue;
                    }
                }
            }
            return upload;
        }

        @Override
        public void prepare() {
            LOG.info("prepare()");
        }

        private int max_buffer_length = 64 * 1024 * 1024;

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            long blockId = this.sliceConfig.getLong(com.alibaba.datax.plugin.writer.odpswriter.Constant.BLOCK_ID, 0);
            List<Long> blocks = new ArrayList<Long>();
            com.alibaba.datax.common.element.Record dataxRecord = null;

            // 当前已经打印的脏数据行数
            try {
                pwriter = new ProtobufRecordWriter(schema, out);
                while ((dataxRecord = recordReceiver.getFromReader()) != null) {
                    Record r = line2Record(dataxRecord, schema);
                    if (null != r) {
                        pwriter.write(r);
                    }

                    if (out.size() >= max_buffer_length) {
                        pwriter.close();
                        writeBlock(blockId);
                        blocks.add(blockId);
                        out.reset();
                        pwriter = new ProtobufRecordWriter(schema, out);

                        // 小心：为了保证分布式情况下，每个blockId号不同，设置步长应该这样取 tempKey
                        // "distributedConcurrencyCount"
                        blockId += this.sliceConfig.getInt("distributedConcurrencyCount");
                    }
                }
                // complete protobuf stream, then write to http
                pwriter.close();
                if (out.size() != 0) {
                    writeBlock(blockId);
                    blocks.add(blockId);
                    // reset the buffer for next block
                    out.reset();
                }

                super.getSlavePluginCollector().collectMessage("slaveWrotedBlockId", StringUtils.join(blockId, ","));
            } catch (Exception e) {
                throw new DataXException(null, "Error when upload data to odps tunnel" + e);
            }
        }


        public void writeBlock(long blockId) {
            String uploadId = this.sliceConfig.getString(com.alibaba.datax.plugin.writer.odpswriter.Constant.UPLOAD_ID, "");

            int count = 0;
            while (true) {
                count++;
                try {
                    Upload upload = createTunnelUpload(uploadId);
                    OutputStream hpout = upload.openOutputStream(blockId);
                    LOG.info("Current buf size: {} .", out.size());
                    LOG.info("Start to write block {}, UploadId is {}.", blockId,
                            uploadId);
                    out.writeTo(hpout);
                    hpout.close();
                    LOG.info("Write block [{}] OK .", blockId);
                    break;
                } catch (Exception e) {
                    if (count > MAX_RETRY_TIME)
                        throw new DataXException(null, "Error when upload data to odps tunnel" + e);
                    else {
                        try {
                            Thread.sleep(2 * count * 1000);
                        } catch (InterruptedException e1) {
                        }
                        continue;
                    }
                }
            }
        }

        private volatile boolean printColumnLess = true;// 是否打印对于源头字段数小于odps目的表的行的日志
        private volatile boolean is_compatible = true;// TODO config or delete it

        public Record line2Record(com.alibaba.datax.common.element.Record dataxRecord, RecordSchema schema) {
            int destColumnCount = schema.getColumnCount();
            int sourceColumnCount = dataxRecord.getColumnNumber();
            Record r = new Record(schema.getColumnCount());

            if (sourceColumnCount > destColumnCount) {
                super.getSlavePluginCollector().collectDirtyRecord(dataxRecord, "source column number bigger than dest column num. ");
                return null;
            } else if (sourceColumnCount < destColumnCount) {
                if (printColumnLess) {
                    printColumnLess = false;
                    LOG.warn(
                            "source column={} is less than dest column={}, fill dest column with null !",
                            dataxRecord.getColumnNumber(), destColumnCount);
                }
            }

            for (int i = 0; i < sourceColumnCount; i++) {
                com.alibaba.datax.common.element.Column v = dataxRecord.getColumn(i);
                if (null == v)
                    continue;
                // for compatible dt lib, "" as null
                if (is_compatible && "".equals(v))
                    continue;
                Column.Type type = schema.getColumnType(i);
                try {
                    switch (type) {
                        case ODPS_BIGINT:
                            r.setBigint(i, v.asLong());
                            break;
                        case ODPS_DOUBLE:
                            r.setDouble(i, v.asDouble());
                            break;
                        case ODPS_DATETIME: {
                            r.setDatetime(i, v.asDate());
                            break;
                        }
                        case ODPS_BOOLEAN:
                            r.setBoolean(i, v.asBoolean());
                            break;
                        case ODPS_STRING:
                            r.setString(i, v.asString());
                    }
                } catch (Exception e) {
                    LOG.warn("BAD TYPE: " + e.toString());
                    super.getSlavePluginCollector().collectDirtyRecord(dataxRecord, "Unsupported type: " + e.getMessage());
                    return null;
                }
            }
            return r;
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
}