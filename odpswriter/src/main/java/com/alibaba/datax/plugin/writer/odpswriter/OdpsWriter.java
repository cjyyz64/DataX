package com.alibaba.datax.plugin.writer.odpswriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.SlavePluginCollector;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.ListUtil;
import com.alibaba.datax.plugin.writer.odpswriter.util.IdAndKeyUtil;
import com.alibaba.datax.plugin.writer.odpswriter.util.OdpsUtil;
import com.alibaba.odps.tunnel.DataTunnel;
import com.alibaba.odps.tunnel.RecordSchema;
import com.alibaba.odps.tunnel.Upload;
import com.aliyun.openservices.odps.Project;
import com.aliyun.openservices.odps.tables.Table;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        private String accountType;
        private boolean truncate;
        private DataTunnel dataTunnel;
        private Project odpsProject;
        private String uploadId;
        private Upload masterUpload;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();

            OdpsUtil.checkNecessaryConfig(this.originalConfig);
            OdpsUtil.dealMaxRetryTime(this.originalConfig);

            this.project = this.originalConfig.getString(Key.PROJECT);
            this.table = this.originalConfig.getString(Key.TABLE);

            this.partition = OdpsUtil.formatPartition(this.originalConfig
                    .getString(Key.PARTITION, ""));
            this.originalConfig.set(Key.PARTITION, this.partition);

            this.accountType = this.originalConfig.getString(Key.ACCOUNT_TYPE,
                    Constant.DEFAULT_ACCOUNT_TYPE);
            this.originalConfig.set(Key.ACCOUNT_TYPE, Constant.DEFAULT_ACCOUNT_TYPE);

            this.truncate = this.originalConfig.getBool(Key.TRUNCATE);

            if (IS_DEBUG) {
                LOG.debug("After init, job config now is: [\n{}\n] .",
                        this.originalConfig.toJSON());
            }
        }

        @Override
        public void prepare() {
            String accessId = null;
            String accessKey = null;

            if (Constant.DEFAULT_ACCOUNT_TYPE
                    .equalsIgnoreCase(this.accountType)) {
                this.originalConfig = IdAndKeyUtil.parseAccessIdAndKey(this.originalConfig);
                accessId = this.originalConfig.getString(Key.ACCESS_ID);
                accessKey = this.originalConfig.getString(Key.ACCESS_KEY);
                if (IS_DEBUG) {
                    LOG.debug("accessId:[{}], accessKey:[{}] .", accessId,
                            accessKey);
                }

                LOG.info("accessId:[{}] .", accessId);
            }

            // init dataTunnel config
            this.dataTunnel = OdpsUtil.initDataTunnel(this.originalConfig);

            // init odps config
            this.odpsProject = OdpsUtil.initOdpsProject(this.originalConfig);

            if (this.truncate) {
                String table = this.originalConfig.getString(Key.TABLE);
                Table tab = new Table(odpsProject, table);
                boolean isPartitionedTable = OdpsUtil.isPartitionedTable(tab);

                if (isPartitionedTable) {
                    //分区表
                    if (StringUtils.isBlank(this.partition)) {
                        throw new DataXException(OdpsWriterErrorCode.TEMP, "Can not truncate partitioned table without assign partition.");
                    } else {
                        LOG.info("Try to clean partition:[{}] in table:[{}].",
                                this.partition, this.table);
                        OdpsUtil.truncatePartition(this.odpsProject, this.table,
                                this.partition);
                    }
                } else {
                    if (StringUtils.isNotBlank(this.partition)) {
                        throw new DataXException(OdpsWriterErrorCode.TEMP, "Can not truncate non partitioned table with assign partition.");
                    } else {
                        LOG.info("Try to truncate table:[{}].", this.table);
                        OdpsUtil.truncateTable(tab);
                    }
                }
            } else {
                boolean isPartitionExists = OdpsUtil.isPartitionExists(this.odpsProject, this.table,
                        this.partition);

                // add part if not exists
                if (StringUtils.isNotBlank(this.partition) && !isPartitionExists) {
                    LOG.info("Try to add partition:[{}] in table:[{}].", this.partition, this.table);
                    OdpsUtil.addPart(this.odpsProject, this.table, this.partition);
                }
            }
        }

        /**
         * 此处主要是对 uploadId进行设置，以及对 blockId 的开始值进行设置。
         * <p/>
         * 对 blockId 需要同时设置开始值以及下一个 blockId 的步长值(INTERVAL_STEP)。
         */
        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configurations = new ArrayList<Configuration>();

            this.masterUpload = OdpsUtil.createMasterTunnelUpload(
                    this.dataTunnel, this.project, this.table, this.partition);
            this.uploadId = this.masterUpload.getUploadId();
            LOG.info("uploadId:[{}].", this.uploadId);

            //TODO log it
            RecordSchema schema = this.masterUpload.getSchema();
            List<String> allColumns = OdpsUtil.getAllColumns(schema);
            LOG.info("allColumnList: {} .", StringUtils.join(allColumns, ','));

            dealColumn(this.originalConfig, allColumns);

            for (int i = 0; i < mandatoryNumber; i++) {
                Configuration tempConfig = this.originalConfig.clone();

                tempConfig.set(Constant.UPLOAD_ID, this.uploadId);
                tempConfig.set(Constant.BLOCK_ID, String.valueOf(i));
                tempConfig.set(Constant.INTERVAL_STEP, mandatoryNumber);

                configurations.add(tempConfig);
            }

            if (IS_DEBUG) {
                LOG.debug("After split, the job config now is:[\n{}\n].", this.originalConfig);
            }

            return configurations;
        }

        private void dealColumn(Configuration originalConfig, List<String> allColumns) {
            List<String> userConfiguredColumns = originalConfig.getList(Key.COLUMN, String.class);
            if (1 == userConfiguredColumns.size() && "*".equals(userConfiguredColumns.get(0))) {
                userConfiguredColumns = allColumns;
                originalConfig.set(Key.COLUMN, allColumns);
            } else {
                //检查列是否重复（所有写入，都是不允许写入段的列重复的）
                ListUtil.makeSureNoValueDuplicate(userConfiguredColumns, false);

                //检查列是否存在
                ListUtil.makeSureBInA(allColumns, userConfiguredColumns, false);
            }

            List<Integer> columnPositions = OdpsUtil.parsePosition(allColumns, userConfiguredColumns);
            originalConfig.set(Constant.COLUMN_POSITION, columnPositions);
        }

        @Override
        public void post() {
            List<Long> blocks = new ArrayList<Long>();
            List<String> salveWroteBlockIds = super.getMasterPluginCollector()
                    .getMessage(Constant.SLAVE_WROTE_BLOCK_MESSAGE);

            for (String context : salveWroteBlockIds) {
                String[] blockIdStrs = context.split(",");
                for (int i = 0, len = blockIdStrs.length; i < len; i++) {
                    blocks.add(Long.parseLong(blockIdStrs[i]));
                }
            }

            if (IS_DEBUG) {
                LOG.debug("salveWroteBlockIds:[{}].", StringUtils.join(salveWroteBlockIds, ","));
            }

            LOG.info("Master begin to commit blocks:[\n{}\n].", StringUtils.join(blocks, ","));
            OdpsUtil.masterCompleteBlocks(this.masterUpload, blocks.toArray(new Long[0]));
        }

        @Override
        public void destroy() {
        }

    }

    public static class Slave extends Writer.Slave {
        private static final Logger LOG = LoggerFactory
                .getLogger(OdpsWriter.Slave.class);

        private static final boolean IS_DEBUG = LOG.isDebugEnabled();

        private Configuration sliceConfig;
        private DataTunnel dataTunnel;
        private String project;
        private String table;
        private String partition;
        private Upload slaveUpload;

        private String uploadId = null;

        @Override
        public void init() {
            this.sliceConfig = super.getPluginJobConf();

            this.project = this.sliceConfig.getString(Key.PROJECT);
            this.table = this.sliceConfig.getString(Key.TABLE);
            this.partition = OdpsUtil.formatPartition(this.sliceConfig
                    .getString(Key.PARTITION, ""));
            this.sliceConfig.set(Key.PARTITION, this.partition);

            this.dataTunnel = OdpsUtil.initDataTunnel(this.sliceConfig);

            uploadId = this.sliceConfig.getString(Constant.UPLOAD_ID);
            this.slaveUpload = OdpsUtil.getSlaveTunnelUpload(this.dataTunnel, this.project,
                    this.table, this.partition, uploadId);

        }

        @Override
        public void prepare() {
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            // blockId 的开始值，master在 split 的时候已经准备好
            long blockId = this.sliceConfig.getLong(Constant.BLOCK_ID);
            int intervalStep = this.sliceConfig.getInt(Constant.INTERVAL_STEP);
            List<Long> blocks = new ArrayList<Long>();
            com.alibaba.datax.common.element.Record dataxRecord = null;

            List<Integer> columnPositions = this.sliceConfig.getList(Constant.COLUMN_POSITION,
                    Integer.class);

            try {
                SlavePluginCollector slavePluginCollector = super.getSlavePluginCollector();

                OdpsWriterProxy proxy = new OdpsWriterProxy(this.slaveUpload, blockId, intervalStep,
                        columnPositions);
                while ((dataxRecord = recordReceiver.getFromReader()) != null) {
                    try {
                        proxy.writeOneRecord(dataxRecord, blocks);
                    } catch (DataXException e1) {
                        throw e1;
                    } catch (Exception e2) {
                        slavePluginCollector.collectDirtyRecord(dataxRecord,
                                "Bad record for: " + e2.getMessage());
                    }
                }

                proxy.writeRemainingRecord(blocks);

                slavePluginCollector.collectMessage(Constant.SLAVE_WROTE_BLOCK_MESSAGE,
                        StringUtils.join(blocks, ","));
            } catch (Exception e) {
                throw new DataXException(OdpsWriterErrorCode.WRITER_BLOCK_FAIL, e);
            }
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }

    }
}