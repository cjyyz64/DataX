package com.alibaba.datax.plugin.writer.odpswriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.odpswriter.util.OdpsSplitUtil;
import com.alibaba.datax.plugin.writer.odpswriter.util.OdpsUtil;
import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.Table;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

//TODO: 换行符：System.getProperties("line.separator")方式获取
public class OdpsWriter extends Writer {
    public static class Master extends Writer.Master {
        private static final Logger LOG = LoggerFactory
                .getLogger(OdpsWriter.Master.class);
        private static final boolean IS_DEBUG = LOG.isDebugEnabled();

        public static final int DEFAULT_MAX_RETRY_TIME = 3;

        private Configuration originalConfig;
        private Odps odps;
        private Table table;
        private List<Long> blockIds = new ArrayList<Long>();
        private String uploadId;
        private UploadSession masterUploadSession;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            dealMaxRetryTime(this.originalConfig);

            this.odps = OdpsUtil.initOdps(this.originalConfig);
            this.table = OdpsUtil.initTable(this.odps, this.originalConfig);

            OdpsUtil.checkIfVirtualTable(this.table);

            dealPartition(this.originalConfig, this.table);

            dealColumn(this.originalConfig, this.table);

            if (IS_DEBUG) {
                LOG.debug("After init(), the originalConfig now is:[\n{}\n]",
                        this.originalConfig.toJSON());
            }
        }


        @Override
        public void prepare() {
            boolean truncate = this.originalConfig.getBool(Key.TRUNCATE, true);

            boolean isPartitionedTable = this.originalConfig
                    .getBool(Constant.IS_PARTITIONED_TABLE);
            if (truncate) {
                if (isPartitionedTable) {
                    String partition = this.originalConfig
                            .getString(Key.PARTITION);
                    LOG.info("Begin to clean partitioned table:[{}], partition:[{}].",
                            this.table.getName(), partition);
                    OdpsUtil.truncatePartition(this.table, partition, this.originalConfig.getBool(
                            Constant.IS_PARTITION_EXIST));

                    LOG.info("Finished clean partitioned table:[{}], partition:[{}].",
                            this.table.getName(), partition);
                } else {
                    LOG.info("Begin to clean non partitioned table:[{}].",
                            this.table.getName());

                    OdpsUtil.truncateTable(this.odps, this.table);
                    LOG.info("Finished clean non partitioned table:[{}].",
                            this.table.getName());
                }
            }

            //注意，createMasterSession要求分区已经创建完成
            this.masterUploadSession = OdpsUtil.createMasterSession(this.odps,
                    this.originalConfig);
            this.uploadId = this.masterUploadSession.getId();
            LOG.info("UploadId:[{}]", this.uploadId);

            if (IS_DEBUG) {
                LOG.debug("After prepare(), the originalConfig now is:[\n{}\n]",
                        this.originalConfig.toJSON());
            }
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            return OdpsSplitUtil.doSplit(this.originalConfig, this.uploadId, this.blockIds,
                    mandatoryNumber);
        }


        @Override
        public void post() {
            LOG.info("Begin to commit all blocks. uploadId:[{}],blocks:[\n{}\n].", this.uploadId,
                    StringUtils.join(this.blockIds, "\n"));

            try {
                this.masterUploadSession.commit(blockIds.toArray(new Long[0]));
            } catch (Exception e) {
                LOG.error(String.format("Error while commit all blocks. uploadId:[%s]",
                        this.uploadId), e);
                throw new DataXException(OdpsWriterErrorCode.COMMIT_BLOCK_FAIL, e);
            }
        }

        @Override
        public void destroy() {
            LOG.info("destroy()");
        }

        private void dealMaxRetryTime(Configuration originalConfig) {
            int maxRetryTime = originalConfig.getInt(Key.MAX_RETRY_TIME, DEFAULT_MAX_RETRY_TIME);
            if (maxRetryTime < 1) {
                throw new DataXException(OdpsWriterErrorCode.ILLEGAL_VALUE,
                        "maxRetryTime should >=1.");
            }

            originalConfig.set(Key.MAX_RETRY_TIME, maxRetryTime);
        }

        /**
         * 对分区的配置处理。如果是分区表，则必须配置一个叶子分区。如果是非分区表，则不允许配置分区。
         */
        private void dealPartition(Configuration originalConfig, Table table) {
            boolean isPartitionedTable = originalConfig
                    .getBool(Constant.IS_PARTITIONED_TABLE);

            String userConfiguredPartition = originalConfig
                    .getString(Key.PARTITION);

            if (isPartitionedTable) {
                // 分区表，需要配置分区
                if (null == userConfiguredPartition) {
                    //缺失 Key:partition
                    throw new DataXException(
                            OdpsWriterErrorCode.REQUIRED_KEY,
                            String.format(
                                    "Lost key named partition, table:[%s] is partitioned.",
                                    table.getName()));
                } else if (StringUtils.isEmpty(userConfiguredPartition)) {
                    //缺失 partition的值配置
                    throw new DataXException(
                            OdpsWriterErrorCode.REQUIRED_VALUE,
                            String.format(
                                    "Lost partition value, table:[%s] is partitioned.",
                                    table.getName()));
                } else {
                    //TODO 需要添加对分区级数校验的方法（sdk 本身有级数api吗？）
                    List<String> tableAllPartitions = OdpsUtil.getTableAllPartitions(this.table,
                            this.originalConfig.getInt(Key.MAX_RETRY_TIME));

                    List<String> formattedTableAllPartitions = listToLowerCase(OdpsUtil.formatPartitions(
                            tableAllPartitions));

                    String formatteddUserConfiguredPartition = checkUserConfiguredPartition(
                            userConfiguredPartition);

                    if (formattedTableAllPartitions.contains(formatteddUserConfiguredPartition.toLowerCase())) {
                        originalConfig.set(Constant.IS_PARTITION_EXIST, true);
                    } else {
                        originalConfig.set(Constant.IS_PARTITION_EXIST, false);
                    }


                    originalConfig.set(Key.PARTITION, formatteddUserConfiguredPartition);
                }
            } else {
                // 非分区表，则不能配置分区( 严格到不能出现 partition 这个 key)
                userConfiguredPartition = originalConfig
                        .getString(Key.PARTITION);
                if (null != userConfiguredPartition) {
                    throw new DataXException(
                            OdpsWriterErrorCode.ILLEGAL_KEY,
                            String.format(
                                    "Can not config partition, Table:[%s] is not partitioned, ",
                                    table.getName()));
                }
            }
        }

        private String checkUserConfiguredPartition(
                String userConfiguredPartition) {
            // 对用户自身配置的所有分区进行特殊字符的处理
            String standardUserConfiguredPartition = OdpsUtil
                    .formatPartition(userConfiguredPartition);

            if ("*".equals(standardUserConfiguredPartition)) {
                // 不允许
                throw new DataXException(OdpsWriterErrorCode.UNSUPPORTED_COLUMN_TYPE,
                        "Partition can not be *.");
            }

            //返回的是：已经进行过特殊字符处理的分区配置值
            return standardUserConfiguredPartition;
        }

        private void dealColumn(Configuration originalConfig, Table table) {
            // 用户配置的 column
            List<String> userConfiguredColumns = originalConfig.getList(
                    Key.COLUMN, String.class);
            if (null == userConfiguredColumns) {
                //缺失 Key:column
                throw new DataXException(
                        OdpsWriterErrorCode.REQUIRED_KEY,
                        String.format(
                                "Lost key named column, table:[%s].",
                                table.getName()));
            } else if (userConfiguredColumns.isEmpty()) {
                //缺失 column 的值配置
                throw new DataXException(
                        OdpsWriterErrorCode.REQUIRED_VALUE,
                        String.format(
                                "Lost column value, table:[%s].",
                                table.getName()));
            } else {
                List<Column> tableOriginalColumns = OdpsUtil.getTableAllColumns(table);
                LOG.info("tableAllColumn:[\n{}\n]", StringUtils.join(tableOriginalColumns, "\n"));

                LOG.info("Column configured as * is not recommend, DataX will convert it to tableOriginalColumns by order.");
                List<String> tableOriginalColumnNameList = OdpsUtil.getTableOriginalColumnNameList(tableOriginalColumns);

                if (1 == userConfiguredColumns.size() && "*".equals(userConfiguredColumns.get(0))) {
                    //处理 * 配置，替换为：odps 表所有列
                    this.originalConfig.set(Key.COLUMN, tableOriginalColumnNameList);
                    List<Integer> columnPositions = new ArrayList<Integer>();
                    for (int i = 0, len = tableOriginalColumnNameList.size(); i < len; i++) {
                        columnPositions.add(i);
                    }
                    this.originalConfig.set(Constant.COLUMN_POSITION, columnPositions);
                } else {
                    /**
                     * 处理配置为["column0","column1"]的情况。
                     *
                     * <p>
                     *     检查列名称是否都是来自表本身，以及不允许列重复等；然后解析其写入顺序。
                     * </p>
                     */

                    doCheckColumn(userConfiguredColumns, tableOriginalColumnNameList);

                    List<Integer> columnPositions = OdpsUtil.parsePosition(
                            userConfiguredColumns, tableOriginalColumnNameList);
                    this.originalConfig.set(Constant.COLUMN_POSITION, columnPositions);
                }
            }

        }

        private void doCheckColumn(List<String> userConfiguredColumns,
                                   List<String> tableOriginalColumnNameList) {
            // 检查列是否重复
            List<String> tempUserConfiguredColumns = new ArrayList<String>(userConfiguredColumns);
            Collections.sort(tempUserConfiguredColumns);
            for (int i = 0, len = tempUserConfiguredColumns.size(); i < len - 1; i++) {
                if (tempUserConfiguredColumns.get(i).equalsIgnoreCase(tempUserConfiguredColumns.get(i + 1))) {
                    throw new DataXException(OdpsWriterErrorCode.ILLEGAL_VALUE,
                            String.format("Can not config duplicate column:[%s].", tempUserConfiguredColumns.get(i)));
                }
            }

            // 检查列是否都来自于表(列名称大小写不敏感)
            List<String> lowerCaseUserConfiguredColumns = listToLowerCase(userConfiguredColumns);
            List<String> lowerCaseTableOriginalColumnNameList = listToLowerCase(tableOriginalColumnNameList);
            for (String aColumn : lowerCaseUserConfiguredColumns) {
                if (!lowerCaseTableOriginalColumnNameList.contains(aColumn)) {
                    throw new DataXException(OdpsWriterErrorCode.ILLEGAL_VALUE,
                            String.format("Can not find column:[%s].", aColumn));
                }
            }
        }

        private List<String> listToLowerCase(List<String> aList) {
            List<String> lowerCaseList = new ArrayList<String>();
            for (String e : aList) {
                lowerCaseList.add(e == null ? null : e.toLowerCase());
            }

            return lowerCaseList;
        }

    }


    public static class Slave extends Writer.Slave {
        private static final Logger LOG = LoggerFactory
                .getLogger(OdpsWriter.Slave.class);

        private static final boolean IS_DEBUG = LOG.isDebugEnabled();

        private Configuration writerSliceConfig;

        private Odps odps = null;
        private long blockId;

        @Override
        public void init() {
            this.writerSliceConfig = getPluginJobConf();

            this.odps = OdpsUtil.initOdps(this.writerSliceConfig);

            //blockId 在 master 中已分配完成
            this.blockId = this.writerSliceConfig.getLong(Constant.BLOCK_ID);

            if (IS_DEBUG) {
                LOG.debug("After init deal, the writerSliceConfig now is:[\n{}\n]",
                        this.writerSliceConfig.toJSON());
            }
        }

        @Override
        public void prepare() {
            LOG.info("prepare()");
        }

        // ref:http://odps.alibaba-inc.com/doc/prddoc/odps_tunnel/odps_tunnel_examples.html#id4
        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            UploadSession uploadSession = OdpsUtil.getSlaveSession(this.odps, this.writerSliceConfig);

            List<Integer> positions = this.writerSliceConfig.getList(Constant.COLUMN_POSITION, Integer.class);

            try {
                LOG.info("Session status:[{}]", uploadSession.getStatus()
                        .toString());
            } catch (Exception e) {
                LOG.error("Failed to get session status.", e);
                throw new DataXException(OdpsWriterErrorCode.GET_SESSION_STATUS_FAIL, e);
            }


            try {
                WriterProxy writerProxy = new WriterProxy(recordReceiver, uploadSession,
                        positions, this.blockId, super.getSlavePluginCollector());
                writerProxy.doWrite();
            } catch (Exception e) {
                LOG.error("Failed to write odps record.", e);
                throw new DataXException(OdpsWriterErrorCode.WRITER_RECORD_FAIL, e);
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

    }
}