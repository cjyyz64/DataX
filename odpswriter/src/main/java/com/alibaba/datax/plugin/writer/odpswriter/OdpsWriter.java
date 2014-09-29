package com.alibaba.datax.plugin.writer.odpswriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.StrUtil;
import com.alibaba.datax.plugin.writer.odpswriter.util.OdpsSplitUtil;
import com.alibaba.datax.plugin.writer.odpswriter.util.OdpsUtil;
import com.aliyun.odps.*;
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
            //TODO 无默认值
            Boolean truncate = this.originalConfig.getBool(Key.TRUNCATE);
            if (null == truncate) {
                throw new DataXException(OdpsWriterErrorCode.ILLEGAL_VALUE,
                        "key named truncate should configured and only be 'true' or 'false'.");
            } else {
                boolean isPartitionedTable = this.originalConfig
                        .getBool(Constant.IS_PARTITIONED_TABLE);
                if (truncate.booleanValue()) {
                    if (isPartitionedTable) {
                        String partition = this.originalConfig
                                .getString(Key.PARTITION);
                        LOG.info("Begin to clean partitioned table:[{}], partition:[{}].",
                                this.table.getName(), partition);
                        OdpsUtil.truncatePartition(this.table, partition,
                                this.originalConfig.getBool(Constant.IS_PARTITION_EXIST));

                        LOG.info("Finished clean partitioned table:[{}], partition:[{}].",
                                this.table.getName(), partition);
                    } else {
                        LOG.info("Begin to clean non partitioned table:[{}].",
                                this.table.getName());

                        OdpsUtil.truncateTable(this.table);
                        LOG.info("Finished clean non partitioned table:[{}].",
                                this.table.getName());
                    }
                }

                //注意，createMasterSession要求分区已经创建完成
                this.masterUploadSession = OdpsUtil.createMasterSession(this.odps,
                        this.originalConfig);
                this.uploadId = this.masterUploadSession.getId();
                LOG.info("UploadId:[{}]", this.uploadId);
            }


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
                String message = StrUtil.buildOriginalCauseMessage(String.format
                        ("Error while commit all blocks. uploadId:[%s]", this.uploadId), null);

                LOG.error(message);
                throw new DataXException(OdpsWriterErrorCode.COMMIT_BLOCK_FAIL, e);
            }
        }

        @Override
        public void destroy() {
            LOG.info("destroy()");
        }

        private void dealMaxRetryTime(Configuration originalConfig) {
            int maxRetryTime = originalConfig.getInt(Key.MAX_RETRY_TIME, Constant.DEFAULT_MAX_RETRY_TIME);
            if (maxRetryTime < 1) {
                String bussinessMessage = "maxRetryTime should >=1.";
                String message = StrUtil.buildOriginalCauseMessage(bussinessMessage, null);

                LOG.error(message);
                throw new DataXException(OdpsWriterErrorCode.ILLEGAL_VALUE, bussinessMessage);
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

                if (StringUtils.isBlank(userConfiguredPartition)) {
                    String message = StrUtil.buildOriginalCauseMessage(String.format(
                            "Lost partition value, table:[%s] is partitioned.",
                            table.getName()), null);

                    LOG.error(message);
                    throw new DataXException(OdpsWriterErrorCode.REQUIRED_VALUE, message);
                } else {
                    //TODO 需要添加对分区级数校验的方法

                    String standardUserConfiguredPartition = OdpsUtil
                            .formatPartition(userConfiguredPartition);
                    boolean isPartitionExist = false;
                    try {
                        isPartitionExist = this.table.hasPartition(new PartitionSpec(standardUserConfiguredPartition));
                    } catch (OdpsException e) {
                        //TODO
                        e.printStackTrace();
                        throw new DataXException(OdpsWriterErrorCode.CHECK_TABLE_FAIL, e);
                    }

                    //TODO remove ?
                    String formattedUserConfiguredPartition = checkUserConfiguredPartition(this.table,
                            userConfiguredPartition);


                    originalConfig.set(Constant.IS_PARTITION_EXIST, isPartitionExist);

                    originalConfig.set(Key.PARTITION, standardUserConfiguredPartition);
                }
            } else {
                // 非分区表，则不能配置分区值
                userConfiguredPartition = originalConfig
                        .getString(Key.PARTITION);

                if (null == userConfiguredPartition) {
                    //nothing
                } else if (StringUtils.isBlank(userConfiguredPartition)) {
                    LOG.warn("It is better to remove key:partition because Table:[{}] is not partitioned",
                            table.getName());
                } else {
                    String bussinessMessage = String.format("Can not config partition, Table:[%s] is not partitioned, ",
                            table.getName());
                    String message = StrUtil.buildOriginalCauseMessage(bussinessMessage, null);

                    LOG.error(message);
                    throw new DataXException(OdpsWriterErrorCode.ILLEGAL_VALUE, bussinessMessage);
                }
            }
        }

        private String checkUserConfiguredPartition(Table table, String userConfiguredPartition) {
            // 对用户自身配置的所有分区进行特殊字符的处理
            String standardUserConfiguredPartition = OdpsUtil
                    .formatPartition(userConfiguredPartition);

            if ("*".equals(standardUserConfiguredPartition)) {
                // 不允许
                String businessMessage = "Partition can not be *.";
                String message = StrUtil.buildOriginalCauseMessage(businessMessage, null);

                LOG.error(message);
                throw new DataXException(OdpsWriterErrorCode.ILLEGAL_VALUE, businessMessage);
            }

            int tablePartitionDepth = OdpsUtil.getPartitionDepth(table);
            int userConfiguredPartitionDepth = userConfiguredPartition.split(",").length;
            if (tablePartitionDepth != userConfiguredPartitionDepth) {
                String businessMessage = String.format("Partition depth not equal. table:[%s] partition depth:[%s], user configured partition depth:[%s].",
                        tablePartitionDepth, userConfiguredPartition);
                String message = StrUtil.buildOriginalCauseMessage(businessMessage, null);

                LOG.error(message);
                throw new DataXException(OdpsWriterErrorCode.ILLEGAL_VALUE, businessMessage);
            }

            //返回的是：已经进行过特殊字符处理的分区配置值
            return standardUserConfiguredPartition;
        }

        private void dealColumn(Configuration originalConfig, Table table) {
            // 用户配置的 column
            List<String> userConfiguredColumns = originalConfig.getList(
                    Key.COLUMN, String.class);
            if (null == userConfiguredColumns || userConfiguredColumns.isEmpty()) {
                //缺失 column配置
                String businessMessage = String.format("Lost column config, table:[%s].",
                        table.getName());
                String message = StrUtil.buildOriginalCauseMessage(businessMessage, null);

                LOG.error(message);
                throw new DataXException(OdpsWriterErrorCode.REQUIRED_KEY, businessMessage);
            } else {
                List<Column> tableOriginalColumns = OdpsUtil.getTableAllColumns(table);
                LOG.info("tableAllColumn:[\n{}\n]", StringUtils.join(tableOriginalColumns, "\n"));

                List<String> tableOriginalColumnNameList = OdpsUtil.getTableOriginalColumnNameList(tableOriginalColumns);

                if (1 == userConfiguredColumns.size() && "*".equals(userConfiguredColumns.get(0))) {
                    LOG.info("Column configured as * is not recommend, DataX will convert it to tableOriginalColumns by order.");
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
            // 检查列是否重复 TODO 大小写
            List<String> tempUserConfiguredColumns = new ArrayList<String>(userConfiguredColumns);
            Collections.sort(tempUserConfiguredColumns);
            for (int i = 0, len = tempUserConfiguredColumns.size(); i < len - 1; i++) {
                if (tempUserConfiguredColumns.get(i).equalsIgnoreCase(tempUserConfiguredColumns.get(i + 1))) {
                    String businessMessage = String.format("Can not config duplicate column:[%s].",
                            tempUserConfiguredColumns.get(i));
                    String message = StrUtil.buildOriginalCauseMessage(businessMessage, null);

                    LOG.error(message);
                    throw new DataXException(OdpsWriterErrorCode.ILLEGAL_VALUE, businessMessage);
                }
            }

            // 检查列是否都来自于表(列名称大小写不敏感)
            List<String> lowerCaseUserConfiguredColumns = listToLowerCase(userConfiguredColumns);
            List<String> lowerCaseTableOriginalColumnNameList = listToLowerCase(tableOriginalColumnNameList);
            for (String aColumn : lowerCaseUserConfiguredColumns) {
                if (!lowerCaseTableOriginalColumnNameList.contains(aColumn)) {
                    String businessMessage = String.format("Can not find column:[%s].", aColumn);
                    String message = StrUtil.buildOriginalCauseMessage(businessMessage, null);

                    LOG.error(message);
                    throw new DataXException(OdpsWriterErrorCode.ILLEGAL_VALUE, businessMessage);
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

            //blockId 在 master 中已分配完成 TODO
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

            List<Integer> positions = this.writerSliceConfig.getList(Constant.COLUMN_POSITION,
                    Integer.class);

            try {
                LOG.info("Session status:[{}]", uploadSession.getStatus()
                        .toString());
            } catch (Exception e) {
                String message = StrUtil.buildOriginalCauseMessage("Failed to get session status.", e);

                LOG.error(message);
                throw new DataXException(OdpsWriterErrorCode.GET_SESSION_STATUS_FAIL, e);
            }


            try {
                WriterProxy writerProxy = new WriterProxy(recordReceiver, uploadSession,
                        positions, this.blockId, super.getSlavePluginCollector());
                writerProxy.doWrite();
            } catch (Exception e) {
                String message = StrUtil.buildOriginalCauseMessage("Failed to write odps record.", e);

                LOG.error(message);
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