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

//TODO: 换行符：System.getProperties("line.separator")方式获取。
public class OdpsWriter extends Writer {
    public static class Master extends Writer.Master {
        private static final Logger LOG = LoggerFactory
                .getLogger(OdpsWriter.Master.class);
        private static boolean IS_DEBUG = LOG.isDebugEnabled();

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

            boolean isVirtualView = this.table.isVirtualView();
            if (isVirtualView) {
                throw new DataXException(
                        OdpsWriterErrorCode.NOT_SUPPORT_TYPE,
                        String.format(
                                "Table:[%s] is virtual view, DataX not support to write data to it.",
                                this.table.getName()));
            }

            this.masterUploadSession = OdpsUtil.createMasterSession(this.odps, this.originalConfig);

            this.uploadId = this.masterUploadSession.getId();
            LOG.info("UploadId:[{}]", this.uploadId);

            dealPartition(this.originalConfig, this.table);

            dealColumn(this.originalConfig, this.table);
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
                    LOG.info(
                            "Begin to clean partitioned table:[{}], partition:[{}].",
                            this.table.getName(), partition);
                    OdpsUtil.truncatePartition(this.table, partition);
                } else {
                    LOG.info("Begin to clean non partitioned table:[{}].",
                            this.table.getName());

                    OdpsUtil.truncateTable(this.odps, this.table);
                }
            }
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            return OdpsSplitUtil.doSplit(this.originalConfig, this.uploadId, this.blockIds, mandatoryNumber);
        }


        @Override
        public void post() {
            LOG.info("Begin to commit all blocks. uploadId:[{}],blocks:[{}].", this.uploadId
                    , StringUtils.join(this.blockIds, "\n"));

            try {
                this.masterUploadSession.commit(blockIds.toArray(new Long[0]));
            } catch (Exception e) {
                e.printStackTrace();
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

            String userConfigedPartition = originalConfig
                    .getString(Key.PARTITION);

            if (isPartitionedTable) {
                // 分区表，需要配置分区
                if (null == userConfigedPartition) {
                    //缺失 Key:partition
                    throw new DataXException(
                            OdpsWriterErrorCode.REQUIRED_KEY,
                            String.format(
                                    "Lost key named partition, table:[%s] is partitioned.",
                                    table.getName()));
                } else if (StringUtils.isEmpty(userConfigedPartition)) {
                    //缺失 partition的值配置
                    throw new DataXException(
                            OdpsWriterErrorCode.REQUIRED_VALUE,
                            String.format(
                                    "Lost partition value, table:[%s] is partitioned.",
                                    table.getName()));
                } else {
                    List<String> allPartitions = OdpsUtil
                            .getTableAllPartitions(table,
                                    originalConfig.getInt(Key.MAX_RETRY_TIME));

                    String standardUserConfigedPartitions = checkUserConfigedPartition(
                            allPartitions, userConfigedPartition);

                    originalConfig.set(Key.PARTITION, standardUserConfigedPartitions);
                }
            } else {
                // 非分区表，则不能配置分区( 严格到不能出现 partition 这个 key)
                userConfigedPartition = originalConfig
                        .getString(Key.PARTITION);
                if (null != userConfigedPartition) {
                    throw new DataXException(
                            OdpsWriterErrorCode.NOT_SUPPORT_TYPE,
                            String.format(
                                    "Can not config partition, Table:[%s] is not partitioned, ",
                                    table.getName()));
                }
            }
        }

        private String checkUserConfigedPartition(
                List<String> allPartitions, String userConfigedPartition) {
            // 对odps 本身的所有分区进行特殊字符的处理
            List<String> allStandardPartitions = OdpsUtil
                    .formatPartitions(allPartitions);

            // 对用户自身配置的所有分区进行特殊字符的处理
            String standardUserConfigedPartitions = OdpsUtil
                    .formatPartition(userConfigedPartition);

            if ("*" .equals(standardUserConfigedPartitions)) {
                // 不允许
                throw new DataXException(OdpsWriterErrorCode.NOT_SUPPORT_TYPE,
                        "Partition can not be *.");
            }

            if (!allStandardPartitions.contains(userConfigedPartition)) {
                throw new DataXException(OdpsWriterErrorCode.NOT_SUPPORT_TYPE, String.format("Can not find partition:[%s] in all partitions:[\n%s\n].",
                        userConfigedPartition, StringUtils.join(allPartitions, "\n")));
            }

            return standardUserConfigedPartitions;
        }

        private void dealColumn(Configuration originalConfig, Table table) {
            // 用户配置的 column
            List<String> userConfigedColumns = this.originalConfig.getList(
                    Key.COLUMN, String.class);
            if (null == userConfigedColumns) {
                //缺失 Key:column
                throw new DataXException(
                        OdpsWriterErrorCode.REQUIRED_KEY,
                        String.format(
                                "Lost key named column, table:[%s].",
                                table.getName()));
            } else if (userConfigedColumns.isEmpty()) {
                //缺失 column 的值配置
                throw new DataXException(
                        OdpsWriterErrorCode.REQUIRED_VALUE,
                        String.format(
                                "Lost column value, table:[%s].",
                                table.getName()));
            } else {
                List<Column> columns = OdpsUtil.getTableAllColumns(table);
                LOG.info("tableAllColumn:[{}]", columns);

                LOG.info("Column configured as * is not recommend, DataX will convert it.");
                List<String> tableOriginalColumnNameList = OdpsUtil.getTableOriginalColumnNameList(columns);

                if (1 == userConfigedColumns.size() && "*" .equals(userConfigedColumns.get(0))) {
                    //处理 * 配置，替换为：odps 表所有列
                    this.originalConfig.set(Key.COLUMN, tableOriginalColumnNameList);
                    List<Integer> positions = new ArrayList<Integer>();
                    for (int i = 0, len = tableOriginalColumnNameList.size(); i < len; i++) {
                        positions.add(i);
                    }
                    this.originalConfig.set(Constant.COLUMN_POSITION, positions);
                } else {
                    /**
                     * 处理配置为["column0","column1"]的情况。
                     *
                     * <p>
                     *     检查列名称是否都是来自表本身，以及不允许列重复等；然后解析其顺序。
                     * </p>
                     */

                    doCheckColumn(userConfigedColumns, tableOriginalColumnNameList);

                    List<Integer> positions = OdpsUtil.parsePosition(
                            userConfigedColumns, tableOriginalColumnNameList);
                    this.originalConfig.set(Constant.COLUMN_POSITION, positions);
                }
            }

        }

        private void doCheckColumn(List<String> userConfigedColumns, List<String> tableOriginalColumnNameList) {
            //检查列是否重复
            List<String> tempUserConfigedCoumns = new ArrayList<String>(userConfigedColumns);
            Collections.sort(tempUserConfigedCoumns);
            for (int i = 0, len = tempUserConfigedCoumns.size(); i < len - 1; i++) {
                if (tempUserConfigedCoumns.get(i).equalsIgnoreCase(tempUserConfigedCoumns.get(i + 1))) {
                    throw new DataXException(OdpsWriterErrorCode.ILLEGAL_VALUE, String.format("Can not config duplicate column:[%s]",
                            tempUserConfigedCoumns.get(i)));
                }
            }

            //检查列是否都来自于表(列名称大小写不敏感)
            List<String> lowerCaseUserConfigedColumns = listToLowerCase(userConfigedColumns);
            List<String> lowerCaseTableOriginalColumnNameList = listToLowerCase(tableOriginalColumnNameList);
            for (String aColumn : lowerCaseUserConfigedColumns) {
                if (!lowerCaseTableOriginalColumnNameList.contains(aColumn)) {
                    throw new DataXException(OdpsWriterErrorCode.ILLEGAL_VALUE, String.format("Can not find column:[%s] in all column:[%s].",
                            aColumn, StringUtils.join(tableOriginalColumnNameList, ",")));
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

        private Configuration writerSliceConf;
        private String tunnelServer;

        private Odps odps = null;
        private String table = null;
        private boolean isPartitionedTable;
        private long blockId;
        private String uploadId;

        @Override
        public void init() {
            this.writerSliceConf = getPluginJobConf();
            this.tunnelServer = this.writerSliceConf.getString(
                    Key.TUNNEL_SERVER, null);

            this.odps = OdpsUtil.initOdps(this.writerSliceConf);
            this.table = this.writerSliceConf.getString(Key.TABLE);

            this.isPartitionedTable = this.writerSliceConf
                    .getBool(Constant.IS_PARTITIONED_TABLE);

            //blockId 在 master 中已分配完成
            this.blockId = this.writerSliceConf.getLong(Constant.BLOCK_ID);
            this.uploadId = this.writerSliceConf.getString(Constant.UPLOAD_ID);
        }

        @Override
        public void prepare() {
            LOG.info("prepare()");
        }

        // ref:http://odps.alibaba-inc.com/doc/prddoc/odps_tunnel/odps_tunnel_examples.html#id4
        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            UploadSession uploadSession = OdpsUtil.getSlaveSession(this.odps, this.writerSliceConf);

            List<Integer> positions = this.writerSliceConf.getList(Constant.COLUMN_POSITION, Integer.class);

            try {
                LOG.info("Session status:[{}]", uploadSession.getStatus()
                        .toString());
                WriterProxy writerProxy = new WriterProxy(recordReceiver, uploadSession,
                        positions, this.blockId);
                writerProxy.doWrite();
            } catch (Exception e) {
                LOG.error("error when startWrite.", e);
                throw new DataXException(OdpsWriterErrorCode.RUNTIME_EXCEPTION, e);
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