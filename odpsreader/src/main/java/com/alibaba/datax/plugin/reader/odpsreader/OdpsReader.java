package com.alibaba.datax.plugin.reader.odpsreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.FilterUtil;
import com.alibaba.datax.plugin.reader.odpsreader.util.OdpsSplitUtil;
import com.alibaba.datax.plugin.reader.odpsreader.util.OdpsUtil;
import com.aliyun.odps.*;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class OdpsReader extends Reader {
    public static class Master extends Reader.Master {
        private static final Logger LOG = LoggerFactory
                .getLogger(OdpsReader.Master.class);

        private static boolean IS_DEBUG = LOG.isDebugEnabled();

        private Configuration originalConfig;

        @Override
        public void init() {
            this.originalConfig = this.getPluginJobConf();

            // 最大尝试次数校验，默认值为3
            OdpsUtil.dealMaxRetryTime(this.originalConfig);

            Odps odps = OdpsUtil.initOdps(this.originalConfig);

            String tableName = this.originalConfig.getNecessaryValue(Key.TABLE,
                    OdpsReaderErrorCode.NOT_SUPPORT_TYPE);

            Table table = null;
            try {
                table = OdpsUtil.getTable(odps, tableName);
                this.originalConfig.set(Constant.IS_PARTITIONED_TABLE,
                        OdpsUtil.isPartitionedTable(table));
            } catch (Exception e) {
                throw DataXException.asDataXException(OdpsReaderErrorCode.RUNTIME_EXCEPTION,
                        e);
            }

            boolean isVirtualView = table.isVirtualView();
            if (isVirtualView) {
                throw DataXException.asDataXException(
                        OdpsReaderErrorCode.NOT_SUPPORT_TYPE,
                        String.format(
                                "Table:[%s] is Virtual View, DataX not support to read data from it.",
                                tableName));
            }

            dealPartition(this.originalConfig, table);

            dealColumn(this.originalConfig, table);
        }

        /**
         * 对分区的配置处理。最终效果是所有正则配置，完全展开成实际对应的分区配置。正则规则如下：
         * <p/>
         * <ol>
         * <li>如果是分区表，则必须配置分区：可以配置为*,表示整表读取；也可以配置为分别列出要读取的叶子分区. </br>TODO
         * 未来会支持一些常用的分区正则筛选配置. 分区配置中，不能在分区所表示的数组中配置多个*，因为那样就是多次读取全表，无意义.</li>
         * <li>如果是非分区表，则不能配置分区值.</li>
         * </ol>
         */
        private void dealPartition(Configuration originalConfig, Table table) {
            List<String> userConfiguredPartitions = originalConfig.getList(
                    Key.PARTITION, String.class);

            boolean isPartitionedTable = originalConfig
                    .getBool(Constant.IS_PARTITIONED_TABLE);

            if (isPartitionedTable) {
                // 分区表，需要配置分区
                if (null == userConfiguredPartitions
                        || userConfiguredPartitions.isEmpty()) {
                    throw DataXException.asDataXException(
                            OdpsReaderErrorCode.NOT_SUPPORT_TYPE,
                            String.format(
                                    "Table:[%s] is partitioned, so you need config its partition.",
                                    table.getName()));
                } else {
                    // TODO 考虑把MAX_RETRY_TIME 设置到OdpsUtil中
                    List<String> allPartitions = OdpsUtil
                            .getTableAllPartitions(table,
                                    originalConfig.getInt(Key.MAX_RETRY_TIME));

                    if (null == allPartitions || allPartitions.isEmpty()) {
                        throw DataXException.asDataXException(OdpsReaderErrorCode.RUNTIME_EXCEPTION,
                                String.format("Table:[%s] is partitioned, but partition value is empty.", table.getName()));
                    }

                    List<String> parsedPartitions = expandUserConfiguredPartition(
                            allPartitions, userConfiguredPartitions);

                    if (null == parsedPartitions || parsedPartitions.isEmpty()) {
                        throw DataXException.asDataXException(
                                OdpsReaderErrorCode.NOT_SUPPORT_TYPE,
                                String.format(
                                        "Can not find matched partition. all partitions:[\n%s\n], you configed partition:[\n%s\n].",
                                        StringUtils.join(allPartitions, "\n"),
                                        StringUtils.join(
                                                userConfiguredPartitions, "\n")));
                    }
                    originalConfig.set(Key.PARTITION, parsedPartitions);
                }
            } else {
                // 非分区表，则不能配置分区
                if (null != userConfiguredPartitions
                        && !userConfiguredPartitions.isEmpty()) {
                    throw DataXException.asDataXException(
                            OdpsReaderErrorCode.NOT_SUPPORT_TYPE,
                            String.format(
                                    "Can not config partition, Table:[%s] is not partitioned, ",
                                    table.getName()));
                }
            }

        }

        private List<String> expandUserConfiguredPartition(
                List<String> allPartitions, List<String> userConfiguredPartitions) {
            // 对odps 本身的所有分区进行特殊字符的处理
            List<String> allStandardPartitions = OdpsUtil
                    .formatPartitions(allPartitions);

            // 对用户自身配置的所有分区进行特殊字符的处理
            List<String> allStandardUserConfiguredPartitions = OdpsUtil
                    .formatPartitions(userConfiguredPartitions);

            if (userConfiguredPartitions.indexOf("*") > 0) {
                // *要么分区只配置一个*，表示全表拖取；不允许在其他位置单独配置一个*
                throw DataXException.asDataXException(OdpsReaderErrorCode.NOT_SUPPORT_TYPE,
                        "* means read the whole table. you can not read one table >1 times.");
            }

            // 处理配置为*的情况，代表全表拖取，直接返回表所有分区即可（当然，是处理了分区中的特殊字符之后的）
            if (1 == userConfiguredPartitions.size()
                    && "*".equals(userConfiguredPartitions.get(0))) {
                return allStandardPartitions;
            } else {
                List<String> retPartitions = FilterUtil.filterByRegulars(
                        allStandardPartitions,
                        allStandardUserConfiguredPartitions);

                List<String> tempCheckPartitions = new ArrayList<String>(
                        retPartitions);
                Collections.sort(tempCheckPartitions);
                for (int i = 0, len = tempCheckPartitions.size(); i < len - 1; i++) {
                    if (tempCheckPartitions.get(i).equalsIgnoreCase(
                            tempCheckPartitions.get(i + 1))) {
                        throw DataXException.asDataXException(
                                OdpsReaderErrorCode.NOT_SUPPORT_TYPE,
                                String.format(
                                        "Partition:[%s] choose more than one time.",
                                        tempCheckPartitions.get(i)));
                    }
                }
                return retPartitions;
            }
        }

        //TODO 对列格式进行调整
        private void dealColumn(Configuration originalConfig, Table table) {
            // 用户配置的 column
            List<String> userConfiguredColumns = this.originalConfig.getList(
                    Key.COLUMN, String.class);

            List<Column> allColumns = OdpsUtil.getTableAllColumns(table);
            List<String> tableOriginalColumnNameList = OdpsUtil
                    .getTableOriginalColumnNameList(allColumns);

            LOG.info("Table:[{}] all columns:[{}]", table.getName(),
                    StringUtils.join(tableOriginalColumnNameList, ","));

            if (null == userConfiguredColumns || userConfiguredColumns.isEmpty()) {
                LOG.warn("column configured to be blank is not recommended. " +
                        "Because it may not work when you changed your table structure.");
                originalConfig.set(Key.COLUMN, tableOriginalColumnNameList);
            } else if (1 == userConfiguredColumns.size()
                    && "*".equals(userConfiguredColumns.get(0))) {
                LOG.warn("column * is not recommended. " +
                        "Because it may not work when you changed your table structure.");
                originalConfig.set(Key.COLUMN, tableOriginalColumnNameList);
            }

            List<String> firstParsedColumns = this.originalConfig.getList(
                    Key.COLUMN, String.class);

            // 把字符串常量，加到表原生字段tableOriginalColumnNameList 列表后，为下一轮解析 position
            // 做准备
            List<String> allColumnParsedWithConstant = OdpsUtil
                    .parseConstantColumn(tableOriginalColumnNameList,
                            firstParsedColumns);

            if (IS_DEBUG) {
                LOG.debug("allColumnParsedWithConstant: {} .", allColumnParsedWithConstant);
            }
            List<Integer> columnPositions = OdpsUtil.parsePosition(
                    allColumnParsedWithConstant, firstParsedColumns);
            if (IS_DEBUG) {
                LOG.debug("columnPositionList: {} .", columnPositions);
            }

            originalConfig.set(Constant.ALL_COLUMN_PARSED_WITH_CONSTANT,
                    allColumnParsedWithConstant);
            originalConfig.set(Constant.COLUMN_POSITION, columnPositions);
        }

        @Override
        public void prepare() {
        }

        @Override
        public List<Configuration> split(int adviceNumber) {

            return OdpsSplitUtil.doSplit(this.originalConfig, adviceNumber);
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }

    public static class Slave extends Reader.Slave {
        private static final Logger LOG = LoggerFactory
                .getLogger(OdpsReader.Slave.class);
        private Configuration readerSliceConf;

        private String tunnelServer;

        private String table = null;

        private Odps odps = null;
        private boolean isPartitionedTable;

        @Override
        public void init() {
            this.readerSliceConf = getPluginJobConf();
            this.tunnelServer = this.readerSliceConf.getString(
                    Key.TUNNEL_SERVER, null);

            this.table = this.readerSliceConf.getString(Key.TABLE);

            this.odps = OdpsUtil.initOdps(this.readerSliceConf);
            this.isPartitionedTable = this.readerSliceConf
                    .getBool(Constant.IS_PARTITIONED_TABLE);
        }

        @Override
        public void prepare() {
        }

        @Override
        public void startRead(RecordSender recordSender) {
            DownloadSession downloadSession = null;
            String partition = this.readerSliceConf.getString(Key.PARTITION);

            if (this.isPartitionedTable) {
                downloadSession = OdpsSplitUtil
                        .getSessionForPartitionedTable(this.odps,
                                this.tunnelServer, this.table, partition);
            } else {
                downloadSession = OdpsSplitUtil
                        .getSessionForNonPartitionedTable(this.odps,
                                this.tunnelServer, this.table);
            }

            long start = this.readerSliceConf.getLong(Constant.START_INDEX, 0);
            long count = this.readerSliceConf.getLong(Constant.STEP_COUNT,
                    downloadSession.getRecordCount());

            if (count > 0) {
                LOG.info(String.format(
                        "Table:[%s],partition:[%s],start:[%s],totalCount:[%s].",
                        this.table, partition, start, count));
            } else if (0 == count) {
                LOG.warn(String
                        .format("Table:[%s],partition:[%s],totalCount:[%s]. no need to read it.",
                                this.table, partition, start));
                return;
            } else {
                throw DataXException.asDataXException(OdpsReaderErrorCode.READ_DATA_FAIL,
                        "TotalCount should >= 0.");
            }

            TableSchema tableSchema = downloadSession.getSchema();

            List<OdpsType> tableOriginalColumnTypeList = OdpsUtil
                    .getTableOriginalColumnTypeList(tableSchema.getColumns());

            try {
                RecordReader recordReader = downloadSession.openRecordReader(
                        start, count);
                ReaderProxy readerProxy = new ReaderProxy(recordSender,
                        recordReader, downloadSession.getSchema(),
                        this.readerSliceConf, tableOriginalColumnTypeList);

                readerProxy.doRead();
            } catch (Exception e) {
                throw DataXException.asDataXException(OdpsReaderErrorCode.READ_DATA_FAIL, e);
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
