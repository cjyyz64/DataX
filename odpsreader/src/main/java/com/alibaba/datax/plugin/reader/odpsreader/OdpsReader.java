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
import java.util.List;

public class OdpsReader extends Reader {
    public static class Master extends Reader.Master {
        private static final Logger LOG = LoggerFactory
                .getLogger(OdpsReader.Master.class);

        private static boolean IS_DEBUG = LOG.isDebugEnabled();

        private Configuration originalConfig;
        private Odps odps;
        private Table table;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            OdpsUtil.checkNecessaryConfig(this.originalConfig);

            dealSplitMode(this.originalConfig);

            this.odps = OdpsUtil.initOdps(this.originalConfig);
            String tableName = this.originalConfig.getString(Key.TABLE);

            this.table = OdpsUtil.getTable(this.odps, tableName);
            this.originalConfig.set(Constant.IS_PARTITIONED_TABLE,
                    OdpsUtil.isPartitionedTable(table));

            boolean isVirtualView = this.table.isVirtualView();
            if (isVirtualView) {
                throw DataXException.asDataXException(OdpsReaderErrorCode.ILLEGAL_VALUE,
                        String.format("源头表:%s 是虚拟视图，DataX 不支持读取虚拟视图.", tableName));
            }

            dealPartition(this.originalConfig, this.table);

            dealColumn(this.originalConfig, this.table);
        }

        private void dealSplitMode(Configuration originalConfig) {
            String splitMode = originalConfig.getString(Key.SPLIT_MODE, Constant.DEFAULT_SPLIT_MODE).trim();
            if (splitMode.equalsIgnoreCase(Constant.DEFAULT_SPLIT_MODE) ||
                    splitMode.equalsIgnoreCase(Constant.PARTITION_SPLIT_MODE)) {
                originalConfig.set(Key.SPLIT_MODE, splitMode);
            } else {
                throw DataXException.asDataXException(OdpsReaderErrorCode.ILLEGAL_VALUE,
                        String.format("您所配置的 splitMode:%s 不正确. splitMode 仅允许配置为 record 或者 partition.", splitMode));
            }
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

            boolean isPartitionedTable = originalConfig.getBool(Constant.IS_PARTITIONED_TABLE);

            if (isPartitionedTable) {
                // 分区表，需要配置分区
                if (null == userConfiguredPartitions || userConfiguredPartitions.isEmpty()) {
                    throw DataXException.asDataXException(OdpsReaderErrorCode.ILLEGAL_VALUE,
                            String.format("源头表:%s 为分区表,所以您需要配置其抽取的分区信息. 格式为:pt=hello,ds=hangzhou",
                                    table.getName()));
                } else {
                    List<String> allPartitions = OdpsUtil.getTableAllPartitions(table);

                    if (null == allPartitions || allPartitions.isEmpty()) {
                        throw DataXException.asDataXException(OdpsReaderErrorCode.ILLEGAL_VALUE,
                                String.format("源头表:%s 虽然为分区表, 但其实际分区值并不存在, 请先在源头表生成数据后，再进行数据抽取.",
                                        table.getName()));
                    }

                    List<String> parsedPartitions = expandUserConfiguredPartition(
                            allPartitions, userConfiguredPartitions);

                    if (null == parsedPartitions || parsedPartitions.isEmpty()) {
                        throw DataXException.asDataXException(
                                OdpsReaderErrorCode.ILLEGAL_VALUE,
                                String.format(
                                        "分区配置错误，根据您所配置的分区没有匹配到源头表中的分区. 源头表所有分区是:[\n%s\n], 您配置的分区是:[\n%s\n].",
                                        StringUtils.join(allPartitions, "\n"),
                                        StringUtils.join(userConfiguredPartitions, "\n")));
                    }
                    originalConfig.set(Key.PARTITION, parsedPartitions);
                }
            } else {
                // 非分区表，则不能配置分区
                if (null != userConfiguredPartitions
                        && !userConfiguredPartitions.isEmpty()) {
                    throw DataXException.asDataXException(OdpsReaderErrorCode.ILLEGAL_VALUE,
                            String.format("源头表:%s 为非分区表, 您不能配置分区.", table.getName()));
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

            /**
             *  对配置的分区级数(深度)进行检查
             *  (1)先检查用户配置的分区级数,自身级数是否相等
             *  (2)检查用户配置的分区级数是否与源头表的的分区级数一样
             */
            String firstPartition = allStandardUserConfiguredPartitions.get(0);
            int firstPartitionDepth = firstPartition.split(",").length;

            String comparedPartition = null;
            int comparedPartitionDepth = -1;
            for (int i = 1, len = allStandardUserConfiguredPartitions.size(); i < len; i++) {
                comparedPartition = allStandardUserConfiguredPartitions.get(i);
                comparedPartitionDepth = comparedPartition.split(",").length;
                if (comparedPartitionDepth != firstPartitionDepth) {
                    throw DataXException.asDataXException(OdpsReaderErrorCode.ILLEGAL_VALUE,
                            String.format("分区配置错误, 您所配置的分区级数不一样, 比如分区:[%s] 是 %s 级分区, 而分区:[%s] 是 %s 级分区. DataX 是通过英文逗号判断您所配置的分区级数的.",
                                    firstPartition, firstPartitionDepth, comparedPartition, comparedPartitionDepth));
                }
            }

            int tableOriginalPartitionDepth = allStandardPartitions.get(0).split(",").length;
            if (firstPartitionDepth != tableOriginalPartitionDepth) {
                throw DataXException.asDataXException(OdpsReaderErrorCode.ILLEGAL_VALUE,
                        String.format("分区配置错误, 您所配置的分区:%s 的级数:%s 与您要读取的 ODPS 源头表的分区级数:%s 不相等. DataX 是通过英文逗号判断您所配置的分区级数的.",
                                firstPartition, firstPartitionDepth, tableOriginalPartitionDepth));
            }

            List<String> retPartitions = FilterUtil.filterByRegulars(allStandardPartitions,
                    allStandardUserConfiguredPartitions);

            return retPartitions;
        }

        private void dealColumn(Configuration originalConfig, Table table) {
            // 用户配置的 column
            List<String> userConfiguredColumns = originalConfig.getList(
                    Key.COLUMN, String.class);

            List<Column> allColumns = OdpsUtil.getTableAllColumns(table);
            List<String> tableOriginalColumnNameList = OdpsUtil
                    .getTableOriginalColumnNameList(allColumns);

            StringBuilder columnMeta = new StringBuilder();
            for (Column column : allColumns) {
                columnMeta.append(column.getName()).append(":").append(column.getType()).append(",");
            }
            columnMeta.setLength(columnMeta.length() - 1);

            LOG.info("源头表:{} 的所有字段是:[{}]", table.getName(), columnMeta.toString());

            if (null == userConfiguredColumns || userConfiguredColumns.isEmpty()) {
                LOG.warn("您未配置 ODPS 读取的列，这是不推荐的行为，因为当您的表字段个数、类型有变动时，可能影响任务正确性甚至会运行出错。");
                originalConfig.set(Key.COLUMN, tableOriginalColumnNameList);
            } else if (1 == userConfiguredColumns.size()
                    && "*".equals(userConfiguredColumns.get(0))) {
                LOG.warn("您配置的 ODPS 读取的列为*，这是不推荐的行为，因为当您的表字段个数、类型有变动时，可能影响任务正确性甚至会运行出错。");
                originalConfig.set(Key.COLUMN, tableOriginalColumnNameList);
            }

            userConfiguredColumns = this.originalConfig.getList(
                    Key.COLUMN, String.class);

            /**
             * 把字符串常量，加到表原生字段tableOriginalColumnNameList 列表后，
             * 为下一轮解析 position 做准备
             */
            List<String> allColumnParsedWithConstant = OdpsUtil.parseConstantColumn(tableOriginalColumnNameList,
                    userConfiguredColumns);

            if (IS_DEBUG) {
                LOG.debug("allColumnParsedWithConstant: {} .", allColumnParsedWithConstant);
            }

            // 去除常量中的首尾标识
            List<String> userConfiguredColumnWhichWithoutConstantHeadAndTailMark = new ArrayList<String>();
            for (String column : userConfiguredColumns) {
                if (OdpsUtil.checkIfConstantColumn(column)) {
                    userConfiguredColumnWhichWithoutConstantHeadAndTailMark.add(column.substring(1, column.length() - 1));
                } else {
                    userConfiguredColumnWhichWithoutConstantHeadAndTailMark.add(column);
                }
            }

            List<Integer> columnPositions = OdpsUtil.parsePosition(
                    allColumnParsedWithConstant, userConfiguredColumnWhichWithoutConstantHeadAndTailMark);

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
            return OdpsSplitUtil.doSplit(this.originalConfig, this.odps, adviceNumber);
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }

    public static class Slave extends Reader.Slave {
        private static final Logger LOG = LoggerFactory.getLogger(OdpsReader.Slave.class);
        private Configuration readerSliceConf;

        private String tunnelServer;
        private Odps odps = null;
        private String tableName = null;
        private boolean isPartitionedTable;
        private String sessionId;

        @Override
        public void init() {
            this.readerSliceConf = super.getPluginJobConf();
            this.tunnelServer = this.readerSliceConf.getString(
                    Key.TUNNEL_SERVER, null);

            this.odps = OdpsUtil.initOdps(this.readerSliceConf);

            this.tableName = this.readerSliceConf.getString(Key.TABLE);
            this.isPartitionedTable = this.readerSliceConf
                    .getBool(Constant.IS_PARTITIONED_TABLE);
            this.sessionId = this.readerSliceConf.getString(Constant.SESSION_ID, null);

            // sessionId 为空的情况是：切分级别只到 partition 的情况
            if (StringUtils.isBlank(this.sessionId)) {
                DownloadSession session = OdpsUtil.createMasterSessionForPartitionedTable(odps,
                        tunnelServer, tableName, this.readerSliceConf.getString(Key.PARTITION));
                this.sessionId = session.getId();
            }

            LOG.info("sessionId:{}", this.sessionId);
        }

        @Override
        public void prepare() {
        }

        @Override
        public void startRead(RecordSender recordSender) {
            DownloadSession downloadSession = null;
            String partition = this.readerSliceConf.getString(Key.PARTITION);

            if (this.isPartitionedTable) {
                downloadSession = OdpsUtil.getSlaveSessionForPartitionedTable(this.odps, this.sessionId,
                        this.tunnelServer, this.tableName, partition);
            } else {
                downloadSession = OdpsUtil.getSlaveSessionForNonPartitionedTable(this.odps, this.sessionId,
                        this.tunnelServer, this.tableName);
            }

            long start = this.readerSliceConf.getLong(Constant.START_INDEX, 0);
            long count = this.readerSliceConf.getLong(Constant.STEP_COUNT,
                    downloadSession.getRecordCount());

            if (count > 0) {
                LOG.info(String.format(
                        "准备读取源头表:%s 的分区:%s ,起始下标为:%s , 此次需要抽取行数为:%s .",
                        this.tableName, partition, start, count));
            } else if (count == 0) {
                LOG.warn(String.format("源头表:%s 的分区:%s 没有内容可抽取, 请您知晓.",
                        this.tableName, partition));
                return;
            } else {
                throw DataXException.asDataXException(OdpsReaderErrorCode.READ_DATA_FAIL,
                        String.format("源头表:%s 的分区:%s  读取行数为负数, 请联系 ODPS 管理员查看表状态!",
                                this.tableName, partition));
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
                throw DataXException.asDataXException(OdpsReaderErrorCode.READ_DATA_FAIL,
                        String.format("源头表:%s 的分区:%s 读取失败, 请联系 ODPS 管理员查看错误详情.", this.tableName, partition), e);
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
