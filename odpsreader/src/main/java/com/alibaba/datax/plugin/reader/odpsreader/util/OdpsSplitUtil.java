package com.alibaba.datax.plugin.reader.odpsreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.odpsreader.Constant;
import com.alibaba.datax.plugin.reader.odpsreader.Key;
import com.alibaba.datax.plugin.reader.odpsreader.OdpsReaderErrorCode;
import com.aliyun.odps.Odps;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;

import java.util.ArrayList;
import java.util.List;

public final class OdpsSplitUtil {

    public static List<Configuration> doSplit(Configuration originalConfig, Odps odps,
                                              int adviceNum) {
        boolean isPartitionedTable = originalConfig.getBool(Constant.IS_PARTITIONED_TABLE);
        if (isPartitionedTable) {
            // 分区表
            return splitPartitionedTable(odps, originalConfig, adviceNum);
        } else {
            // 非分区表
            return splitForNonPartitionedTable(odps, adviceNum, originalConfig);
        }

    }

    private static List<Configuration> splitPartitionedTable(Odps odps, Configuration originalConfig,
                                                             int adviceNum) {
        List<Configuration> splittedConfigs = new ArrayList<Configuration>();

        List<String> partitions = originalConfig.getList(Key.PARTITION,
                String.class);

        // TODO
        if (null == partitions || partitions.isEmpty()) {
            throw DataXException.asDataXException(OdpsReaderErrorCode.ILLEGAL_VALUE,
                    "您所配置的分区不能为空白.");
        }

        //splitMode 默认为 record
        String splitMode = originalConfig.getString(Key.SPLIT_MODE);
        Configuration tempConfig = null;
        if (partitions.size() > adviceNum || Constant.PARTITION_SPLIT_MODE.equals(splitMode)) {
            // 此时不管 splitMode 是什么，都不需要再进行切分了
            // 注意：此处没有把 sessionId 设置到 config 中去，所以后续在 slave 中获取 sessionId 时，需要针对这种情况重新创建 sessionId
            for (String onePartition : partitions) {
                tempConfig = originalConfig.clone();
                tempConfig.set(Key.PARTITION, onePartition);
                splittedConfigs.add(tempConfig);
            }

            return splittedConfigs;
        } else {
            // 还需要计算对每个分区，切分份数等信息
            int eachPartitionShouldSplittedNumber = calculateEachPartitionShouldSplittedNumber(
                    adviceNum, partitions.size());

            for (String onePartition : partitions) {
                List<Configuration> configs = splitOnePartition(odps,
                        onePartition, eachPartitionShouldSplittedNumber,
                        originalConfig);
                splittedConfigs.addAll(configs);
            }

            return splittedConfigs;
        }
    }

    // TODO Mysqlreader 中有这个方法，考虑抽象
    private static int calculateEachPartitionShouldSplittedNumber(
            int adviceNumber, int partitionNumber) {
        double tempNum = 1.0 * adviceNumber / partitionNumber;

        return (int) Math.ceil(tempNum);
    }

    private static List<Configuration> splitForNonPartitionedTable(Odps odps,
                                                                   int adviceNum, Configuration sliceConfig) {
        List<Configuration> params = new ArrayList<Configuration>();

        String tunnelServer = sliceConfig.getString(Key.TUNNEL_SERVER);
        String tableName = sliceConfig.getString(Key.TABLE);

        DownloadSession session = OdpsUtil.createMasterSessionForNonPartitionedTable(odps,
                tunnelServer, tableName);

        String id = session.getId();
        long count = session.getRecordCount();
        long start = 0;
        long end = 0;

        long step = count / adviceNum;

        // 先进行处理，以免 while 死循环
        if (step == 0) {
            // 不需要切分
            Configuration iParam = sliceConfig.clone();
            iParam.set(Constant.SESSION_ID, id);
            iParam.set(Constant.START_INDEX, start);
            iParam.set(Constant.STEP_COUNT, count);

            params.add(iParam);
            return params;
        }


        while (end < count) {
            end = start + step;
            if (end > count) {
                end = count;
            }

            Configuration iParam = sliceConfig.clone();
            iParam.set(Constant.SESSION_ID, id);
            iParam.set(Constant.START_INDEX, start);
            iParam.set(Constant.STEP_COUNT, end - start);

            params.add(iParam);
            start = end;
        }
        return params;
    }

    private static List<Configuration> splitOnePartition(Odps odps,
                                                         String onePartition, int adviceNum, Configuration sliceConfig) {
        List<Configuration> params = new ArrayList<Configuration>();

        String tunnelServer = sliceConfig.getString(Key.TUNNEL_SERVER);
        String tableName = sliceConfig.getString(Key.TABLE);

        DownloadSession session = OdpsUtil.createMasterSessionForPartitionedTable(odps,
                tunnelServer, tableName, onePartition);

        String id = session.getId();
        long count = session.getRecordCount();
        long start = 0;
        long end = 0;

        long step = count / adviceNum;

        // 先进行处理，以免 while 死循环
        if (step == 0) {
            // 不需要切分
            Configuration iParam = sliceConfig.clone();
            iParam.set(Key.PARTITION, onePartition);
            iParam.set(Constant.SESSION_ID, id);
            iParam.set(Constant.START_INDEX, start);
            iParam.set(Constant.STEP_COUNT, count);

            params.add(iParam);
            return params;
        }

        while (end < count) {
            end = start + step;
            if (end > count) {
                end = count;
            }

            Configuration iParam = sliceConfig.clone();
            iParam.set(Key.PARTITION, onePartition);
            iParam.set(Constant.SESSION_ID, id);
            iParam.set(Constant.START_INDEX, start);
            iParam.set(Constant.STEP_COUNT, end - start);

            params.add(iParam);
            start = end;
        }
        return params;
    }

}
