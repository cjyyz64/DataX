package com.alibaba.datax.plugin.reader.odpsreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.odpsreader.ColumnType;
import com.alibaba.datax.plugin.reader.odpsreader.Constant;
import com.alibaba.datax.plugin.reader.odpsreader.Key;
import com.alibaba.datax.plugin.reader.odpsreader.OdpsReaderErrorCode;
import com.aliyun.odps.*;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.account.TaobaoAccount;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class OdpsUtil {
    private static final Logger LOG = LoggerFactory.getLogger(OdpsUtil.class);

    public static void checkNecessaryConfig(Configuration originalConfig) {
        originalConfig.getNecessaryValue(Key.ODPS_SERVER, OdpsReaderErrorCode.REQUIRED_VALUE);

        originalConfig.getNecessaryValue(Key.PROJECT, OdpsReaderErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(Key.TABLE, OdpsReaderErrorCode.REQUIRED_VALUE);

        if (null == originalConfig.getList(Key.COLUMN) ||
                originalConfig.getList(Key.COLUMN, String.class).isEmpty()) {
            throw DataXException.asDataXException(OdpsReaderErrorCode.REQUIRED_VALUE, "datax获取不到源表的列信息， 由于您未配置读取源头表的列信息. datax无法知道该抽取表的哪些字段的数据 " +
                    "正确的配置方式是给 column 配置上您需要读取的列名称,用英文逗号分隔.");
        }

    }

    public static Odps initOdps(Configuration originalConfig) {
        String odpsServer = originalConfig.getString(Key.ODPS_SERVER);

        String accessId = originalConfig.getString(Key.ACCESS_ID);
        String accessKey = originalConfig.getString(Key.ACCESS_KEY);
        String project = originalConfig.getString(Key.PROJECT);

        String accountType = originalConfig.getString(Key.ACCOUNT_TYPE,
                Constant.DEFAULT_ACCOUNT_TYPE);

        Account account = null;
        if (accountType.equalsIgnoreCase(Constant.DEFAULT_ACCOUNT_TYPE)) {
            account = new AliyunAccount(accessId, accessKey);
        } else if (accountType.equalsIgnoreCase(Constant.TAOBAO_ACCOUNT_TYPE)) {
            account = new TaobaoAccount(accessId, accessKey);
        } else {
            throw DataXException.asDataXException(OdpsReaderErrorCode.ILLEGAL_VALUE,
                    String.format("不支持的账号类型:[%s]. 账号类型目前仅支持aliyun, taobao.", accountType));
        }

        Odps odps = new Odps(account);
        odps.setDefaultProject(project);
        odps.setEndpoint(odpsServer);

        return odps;
    }

    public static Table getTable(Odps odps, String tableName) {
        Table table = null;
        try {
            table = odps.tables().get(tableName);

            //通过这种方式检查表是否存在
            table.reload();
        } catch (OdpsException e) {
            throw DataXException.asDataXException(OdpsReaderErrorCode.ILLEGAL_VALUE,
                    String.format("加载 ODPS 源头表:%s 失败. " +
                            "请检查您配置的 ODPS 源头表的 project,table,accessId,accessKey,odpsServer等值.", tableName), e);
        }

        return table;
    }

    public static boolean isPartitionedTable(Table table) {
        return getPartitionDepth(table) > 0;
    }

    public static int getPartitionDepth(Table table) {
        TableSchema tableSchema = table.getSchema();

        return tableSchema.getPartitionColumns().size();
    }

    public static List<String> getTableAllPartitions(Table table) {
        List<Partition> tableAllPartitions = table.getPartitions();

        List<String> retPartitions = new ArrayList<String>();

        if (null != tableAllPartitions) {
            for (Partition partition : tableAllPartitions) {
                retPartitions.add(partition.getPartitionSpec().toString());
            }
        }

        return retPartitions;
    }

    public static List<Column> getTableAllColumns(Table table) {
        TableSchema tableSchema = table.getSchema();
        return tableSchema.getColumns();
    }


    public static List<String> getTableOriginalColumnNameList(
            List<Column> columns) {
        List<String> tableOriginalColumnNameList = new ArrayList<String>();

        for (Column column : columns) {
            tableOriginalColumnNameList.add(column.getName());
        }

        return tableOriginalColumnNameList;
    }

    public static String formatPartition(String partition) {
        if (StringUtils.isBlank(partition)) {
            throw DataXException.asDataXException(OdpsReaderErrorCode.ILLEGAL_VALUE,
                    "您所配置的分区不能为空白.");
        } else {
            return partition.trim().replaceAll(" *= *", "=")
                    .replaceAll(" */ *", ",").replaceAll(" *, *", ",")
                    .replaceAll("'", "");
        }
    }

    public static List<String> formatPartitions(List<String> partitions) {
        if (null == partitions || partitions.isEmpty()) {
            return Collections.emptyList();
        } else {
            List<String> formattedPartitions = new ArrayList<String>();
            for (String partition : partitions) {
                formattedPartitions.add(formatPartition(partition));

            }
            return formattedPartitions;
        }
    }

    public static List<Pair<String, ColumnType>> parseColumns(
            List<String> allNormalColumns, List<String> allPartitionColumns,
            List<String> userConfiguredColumns) {
        List<Pair<String, ColumnType>> parsededColumns = new ArrayList<Pair<String, ColumnType>>();
        // warn: upper & lower case
        for (String column : userConfiguredColumns) {
            MutablePair<String, ColumnType> pair = new MutablePair<String, ColumnType>();
            // if constant column
            if (OdpsUtil.checkIfConstantColumn(column)) {
                // remove first and last '
                pair.setLeft(column.substring(1, column.length() - 1));
                pair.setRight(ColumnType.CONSTANT);
            }
            // if normal column, warn: in o d p s normal columns can not
            // repeated in partitioning columns
            else if (OdpsUtil.checkContains(allNormalColumns, column)) {
                pair.setLeft(column);
                pair.setRight(ColumnType.NORMAL);
            }
            // if partition column
            else if (OdpsUtil.checkContains(allPartitionColumns, column)) {
                pair.setLeft(column);
                pair.setRight(ColumnType.PARTITION);
            }
            // not exist column
            else {
                throw DataXException.asDataXException(
                        OdpsReaderErrorCode.ILLEGAL_VALUE,
                        String.format("源头表的列配置错误. 您所配置的列 [%s] 不存在.", column));
            }
            parsededColumns.add(pair);
        }
        return parsededColumns;
    }
    
    private static boolean checkContains(List<String> columnCollection,
            String column) {
        for (String eachCol : columnCollection) {
            if (eachCol.equalsIgnoreCase(column)) {
                return true;
            }
        }
        return false;
    }

    public static boolean checkIfConstantColumn(String column) {
        if (column.length() >= 2 && column.startsWith(Constant.COLUMN_CONSTANT_FLAG) &&
                column.endsWith(Constant.COLUMN_CONSTANT_FLAG)) {
            return true;
        } else {
            return false;
        }
    }

    public static TableTunnel.DownloadSession createMasterSessionForNonPartitionedTable(Odps odps,
                                                                                        String tunnelServer, String tableName) {

        TableTunnel tunnel = new TableTunnel(odps);
        if (StringUtils.isNoneBlank(tunnelServer)) {
            tunnel.setEndpoint(tunnelServer);
        }

        TableTunnel.DownloadSession downloadSession = null;
        try {
            downloadSession = tunnel.createDownloadSession(
                    odps.getDefaultProject(), tableName);
        } catch (TunnelException e) {
            throw DataXException.asDataXException(OdpsReaderErrorCode.CREATE_DOWNLOADSESSION_FAIL, e);
        }

        return downloadSession;
    }

    public static TableTunnel.DownloadSession getSlaveSessionForNonPartitionedTable(Odps odps, String sessionId,
                                                                                    String tunnelServer, String tableName) {

        TableTunnel tunnel = new TableTunnel(odps);
        if (StringUtils.isNoneBlank(tunnelServer)) {
            tunnel.setEndpoint(tunnelServer);
        }

        TableTunnel.DownloadSession downloadSession = null;
        try {
            downloadSession = tunnel.getDownloadSession(
                    odps.getDefaultProject(), tableName, sessionId);
        } catch (TunnelException e) {
            throw DataXException.asDataXException(OdpsReaderErrorCode.GET_DOWNLOADSESSION_FAIL, e);
        }

        return downloadSession;
    }

    public static TableTunnel.DownloadSession createMasterSessionForPartitionedTable(Odps odps,
                                                                                     String tunnelServer, String tableName, String partition) {

        TableTunnel tunnel = new TableTunnel(odps);
        if (StringUtils.isNoneBlank(tunnelServer)) {
            tunnel.setEndpoint(tunnelServer);
        }

        TableTunnel.DownloadSession downloadSession = null;

        PartitionSpec partitionSpec = new PartitionSpec(partition);

        try {
            downloadSession = tunnel.createDownloadSession(
                    odps.getDefaultProject(), tableName, partitionSpec);
        } catch (TunnelException e) {
            throw DataXException.asDataXException(OdpsReaderErrorCode.CREATE_DOWNLOADSESSION_FAIL, e);
        }

        return downloadSession;
    }

    public static TableTunnel.DownloadSession getSlaveSessionForPartitionedTable(Odps odps, String sessionId,
                                                                                 String tunnelServer, String tableName, String partition) {

        TableTunnel tunnel = new TableTunnel(odps);
        if (StringUtils.isNoneBlank(tunnelServer)) {
            tunnel.setEndpoint(tunnelServer);
        }

        TableTunnel.DownloadSession downloadSession = null;

        PartitionSpec partitionSpec = new PartitionSpec(partition);

        try {
            downloadSession = tunnel.getDownloadSession(
                    odps.getDefaultProject(), tableName, partitionSpec, sessionId);
        } catch (TunnelException e) {
            throw DataXException.asDataXException(OdpsReaderErrorCode.GET_DOWNLOADSESSION_FAIL, e);
        }

        return downloadSession;
    }
}
