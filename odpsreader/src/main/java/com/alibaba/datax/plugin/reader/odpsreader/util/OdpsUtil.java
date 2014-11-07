package com.alibaba.datax.plugin.reader.odpsreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
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
            throw DataXException.asDataXException(OdpsReaderErrorCode.REQUIRED_VALUE, "您未配置读取源头表的列信息. " +
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
            odps.tables().exists(tableName);
        } catch (OdpsException e) {
            throw DataXException.asDataXException(OdpsReaderErrorCode.ILLEGAL_VALUE,
                    String.format("项目:%s 中的表:%s 不存在.", odps.getDefaultProject(), tableName), e);
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

    public static List<OdpsType> getTableOriginalColumnTypeList(
            List<Column> columns) {
        List<OdpsType> tableOriginalColumnTypeList = new ArrayList<OdpsType>();

        for (Column column : columns) {
            tableOriginalColumnTypeList.add(column.getType());
        }

        return tableOriginalColumnTypeList;
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

    public static List<String> parseConstantColumn(List<String> tableOriginalColumnList,
                                                   List<String> userConfiguredColumns) {
        List<String> retList = new ArrayList<String>(tableOriginalColumnList);
        for (String col : userConfiguredColumns) {
            if (checkIfConstantColumn(col)) {
                retList.add(col.substring(1, col.length() - 1));
            }
        }
        return retList;
    }

    public static boolean checkIfConstantColumn(String column) {
        if (column.length() >= 2 && column.startsWith(Constant.COLUMN_CONSTANT_FLAG) &&
                column.endsWith(Constant.COLUMN_CONSTANT_FLAG)) {
            return true;
        } else {
            return false;
        }
    }

    public static List<Integer> parsePosition(List<String> allColumnList,
                                              List<String> userConfiguredColumns) {
        List<Integer> retList = new ArrayList<Integer>();

        boolean hasColumn = false;
        for (String col : userConfiguredColumns) {
            hasColumn = false;
            for (int i = 0, len = allColumnList.size(); i < len; i++) {
                if (allColumnList.get(i).equalsIgnoreCase(col)) {
                    retList.add(i);
                    hasColumn = true;
                    break;
                }
            }
            if (!hasColumn) {
                throw DataXException.asDataXException(OdpsReaderErrorCode.ILLEGAL_VALUE,
                        String.format("读取源头表的列配置错误. 您所配置的列:%s 不存在.", col));
            }
        }
        return retList;
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
