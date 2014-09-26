package com.alibaba.datax.plugin.writer.odpswriter.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.odpswriter.Constant;
import com.alibaba.datax.plugin.writer.odpswriter.Key;
import com.alibaba.datax.plugin.writer.odpswriter.OdpsWriterErrorCode;
import com.aliyun.odps.*;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.account.TaobaoAccount;
import com.aliyun.odps.task.SQLTask;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;
import java.util.Map.Entry;

public final class OdpsUtil {
    private static final Logger LOG = LoggerFactory.getLogger(OdpsUtil.class);

    public static Odps initOdps(Configuration originalConfig) {
        String odpsServer = originalConfig.getNecessaryValue(Key.ODPS_SERVER,
                OdpsWriterErrorCode.NOT_SUPPORT_TYPE);

        String accessId = originalConfig.getNecessaryValue(Key.ACCESS_ID,
                OdpsWriterErrorCode.NOT_SUPPORT_TYPE);
        String accessKey = originalConfig.getNecessaryValue(Key.ACCESS_KEY,
                OdpsWriterErrorCode.NOT_SUPPORT_TYPE);
        String project = originalConfig.getNecessaryValue(Key.PROJECT,
                OdpsWriterErrorCode.NOT_SUPPORT_TYPE);

        String accountType = originalConfig.getString(Key.ACCOUNT_TYPE,
                Constant.DEFAULT_ACCOUNT_TYPE);

        Account account = null;
        if (accountType.equalsIgnoreCase(Constant.DEFAULT_ACCOUNT_TYPE)) {
            account = new AliyunAccount(accessId, accessKey);
        } else if (accountType.equalsIgnoreCase("taobao")) {
            account = new TaobaoAccount(accessId, accessKey);
        } else {
            throw new DataXException(OdpsWriterErrorCode.NOT_SUPPORT_TYPE,
                    String.format("Unsupport account type:[%s].", accountType));
        }

        Odps odps = new Odps(account);
        odps.setDefaultProject(project);
        odps.setEndpoint(odpsServer);

        return odps;
    }

    //处理逻辑是：如果分区存在，则先删除分区，再重建分区；如果分区不存在，则直接创建分区。
    public static void truncatePartition(Table table, String partition) {
        PartitionSpec part = new PartitionSpec(partition);

        boolean isPartExist = table.getPartition(part) != null;
        if (isPartExist) {
            try {
                table.deletePartition(new PartitionSpec(partition));
            } catch (OdpsException e) {
                //TODO
                e.printStackTrace();
            }
        }

        try {
            table.createPartition(new PartitionSpec(partition));
        } catch (OdpsException e) {
            String errorMsg = String.format("error when truncate partition:[%s], table:[%s]",
                    partition, table.getName());
            LOG.error(errorMsg, e);
        }
    }

    public static void truncateTable(Odps odps, Table table) {
        String dropDdl = "truncate table " + table.getName() + ";";
        try {
            SQLTask.run(odps, dropDdl);
        } catch (OdpsException e) {
            LOG.error(String.format("error when truncate table. SQL:[%s].", dropDdl), e);
            new DataXException(OdpsWriterErrorCode.NOT_SUPPORT_TYPE, e);
        }
    }

    // TODO retry
    public static Table getTable(Odps odps, String tableName) {
        Table table = odps.tables().get(tableName);

        return table;
    }

    public static boolean isPartitionedTable(Table table) {
        TableSchema tableSchema = table.getSchema();

        return tableSchema.getPartitionColumns().size() > 0;
    }

    public static List<String> getTableAllPartitions(Table table, int retryTime) {
        List<Partition> tableAllPartitions = null;

        for (int i = 0; i < retryTime; i++) {
            try {
                tableAllPartitions = table.getPartitions();
            } catch (Exception e) {
                if (i < retryTime) {
                    LOG.warn("try to list odps partitions for {} times.", i + 1);
                    continue;
                } else {
                    throw new DataXException(
                            OdpsWriterErrorCode.RUNTIME_EXCEPTION, e);
                }
            }

            if (null != tableAllPartitions) {
                break;
            }
        }
        List<String> retPartitions = new ArrayList<String>();

        for (Partition partition : tableAllPartitions) {
            retPartitions.add(partition.getPartitionSpec().toString());
        }

        return retPartitions;
    }

    public static List<Column> getTableAllColumns(Table table) {
        TableSchema tableSchema = table.getSchema();
        List<Column> columns = tableSchema.getColumns();

        return columns;
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

    public static Map<String, String> getOdpsProp(String filePath) {
        Map<String, String> propsMap = new HashMap<String, String>();

        Properties props = new Properties();
        try {
            InputStream in = new BufferedInputStream(new FileInputStream(
                    filePath));
            props.load(in);
            Set<Entry<Object, Object>> sets = props.entrySet();
            for (Entry<Object, Object> e : sets) {
                propsMap.put(String.valueOf(e.getKey()),
                        String.valueOf(e.getValue()));
            }
        } catch (Exception e) {
            throw new DataXException(OdpsWriterErrorCode.NOT_SUPPORT_TYPE, e);
        }
        return propsMap;
    }

    public static String formatPartition(String partition) {
        if (StringUtils.isBlank(partition)) {
            throw new DataXException(OdpsWriterErrorCode.NOT_SUPPORT_TYPE,
                    "bad partition which is blank.");
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

    public static List<Integer> parsePosition(List<String> allColumnList,
                                              List<String> userConfigedColumns) {
        List<Integer> retList = new ArrayList<Integer>();

        boolean hasColumn = false;
        for (String col : userConfigedColumns) {
            hasColumn = false;
            for (int i = 0, len = allColumnList.size(); i < len; i++) {
                if (allColumnList.get(i).equalsIgnoreCase(col)) {
                    retList.add(i);
                    hasColumn = true;
                    break;
                }
            }
            if (!hasColumn) {
                throw new DataXException(OdpsWriterErrorCode.NOT_SUPPORT_TYPE,
                        String.format("no column named [%s] !", col));
            }
        }
        return retList;
    }

}
