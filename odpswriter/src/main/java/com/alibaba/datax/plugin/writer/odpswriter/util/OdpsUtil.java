package com.alibaba.datax.plugin.writer.odpswriter.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.datax.plugin.writer.odpswriter.Constant;
import com.alibaba.datax.plugin.writer.odpswriter.Key;
import com.alibaba.datax.plugin.writer.odpswriter.OdpsWriterErrorCode;
import com.alibaba.odps.tunnel.*;
import com.aliyun.openservices.odps.ODPSConnection;
import com.aliyun.openservices.odps.Project;
import com.aliyun.openservices.odps.jobs.*;
import com.aliyun.openservices.odps.jobs.TaskStatus.Status;
import com.aliyun.openservices.odps.tables.Table;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

public class OdpsUtil {
    private static final Logger LOG = LoggerFactory.getLogger(OdpsUtil.class);

    public static int MAX_RETRY_TIME = 4;

    public static void checkNecessaryConfig(Configuration originalConfig) {
        originalConfig.getNecessaryValue(Key.ODPS_SERVER,
                OdpsWriterErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(Key.TUNNEL_SERVER,
                OdpsWriterErrorCode.REQUIRED_VALUE);

        originalConfig.getNecessaryValue(Key.PROJECT,
                OdpsWriterErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(Key.TABLE,
                OdpsWriterErrorCode.REQUIRED_VALUE);

        if (null == originalConfig.getList(Key.COLUMN) ||
                originalConfig.getList(Key.COLUMN, String.class).isEmpty()) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.REQUIRED_VALUE, "您未配置写入 ODPS 目的表的列信息. " +
                    "正确的配置方式是给datax的 column 项配置上您需要读取的列名称,用英文逗号分隔 例如:  \"column\": [\"id\",\"name\"].");
        }

        // getBool 内部要求，值只能为 true,false 的字符串（大小写不敏感），其他一律报错，不再有默认配置
        Boolean truncate = originalConfig.getBool(Key.TRUNCATE);
        if (null == truncate) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.REQUIRED_VALUE, "由于您未配置写入 ODPS 目的表前是否清空表/分区，所以任务失败了 " +
                    "请您增加 truncate 的配置，根据业务需要选择上true 或者 false.");
        }
    }

    public static void dealMaxRetryTime(Configuration originalConfig) {
        int maxRetryTime = originalConfig.getInt(Key.MAX_RETRY_TIME,
                OdpsUtil.MAX_RETRY_TIME);
        if (maxRetryTime < 1) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.ILLEGAL_VALUE, "您所配置的maxRetryTime 值错误. 该值不能小于1. " +
                    "推荐的配置方式是给maxRetryTime 配置2-5之间的某个值. 请您检查配置并做出相应修改.");
        }

        MAX_RETRY_TIME = maxRetryTime;
    }

    public static String formatPartition(String partitionString) {
        if (null == partitionString) {
            return null;
        }

        return partitionString.trim().replaceAll(" *= *", "=").replaceAll(" */ *", ",")
                .replaceAll(" *, *", ",").replaceAll("'", "");
    }

    public static DataTunnel initDataTunnel(Configuration originalConfig) {
        String accountType = originalConfig.getString(Key.ACCOUNT_TYPE);
        String accessId = originalConfig.getString(Key.ACCESS_ID);
        String accessKey = originalConfig.getString(Key.ACCESS_KEY);

        String tunnelServer = originalConfig.getString(Key.TUNNEL_SERVER);

        Account tunnelAccount = new Account(accountType, accessId, accessKey);
        if (Constant.TAOBAO_ACCOUNT_TYPE.equalsIgnoreCase(accountType)) {
            tunnelAccount.setAlgorithm(Constant.TAOBAO_ACCOUNT_TYPE_ALGORITHM);
        }

        com.alibaba.odps.tunnel.Configuration tunnelConfig =
                new com.alibaba.odps.tunnel.Configuration(tunnelAccount, tunnelServer);
        return new DataTunnel(tunnelConfig);
    }

    public static Project initOdpsProject(Configuration originalConfig) {
        String accountType = originalConfig.getString(Key.ACCOUNT_TYPE);
        String accessId = originalConfig.getString(Key.ACCESS_ID);
        String accessKey = originalConfig.getString(Key.ACCESS_KEY);

        String odpsServer = originalConfig.getString(Key.ODPS_SERVER);
        String project = originalConfig.getString(Key.PROJECT);

        com.aliyun.openservices.odps.Account odpsAccount = new com.aliyun.openservices.odps.Account(
                accountType, accessId, accessKey);
        if (Constant.TAOBAO_ACCOUNT_TYPE.equalsIgnoreCase(accountType)) {
            odpsAccount.setAlgorithm(Constant.TAOBAO_ACCOUNT_TYPE_ALGORITHM);
        }

        return new Project(new ODPSConnection(odpsServer, odpsAccount), project);
    }

    public static List<String> listOdpsPartitions(Table table) {
        List<String> parts;
        try {
            parts = table.listPartitions();
            if (null == parts) {
                throw DataXException.asDataXException(OdpsWriterErrorCode.GET_PARTITION_FAIL, String.format("获取 ODPS 目的表:%s 的所有分区失败. 请联系 ODPS 管理员处理.",
                        table.getName()));
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.GET_PARTITION_FAIL, String.format("获取 ODPS 目的表:%s 的所有分区失败. 请联系 ODPS 管理员处理.",
                    table.getName()), e);
        }
        return parts;
    }

    public static boolean isPartitionedTable(Table table) {
        //必须要是非分区表才能 truncate 整个表
        List<com.aliyun.openservices.odps.tables.Column> partitionKeys;
        try {
            partitionKeys = table.getPartitionKeys();
            if (null != partitionKeys && !partitionKeys.isEmpty()) {
                return true;
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.CHECK_IF_PARTITIONED_TABLE_FAILED,
                    String.format("检查 ODPS 目的表:%s 是否为分区表失败, 请联系 ODPS 管理员处理.", table.getName()), e);
        }
        return false;
    }


    public static void truncateNonPartitionedTable(Table tab) {
        String truncateNonPartitionedTableSql = "truncate table " + tab.getName() + ";";

        try {
            runSqlTask(tab.getProject(), truncateNonPartitionedTableSql);
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.TABLE_TRUNCATE_ERROR,
                    String.format(" 清空 ODPS 目的表:%s 失败, 请联系 ODPS 管理员处理.", tab.getName()), e);
        }
    }

    public static void truncatePartition(Table table, String partition) {
        if (isPartitionExist(table, partition)) {
            dropPart(table, partition);
        }
        addPart(table, partition);
    }

    private static boolean isPartitionExist(Table table, String partition) {
        // check if exist partition 返回值不为 null
        List<String> odpsParts = OdpsUtil.listOdpsPartitions(table);

        int j = 0;
        for (; j < odpsParts.size(); j++) {
            if (odpsParts.get(j).replaceAll("'", "").equals(partition)) {
                break;
            }
        }

        return j != odpsParts.size();
    }

    public static void addPart(Table table, String partition) {
        String partSpec = getPartSpec(partition);
        // add if not exists partition
        StringBuilder addPart = new StringBuilder();
        addPart.append("alter table ").append(table.getName()).append(" add IF NOT EXISTS partition(")
                .append(partSpec).append(");");
        try {
            runSqlTask(table.getProject(), addPart.toString());
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.ADD_PARTITION_FAILED,
                    String.format("添加 ODPS 目的表的分区失败. 错误发生在添加 ODPS 的项目:%s 的表:%s 的分区:%s. 请联系 ODPS 管理员处理.",
                            table.getProject().getName(), table.getName(), partition), e);
        }
    }


    public static Upload createMasterTunnelUpload(final DataTunnel tunnel, final String project,
                                                  final String table, final String partition) {
        try {
            return RetryUtil.executeWithRetry(new Callable<Upload>() {
                @Override
                public Upload call() throws Exception {
                    return tunnel.createUpload(project, table, partition);
                }
            }, MAX_RETRY_TIME, 1000L, true);
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.CREATE_MASTER_UPLOAD_FAIL,
                    "创建TunnelUpload失败. 请联系 ODPS 管理员处理.", e);
        }
    }

    public static Upload getSlaveTunnelUpload(final DataTunnel tunnel, final String project, final String table,
                                              final String partition, final String uploadId) {
        try {
            return RetryUtil.executeWithRetry(new Callable<Upload>() {
                @Override
                public Upload call() throws Exception {
                    return tunnel.createUpload(project, table, partition, uploadId);
                }
            }, MAX_RETRY_TIME, 1000L, true);

        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.GET_SLAVE_UPLOAD_FAIL,
                    "获取TunnelUpload失败. 请联系 ODPS 管理员处理.", e);
        }
    }


    private static void dropPart(Table table, String partition) {
        String partSpec = getPartSpec(partition);
        StringBuilder dropPart = new StringBuilder();
        dropPart.append("alter table ").append(table.getName())
                .append(" drop IF EXISTS partition(").append(partSpec)
                .append(");");
        try {
            runSqlTask(table.getProject(), dropPart.toString());
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.ADD_PARTITION_FAILED,
                    String.format("Drop  ODPS 目的表分区失败. 错误发生在项目:%s 的表:%s 的分区:%s .请联系 ODPS 管理员处理.",
                            table.getProject().getName(), table.getName(), partition), e);
        }
    }

    private static String getPartSpec(String partition) {
        StringBuilder partSpec = new StringBuilder();
        String[] parts = partition.split(",");
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            String[] kv = part.split("=");
            if (kv.length != 2) {
                throw DataXException.asDataXException(OdpsWriterErrorCode.ILLEGAL_VALUE,
                        String.format("ODPS 目的表自身的 partition:%s 格式不对. 正确的格式形如: pt=1,ds=hangzhou", partition));
            }
            partSpec.append(kv[0]).append("=");
            partSpec.append("'").append(kv[1].replace("'", "")).append("'");
            if (i != parts.length - 1) {
                partSpec.append(",");
            }
        }
        return partSpec.toString();
    }


    private static void runSqlTask(Project project, String query) throws Exception {
        if (StringUtils.isBlank(query)) {
            return;
        }

        String taskName = "datax_odpswriter_trunacte_" + UUID.randomUUID().toString().replace('-', '_');

        LOG.info("Try to start sqlTask:[{}] to run odps sql:[\n{}\n] .", taskName, query);
        Task task = new SqlTask(taskName, query);
        JobInstance instance = Job.run(project, task);
        instance.waitForCompletion();
        TaskStatus status = instance.getTaskStatus().get(taskName);

        if (!Status.SUCCESS.equals(status.getStatus())) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.RUN_SQL_FAILED,
                    String.format("ODPS 目的表在运行 ODPS SQL失败, 返回值为:%s. 请联系 ODPS 管理员处理. SQL 内容为:[\n%s\n].", instance.getResult().get(taskName),
                            query));
        }
    }

    public static void masterCompleteBlocks(final Upload masterUpload, final Long[] blocks) {
        try {
            RetryUtil.executeWithRetry(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    masterUpload.complete(blocks);
                    return null;
                }
            }, MAX_RETRY_TIME, 1000L, true);
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.COMMIT_BLOCK_FAIL,
                    String.format("ODPS 目的表在提交 block:[\n%s\n] 时失败, uploadId=[%s]. 请联系 ODPS 管理员处理.", StringUtils.join(blocks, ","), masterUpload.getUploadId()), e);
        }
    }

    public static void slaveWriteOneBlock(final Upload slaveUpload, final ByteArrayOutputStream byteArrayOutputStream,
                                          final long blockId) {
        try {
            RetryUtil.executeWithRetry(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    OutputStream hpout = slaveUpload.openOutputStream(blockId);
                    byteArrayOutputStream.writeTo(hpout);
                    hpout.close();
                    return null;
                }
            }, MAX_RETRY_TIME, 1000L, true);
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.WRITER_RECORD_FAIL,
                    String.format("ODPS 目的表写 block:%s 失败， uploadId=[%s]. 请联系 ODPS 管理员处理.", blockId, slaveUpload.getUploadId()), e);
        }

    }

    public static List<Integer> parsePosition(List<String> allColumnList,
                                              List<String> userConfiguredColumns) {
        List<Integer> retList = new ArrayList<Integer>();

        boolean hasColumn;
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
                throw DataXException.asDataXException(OdpsWriterErrorCode.ILLEGAL_VALUE,
                        String.format("ODPS 目的表的列配置错误. 由于您所配置的列:%s 不存在，会导致datax无法正常插入数据，请检查该列是否存在，如果存在请检查大小写等配置.", col));
            }
        }
        return retList;
    }

    public static List<String> getAllColumns(RecordSchema schema) {
        if (null == schema) {
            throw new IllegalArgumentException("parameter schema can not be null.");
        }
//todo
        List<String> allColumns = new ArrayList<String>();
        Column.Type type;
        for (int i = 0, columnCount = schema.getColumnCount(); i < columnCount; i++) {
            allColumns.add(schema.getColumnName(i));
            type = schema.getColumnType(i);
            if (type == Column.Type.ODPS_ARRAY || type == Column.Type.ODPS_MAP) {
                throw DataXException.asDataXException(OdpsWriterErrorCode.UNSUPPORTED_COLUMN_TYPE,
                        String.format("DataX 写入 ODPS 表不支持该字段类型:[%s]. 目前支持抽取的字段类型有：bigint, boolean, datetime, double, string. " +
                                        "您可以选择不抽取 DataX 不支持的字段或者联系 ODPS 管理员寻求帮助.",
                                type));
            }
        }
        return allColumns;
    }

    public static List<Column.Type> getTableOriginalColumnTypeList(RecordSchema schema) {
        List<Column.Type> tableOriginalColumnTypeList = new ArrayList<Column.Type>();

        for (int i = 0, columnCount = schema.getColumnCount(); i < columnCount; i++) {
            tableOriginalColumnTypeList.add(schema.getColumnType(i));
        }

        return tableOriginalColumnTypeList;
    }

    public static void dealTruncate(Table table, String partition, boolean truncate) {
        boolean isPartitionedTable = OdpsUtil.isPartitionedTable(table);

        if (truncate) {
            //需要 truncate
            if (isPartitionedTable) {
                //分区表
                if (StringUtils.isBlank(partition)) {
                    throw DataXException.asDataXException(OdpsWriterErrorCode.ILLEGAL_VALUE, String.format("您没有配置分区信息，因为你配置的表是分区表:%s 如果需要进行 truncate 操作，必须指定需要清空的具体分区. 请修改分区配置，格式形如 pt=${bizdate} .",
                            table.getName()));
                } else {
                    LOG.info("Try to truncate partition=[{}] in table=[{}].", partition, table.getName());
                    OdpsUtil.truncatePartition(table, partition);
                }
            } else {
                //非分区表
                if (StringUtils.isNotBlank(partition)) {
                    throw DataXException.asDataXException(OdpsWriterErrorCode.ILLEGAL_VALUE, String.format("分区信息配置错误，你的ODPS表是非分区表:%s 进行 truncate 操作时不需要指定具体分区值. 请检查您的分区配置，删除该配置项的值.",
                            table.getName()));
                } else {
                    LOG.info("Try to truncate table:[{}].", table.getName());
                    OdpsUtil.truncateNonPartitionedTable(table);
                }
            }
        } else {
            //不需要 truncate
            if (isPartitionedTable) {
                //分区表
                if (StringUtils.isBlank(partition)) {
                    throw DataXException.asDataXException(OdpsWriterErrorCode.ILLEGAL_VALUE,
                            String.format("您的目的表是分区表，写入分区表:%s 时必须指定具体分区值. 请修改您的分区配置信息，格式形如 格式形如 pt=${bizdate}.", table.getName()));
                } else {
                    boolean isPartitionExists = OdpsUtil.isPartitionExist(table, partition);
                    if (!isPartitionExists) {
                        LOG.info("Try to add partition:[{}] in table:[{}] by drop it and then add it..", partition,
                                table.getName());
                        OdpsUtil.addPart(table, partition);
                    }
                }
            } else {
                //非分区表
                if (StringUtils.isNotBlank(partition)) {
                    throw DataXException.asDataXException(OdpsWriterErrorCode.ILLEGAL_VALUE,
                            String.format("您的目的表是非分区表，写入非分区表:%s 时不需要指定具体分区值. 请删除分区配置信息", table.getName()));
                }
            }
        }
    }

}
