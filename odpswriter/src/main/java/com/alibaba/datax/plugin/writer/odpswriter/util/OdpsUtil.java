package com.alibaba.datax.plugin.writer.odpswriter.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.datax.common.util.StrUtil;
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
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
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
            String bussinessMessage = "Lost config Key:column.";
            String message = StrUtil.buildOriginalCauseMessage(
                    bussinessMessage, null);
            LOG.error(message);

            throw new DataXException(OdpsWriterErrorCode.ILLEGAL_VALUE, bussinessMessage);
        }

        // getBool 内部要求，值只能为 true,false 的字符串（大小写不敏感），其他一律报错，不再有默认配置
        Boolean truncate = originalConfig.getBool(Key.TRUNCATE);
        if (null == truncate) {
            String bussinessMessage = "Lost config Key:truncate.";
            String message = StrUtil.buildOriginalCauseMessage(
                    bussinessMessage, null);
            LOG.error(message);

            throw new DataXException(OdpsWriterErrorCode.REQUIRED_VALUE, bussinessMessage);
        }
    }

    public static void dealMaxRetryTime(Configuration originalConfig) {
        int maxRetryTime = originalConfig.getInt(Key.MAX_RETRY_TIME,
                OdpsUtil.MAX_RETRY_TIME);
        if (maxRetryTime < 1) {
            String bussinessMessage = "maxRetryTime should >=1.";
            String message = StrUtil.buildOriginalCauseMessage(
                    bussinessMessage, null);

            LOG.error(message);
            throw new DataXException(OdpsWriterErrorCode.ILLEGAL_VALUE,
                    bussinessMessage);
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

        com.alibaba.odps.tunnel.Configuration tunnelConfig = new com.alibaba.odps.tunnel.Configuration(tunnelAccount, tunnelServer);
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
        List<String> parts = null;
        try {
            parts = table.listPartitions();
        } catch (Exception e) {
            throw new DataXException(OdpsWriterErrorCode.TEMP, "Error when list table partitions." + e);
        }
        return parts;
    }

    public static boolean isPartitionedTable(Table table) {
        //必须要是非分区表才能 truncate 整个表
        List<com.aliyun.openservices.odps.tables.Column> partitionKeys = null;
        try {
            partitionKeys = table.getPartitionKeys();
            if (null != partitionKeys && !partitionKeys.isEmpty()) {
                return true;
            }
        } catch (Exception e) {
            String bussinessMessage = String.format("Check if partitioned table failed. detail: table=[%s].",
                    table.getName());
            String message = StrUtil.buildOriginalCauseMessage(bussinessMessage, null);
            LOG.error(message);

            throw new DataXException(OdpsWriterErrorCode.CHECK_IF_PARTITIONED_TABLE_FAILED, e);
        }
        return false;
    }


    public static void truncateTable(Table tab) {
        //必须要是非分区表才能 truncate 整个表
        List<com.aliyun.openservices.odps.tables.Column> partitionKeys = null;
        try {
            partitionKeys = tab.getPartitionKeys();
        } catch (Exception e) {
            throw new DataXException(OdpsWriterErrorCode.TEMP, e);
        }

        if (null != partitionKeys && !partitionKeys.isEmpty()) {
            String bussinessMessage = String.format("Can not truncate partitioned table. detail: table=[%s] has partition=[%s].",
                    tab.getName(), StringUtils.join(partitionKeys, ","));
            String message = StrUtil.buildOriginalCauseMessage(bussinessMessage, null);
            LOG.error(message);

            throw new DataXException(OdpsWriterErrorCode.TABLE_TRUNCATE_ERROR, bussinessMessage);
        }

        try {
            tab.load();
        } catch (Exception e) {
            String bussinessMessage = String.format("Can not load table. detail: table=[%s]. detail:[%s].",
                    tab.getName(), e.getMessage());
            String message = StrUtil.buildOriginalCauseMessage(bussinessMessage, null);
            LOG.error(message);

            throw new DataXException(OdpsWriterErrorCode.TABLE_TRUNCATE_ERROR, e);
        }

        String dropDdl = "drop table IF EXISTS " + tab.getName() + ";";
        String ddl = OdpsUtil.getTableDdl(tab);

        try {
            runSqlTask(tab.getProject(), dropDdl);
            runSqlTask(tab.getProject(), ddl);
        } catch (Exception e) {
            String bussinessMessage = String.format("Truncate table:[%s] failed. detail:[%s].",
                    tab.getName(), e.getMessage());
            String message = StrUtil.buildOriginalCauseMessage(bussinessMessage, null);
            LOG.error(message);

            throw new DataXException(OdpsWriterErrorCode.TABLE_TRUNCATE_ERROR, e);
        }
    }

    public static void truncatePartition(Project odpsProject, String table, String partition) {
        if (isPartitionExists(odpsProject, table, partition)) {
            dropPart(odpsProject, table, partition);
        }
        addPart(odpsProject, table, partition);
    }

    public static boolean isPartitionExists(Project odpsProject, String table, String partition) {
        Table tbl = new Table(odpsProject, table);
        // check if exist partition
        List<String> odpsParts = OdpsUtil.listOdpsPartitions(tbl);
        if (null == odpsParts) {
            throw new DataXException(null, "Error when list table partitions.");
        }
        int j = 0;
        for (j = 0; j < odpsParts.size(); j++) {
            if (odpsParts.get(j).replaceAll("'", "").equals(partition)) {
                break;
            }
        }
        if (j == odpsParts.size()) {
            return false;
        }

        return true;
    }

    public static void addPart(Project odpsProject, String table, String partition) {
        String partSpec = getPartSpec(partition);
        // add if not exists partition
        StringBuilder addPart = new StringBuilder();
        addPart.append("alter table ").append(table).append(" add IF NOT EXISTS partition(")
                .append(partSpec).append(");");
        try {
            runSqlTask(odpsProject, addPart.toString());
        } catch (Exception e) {
            String bussinessMessage = String.format("addPartition failed. detail:project=[%s], table=[%s],partition=[%s]."
                    , odpsProject.getName(), table, partition);
            String message = StrUtil.buildOriginalCauseMessage(bussinessMessage, null);
            LOG.error(message);

            throw new DataXException(OdpsWriterErrorCode.ADD_PARTITION_FAILED, bussinessMessage);
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
            throw new DataXException(OdpsWriterErrorCode.CREATE_MASTER_UPLOAD_FAIL, e);
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
            throw new DataXException(OdpsWriterErrorCode.GET_SLAVE_UPLOAD_FAIL, e);
        }
    }


    private static void dropPart(Project odpsProject, String table, String partition) {
        String partSpec = getPartSpec(partition);
        StringBuilder dropPart = new StringBuilder();
        dropPart.append("alter table ").append(table)
                .append(" drop IF EXISTS partition(").append(partSpec)
                .append(");");
        try {
            runSqlTask(odpsProject, dropPart.toString());
        } catch (Exception e) {
            String bussinessMessage = String.format("dropPartition failed. detail:project=[%s], table=[%s],partition=[%s]."
                    , odpsProject.getName(), table, partition);
            String message = StrUtil.buildOriginalCauseMessage(bussinessMessage, null);
            LOG.error(message);

            throw new DataXException(OdpsWriterErrorCode.ADD_PARTITION_FAILED, bussinessMessage);
        }
    }

    private static String getPartSpec(String partition) {
        StringBuilder partSpec = new StringBuilder();
        String[] parts = partition.split(",");
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            String[] kv = part.split("=");
            if (kv.length != 2) {
                throw new DataXException(null, "Wrong partition Spec: " + partition);
            }
            partSpec.append(kv[0]).append("=");
            partSpec.append("'").append(kv[1].replace("'", "")).append("'");
            if (i != parts.length - 1)
                partSpec.append(",");
        }
        return partSpec.toString();
    }

    private static String getTableDdl(Table table) {
        String jsonStr = table.getSchema().toJson();
        StringBuilder sqlBuilder = new StringBuilder(1024);
        try {
            JSONObject js = new JSONObject(jsonStr);
            JSONArray jaColumns = js.getJSONArray("columns");

            sqlBuilder.append("create table IF NOT EXISTS ")
                    .append(table.getName()).append("(");
            for (int i = 0; i < jaColumns.length(); i++) {
                JSONObject jsColumn = jaColumns.getJSONObject(i);

                String name = jsColumn.getString("name");
                String type = jsColumn.getString("type");

                sqlBuilder.append("`" + name + "`").append(" ").append(type);

                if (jsColumn.has("comment")) {
                    String comment = jsColumn.getString("comment");

                    // 把双引号转意
                    if (comment.indexOf("\"") > 0) {
                        comment = comment.replaceAll("\"", "\\\\\"");
                    }
                    sqlBuilder.append(" comment \"" + comment + "\"");
                }

                if (i < jaColumns.length() - 1) {
                    sqlBuilder.append(", ");
                }
            }
            sqlBuilder.append(")");

            JSONArray jaPartitionKeys = js.getJSONArray("partitionKeys");
            if (jaPartitionKeys.length() > 0) {
                sqlBuilder.append(" partitioned by(");
            }

            for (int i = 0; i < jaPartitionKeys.length(); i++) {
                JSONObject jsPartionKey = jaPartitionKeys.getJSONObject(i);

                String name = jsPartionKey.getString("name");
                String type = jsPartionKey.getString("type");

                sqlBuilder.append(name).append(" ").append(type);

                if (jsPartionKey.has("comment")) {
                    String comment = jsPartionKey.getString("comment");
                    if (comment.indexOf("\"") > 0) {
                        comment = comment.replaceAll("\"", "\\\\\"");
                    }

                    sqlBuilder.append(" comment \"" + comment + "\"");

                }

                if (i < jaPartitionKeys.length() - 1) {
                    sqlBuilder.append(", ");
                }
            }
            if (jaPartitionKeys.length() > 0) {
                sqlBuilder.append(")");
            }

        } catch (JSONException e) {
            throw new DataXException(OdpsWriterErrorCode.TEMP, e);
        }
        sqlBuilder.append(";\r\n");
        return sqlBuilder.toString();
    }

    private static String getAddPartitionDdl(Table table) {

        List<String> partionSpecList = null;
        try {
            partionSpecList = table.listPartitions();
        } catch (Exception e) {
            throw new DataXException(OdpsWriterErrorCode.TEMP, e);
        }

        StringBuilder addPartitionBuilder = new StringBuilder();
        for (String partionSpec : partionSpecList) {

            if (partionSpec.indexOf("__HIVE_DEFAULT_PARTITION__") >= 0
                    || partionSpec.indexOf("''20120529''") >= 0) {
                addPartitionBuilder.append("--alter table ")
                        .append(table.getName())
                        .append(" add IF NOT EXISTS partition(")
                        .append(partionSpec.replace(":", "=")).append(");\r\n");
            } else {
                addPartitionBuilder.append("alter table ")
                        .append(table.getName())
                        .append(" add IF NOT EXISTS partition(")
                        .append(partionSpec.replace(":", "=")).append(");\r\n");
            }
        }

        return addPartitionBuilder.toString();
    }

    private static void runSqlTask(Project project, String query) throws Exception {
        if (StringUtils.isBlank(query)) {
            return;
        }

        String taskName = "datax_odpswriter_trunacte_" + UUID.randomUUID();

        LOG.info("Try to start sqlTtask:[{}] to run odps sql:[\n{}\n] .", taskName, query);
        Task task = new SqlTask(taskName, query);
        JobInstance instance = Job.run(project, task);
        instance.waitForCompletion();
        TaskStatus status = instance.getTaskStatus().get(taskName);

        if (!Status.SUCCESS.equals(status.getStatus())) {
            String message = String.format("Run odps sql task not success. detail:result=[%s].",
                    instance.getResult().get(taskName));
            throw new Exception(message);
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
            throw new DataXException(OdpsWriterErrorCode.COMMIT_BLOCK_FAIL, e);
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
            throw new DataXException(OdpsWriterErrorCode.WRITER_BLOCK_FAIL, e);
        }

    }

    /**
     * 与 OdpsReader 中代码一样
     */
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
                throw new DataXException(OdpsWriterErrorCode.TEMP,
                        String.format("No column named [%s] !", col));
            }
        }
        return retList;
    }

    /**
     * 转为小写(对类型进行了是否支持的判断)
     */
    public static List<String> getAllColumns(RecordSchema schema) {
        if (null == schema) {
            throw new IllegalArgumentException("parameter schema can not be null.");
        }

        List<String> allColumns = new ArrayList<String>();
        Column.Type type = null;
        for (int i = 0, columnCount = schema.getColumnCount(); i < columnCount; i++) {
            allColumns.add(schema.getColumnName(i).toLowerCase());
            type = schema.getColumnType(i);
            if (type == Column.Type.ODPS_ARRAY || type == Column.Type.ODPS_MAP) {
                throw new DataXException(OdpsWriterErrorCode.TEMP, "Unsupported data type:" + type);
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
}
