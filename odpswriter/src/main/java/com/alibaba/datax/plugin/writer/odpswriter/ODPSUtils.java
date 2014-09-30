package com.alibaba.datax.plugin.writer.odpswriter;

import com.alibaba.datax.common.exception.DataXException;
import com.aliyun.openservices.odps.Project;
import com.aliyun.openservices.odps.jobs.*;
import com.aliyun.openservices.odps.jobs.TaskStatus.Status;
import com.aliyun.openservices.odps.tables.Table;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class ODPSUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ODPSUtils.class);

    public static String getTableDdl(Table table) {
        String jsonStr = table.getSchema().toJson();
        StringBuilder sqlBuilder = new StringBuilder();
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

    public static String getAddPartitionDdl(Table table) {

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

    public static void runSqlTask(Project project, String query) {
        if (null == query || "".endsWith(query))
            return;
        LOG.info("ODPSWriter try to execute :[{}] .", query);
        Task task = new SqlTask("datax_odpstunnel_writer_trunacte", query);
        JobInstance instance = null;
        try {
            instance = Job.run(project, task);
            instance.waitForCompletion();
            TaskStatus status = instance.getTaskStatus().get(
                    "datax_odpstunnel_writer_trunacte");
            LOG.info(String.format("ODPSWriter execute query result :%s .",
                    status.getStatus()));
            if (status.getStatus().equals(Status.FAILED)) {
                Map<String, String> result = null;
                result = instance.getResult();
                throw new DataXException(OdpsWriterErrorCode.TEMP, "Error when Execute query. "
                        + result.get(task.getName()));
            }
        } catch (Exception e) {
            LOG.info("Failed to run the query due to an error from ODPS."
                    + "Reason: " + e.getMessage());
            throw new DataXException(OdpsWriterErrorCode.TEMP, "Error when truncate table." + e);
        }
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
}
