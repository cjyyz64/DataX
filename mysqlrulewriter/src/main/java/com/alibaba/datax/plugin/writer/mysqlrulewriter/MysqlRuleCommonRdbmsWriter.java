package com.alibaba.datax.plugin.writer.mysqlrulewriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.mysqlrulewriter.buffer.RuleWriterDbBuffer;
import com.alibaba.datax.plugin.writer.mysqlrulewriter.groovy.GroovyRuleExecutor;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Date: 15/3/19 下午4:05
 *
 * @author liupeng <a href="mailto:liupengjava@gmail.com">Ricoul</a>
 */
public class MysqlRuleCommonRdbmsWriter extends CommonRdbmsWriter {

    public static class Task extends CommonRdbmsWriter.Task {

        private Map<String, RuleWriterDbBuffer> bufferMap = new HashMap<String, RuleWriterDbBuffer>();

        private GroovyRuleExecutor dbRuleExecutor;

        private GroovyRuleExecutor tableRuleExecutor;

        private String dbNamePattern;

        private String dbRule;

        private String tableNamePattern;

        private String tableRule;

        private MutablePair<String, String> metaDbTablePair = new MutablePair<String, String>();;

        private Map<String, String> tableWriteSqlMap = new HashMap<String, String>();

        public Task(DataBaseType dataBaseType) {
            super(dataBaseType);
        }

        @Override
        public void init(Configuration writerSliceConfig) {
            this.username = writerSliceConfig.getString(Key.USERNAME);
            this.password = writerSliceConfig.getString(Key.PASSWORD);
            //获取规则
            this.dbNamePattern = writerSliceConfig.getString(Key.DB_NAME_PATTERN);
            this.dbRule = writerSliceConfig.getString(Key.DB_RULE);
            this.dbRule = (dbRule == null) ? "" : dbRule;
            this.tableNamePattern = writerSliceConfig.getString(Key.TABLE_NAME_PATTERN);
            this.tableRule = writerSliceConfig.getString(Key.TABLE_RULE);
            this.tableRule = (tableRule == null) ? "" : tableRule;

            dbRuleExecutor = new GroovyRuleExecutor(dbRule, dbNamePattern);
            tableRuleExecutor = new GroovyRuleExecutor(tableRule, tableNamePattern);

            this.columns = writerSliceConfig.getList(Key.COLUMN, String.class);
            this.columnNumber = this.columns.size();
            this.batchSize = writerSliceConfig.getInt(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_SIZE);
            this.batchByteSize = writerSliceConfig.getInt(Key.BATCH_BYTE_SIZE, Constant.DEFAULT_BATCH_BYTE_SIZE);
            writeMode = writerSliceConfig.getString(Key.WRITE_MODE, "INSERT");
            emptyAsNull = writerSliceConfig.getBool(Key.EMPTY_AS_NULL, true);
            INSERT_OR_REPLACE_TEMPLATE = writerSliceConfig.getString(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK);

            //init writerbuffer map,每一个db对应一个buffer
            List<Object> conns = writerSliceConfig.getList(Constant.CONN_MARK, Object.class);

            for(int i = 0; i < conns.size(); i++) {
                Configuration connConf = Configuration.from(conns.get(i).toString());
                String jdbcUrl = connConf.getString(Key.JDBC_URL);
                List<String> tableList = connConf.getList(Key.TABLE, String.class);
                RuleWriterDbBuffer writerBuffer = new RuleWriterDbBuffer();
                writerBuffer.setJdbcUrl(jdbcUrl);
                writerBuffer.initTableBuffer(tableList);
                String dbName = getDbNameFromJdbcUrl(jdbcUrl);
                bufferMap.put(dbName, writerBuffer);
                //确定获取meta元信息的db和table
                if(i == 0 && tableList.size() > 0) {
                    metaDbTablePair.setLeft(dbName);
                    metaDbTablePair.setRight(tableList.get(0));
                }
                //init每一个table的insert语句
                for(String tableName : tableList) {
                    tableWriteSqlMap.put(tableName, String.format(INSERT_OR_REPLACE_TEMPLATE, tableName));
                }
            }
        }

        public String getDbNameFromJdbcUrl(String jdbcUrl) {
            if(jdbcUrl.contains("?")) {
                return jdbcUrl.substring(jdbcUrl.lastIndexOf("/") + 1, jdbcUrl.indexOf("?"));
            } else {
                return jdbcUrl.substring(jdbcUrl.lastIndexOf("/") + 1);
            }
        }

        public Map<String, Object> convertRecord2Map(Record record) {
            Map<String, Object> map = new HashMap<String, Object>();
            for (int i = 0; i < this.columnNumber; i++) {
                String columnName = this.resultSetMetaData.getLeft().get(i);
                map.put(columnName, record.getColumn(i).getRawData());
            }
            return map;
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver, Configuration writerSliceConfig, TaskPluginCollector taskPluginCollector) {

            for(Map.Entry<String, RuleWriterDbBuffer> entry : bufferMap.entrySet()) {
                entry.getValue().initConnection(writerSliceConfig, username, password);
            }
            this.taskPluginCollector = taskPluginCollector;

            // 用于写入数据的时候的类型根据目的表字段类型转换
            Connection metaConn = bufferMap.get(metaDbTablePair.getLeft()).getConnection();
            String metaTable = metaDbTablePair.getRight();
            this.resultSetMetaData = DBUtil.getColumnMetaData(metaConn, metaTable, StringUtils.join(this.columns, ","));

            List<Record> writeBuffer = new ArrayList<Record>(this.batchSize);
            int bufferBytes = 0;
            try {
                Record record;
                while ((record = recordReceiver.getFromReader()) != null) {
                    if (record.getColumnNumber() != this.columnNumber) {
                        // 源头读取字段列数与目的表字段写入列数不相等，直接报错
                        throw DataXException
                                .asDataXException(
                                        DBUtilErrorCode.CONF_ERROR,
                                        String.format(
                                                "列配置信息有错误. 因为您配置的任务中，源头读取字段数:%s 与 目的表要写入的字段数:%s 不相等. 请检查您的配置并作出修改.",
                                                record.getColumnNumber(),
                                                this.columnNumber));
                    }

                    writeBuffer.add(record);
                    bufferBytes += record.getByteSize();

                    if (writeBuffer.size() >= batchSize || bufferBytes >= batchByteSize) {
                        calcRuleAndDoBatchInsert(writeBuffer);
                        writeBuffer.clear();
                        bufferBytes = 0;
                    }
                }
                if (!writeBuffer.isEmpty()) {
                    calcRuleAndDoBatchInsert(writeBuffer);
                    writeBuffer.clear();
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                writeBuffer.clear();
                closeBufferConn();
            }
        }

        public void closeBufferConn() {
            for(Map.Entry<String, RuleWriterDbBuffer> entry : bufferMap.entrySet()) {
                DBUtil.closeDBResources(null, null, entry.getValue().getConnection());
            }
        }

        public void calcRuleAndDoBatchInsert(List<Record> recordBuffer) throws SQLException {
            //calcRule add all record
            for(Record record : recordBuffer) {
                Map<String, Object> recordMap = convertRecord2Map(record);
                String dbName = dbRuleExecutor.executeRule(recordMap);
                String tableName = tableRuleExecutor.executeRule(recordMap);
                RuleWriterDbBuffer ruleWriterDbBuffer = bufferMap.get(dbName);
                if(ruleWriterDbBuffer == null) {
                    throw DataXException.asDataXException(
                            DBUtilErrorCode.WRITE_DATA_ERROR, "通过规则计算出来的db不存在，算出的dbName=" + dbName + ", 请检查您配置的规则.");
                }

                ruleWriterDbBuffer.addRecord(record, tableName);
            }

            //do batchInsert
            for(Map.Entry<String, RuleWriterDbBuffer> entry : bufferMap.entrySet()) {
                RuleWriterDbBuffer dbBuffer = entry.getValue();
                Connection connection = dbBuffer.getConnection();
                PreparedStatement preparedStatement = null;
                try {
                    connection.setAutoCommit(false);
                    for (Map.Entry<String, List<Record>> tableBufferEntry : dbBuffer.getTableBuffer().entrySet()) {
                        String tableName = tableBufferEntry.getKey();
                        List<Record> recordList = tableBufferEntry.getValue();
                        String writeRecordSql = tableWriteSqlMap.get(tableName);
                        preparedStatement = connection.prepareStatement(writeRecordSql);
                        for (Record record : recordList) {
                            preparedStatement = fillPreparedStatement(preparedStatement, record);
                            preparedStatement.addBatch();
                        }
                        preparedStatement.executeBatch();
                    }
                    connection.commit();

                    //在commit之后清空数据
                    for (Map.Entry<String, List<Record>> tableBufferEntry : dbBuffer.getTableBuffer().entrySet()) {
                        List<Record> recordList = tableBufferEntry.getValue();
                        recordList.clear();
                    }
                } catch (SQLException e) {
                    LOG.warn("回滚此次写入, 采用每次写入一行方式提交. 因为:" + e.getMessage());
                    connection.rollback();
                    doRuleOneInsert(connection, dbBuffer);
                } catch (Exception e) {
                    throw DataXException.asDataXException(
                            DBUtilErrorCode.WRITE_DATA_ERROR, e);
                } finally {
                    DBUtil.closeDBResources(preparedStatement, null);
                }
            }
        }

        protected void doRuleOneInsert(Connection connection, RuleWriterDbBuffer dbBuffer) {
            PreparedStatement preparedStatement = null;
            try {
                connection.setAutoCommit(true);
                for (Map.Entry<String, List<Record>> entry : dbBuffer.getTableBuffer().entrySet()) {
                    String tableName = entry.getKey();
                    preparedStatement = connection.prepareStatement(this.tableWriteSqlMap.get(tableName));
                    List<Record> recordList = entry.getValue();
                    for (Record record : recordList) {
                        try {
                            preparedStatement = fillPreparedStatement(preparedStatement, record);
                            preparedStatement.execute();
                        } catch (SQLException e) {
                            LOG.error("写入表[" + tableName + "]存在脏数据, 写入异常为:" + e.toString());
                            this.taskPluginCollector.collectDirtyRecord(record, e);
                        } finally {
                            // 最后不要忘了关闭 preparedStatement
                            preparedStatement.clearParameters();
                        }
                    }
                    //在commit之后完成清理
                    recordList.clear();
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                DBUtil.closeDBResources(preparedStatement, null);
            }
        }
    }

}
