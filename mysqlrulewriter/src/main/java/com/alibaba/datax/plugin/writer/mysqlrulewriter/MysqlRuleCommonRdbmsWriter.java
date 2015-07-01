package com.alibaba.datax.plugin.writer.mysqlrulewriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.datax.plugin.writer.mysqlrulewriter.buffer.RuleWriterDbBuffer;
import com.alibaba.datax.plugin.writer.mysqlrulewriter.groovy.GroovyRuleExecutor;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * Date: 15/3/19 下午4:05
 *
 * @author liupeng <a href="mailto:liupengjava@gmail.com">Ricoul</a>
 */
public class MysqlRuleCommonRdbmsWriter extends CommonRdbmsWriter {

    public static class Task extends CommonRdbmsWriter.Task {

        private List<RuleWriterDbBuffer> dbBufferList = new ArrayList<RuleWriterDbBuffer>();

        private GroovyRuleExecutor dbRuleExecutor = null;

        private GroovyRuleExecutor tableRuleExecutor = null;

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

            if(StringUtils.isNotBlank(this.dbRule)) {
                dbRuleExecutor = new GroovyRuleExecutor(dbRule, dbNamePattern);
            }
            if(StringUtils.isNoneBlank(this.tableRule)) {
                tableRuleExecutor = new GroovyRuleExecutor(tableRule, tableNamePattern);
            }

            this.columns = writerSliceConfig.getList(Key.COLUMN, String.class);
            this.columnNumber = this.columns.size();
            this.batchSize = writerSliceConfig.getInt(Key.BATCH_SIZE, 2048);
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
                writerBuffer.setDbName(dbName);
                dbBufferList.add(writerBuffer);
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

            //检查规则配置
            checkRule(dbBufferList);
        }

        public void checkRule(List<RuleWriterDbBuffer> dbBufferList) {
            //如果配置的分表名完全不同， 则必须要填写tableRule规则
            List<String> allTableList = new ArrayList<String>();
            List<String> allDbList = new ArrayList<String>();
            for(RuleWriterDbBuffer ruleWriterDbBuffer : dbBufferList) {
                allTableList.addAll(ruleWriterDbBuffer.getTableBuffer().keySet());
                allDbList.add(ruleWriterDbBuffer.getDbName());
            }
            Set<String> allTableSet = new HashSet<String>(allTableList);
            Set<String> allDbSet = new HashSet<String>(allDbList);

            //如果是多表，必须要配置table规则
            if(allTableList.size() == allTableSet.size() && allTableSet.size() > 1) {
                if(tableRuleExecutor == null) {
                    throw DataXException.asDataXException(
                            DBUtilErrorCode.WRITE_DATA_ERROR, "配置的tableList为多表，但未配置分表规则，请检查您的配置");
                }
                return;
            }
            //如果分表在每个db下，有部分重复的情况，则dbRule和tableRule都要填写
            if(allTableList.size() != allTableSet.size() && allTableSet.size() > 1 && allDbSet.size() > 1) {
                if(tableRuleExecutor == null || dbRuleExecutor == null) {
                    throw DataXException.asDataXException(
                            DBUtilErrorCode.WRITE_DATA_ERROR, "配置的多库中的表名有重复的，但未配置分库规则和分表规则，请检查您的配置");
                }
                return;
            }
            //如果分表名都相同，分库名不同，那么可以只填写分库规则
            if(allTableList.size() != allTableSet.size() && allTableSet.size() == 1) {
                if(dbRuleExecutor == null) {
                    throw DataXException.asDataXException(
                            DBUtilErrorCode.WRITE_DATA_ERROR, "配置的所有表名都相同，但未配置分库规则，请检查您的配置");
                }
                return;
            }
            //如果分表名和分库名都相同，只是jdbcurl不同，这种不支持
            if(allDbSet.size() == 1 && allTableSet.size() == 1 && allTableList.size() > 1 && allDbList.size() > 1) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.WRITE_DATA_ERROR, "配置的table和db名称都相同，此种回流方式不支持");
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
            for(RuleWriterDbBuffer dbBuffer : dbBufferList) {
                dbBuffer.initConnection(writerSliceConfig, username, password);
            }

            this.taskPluginCollector = taskPluginCollector;

            // 用于写入数据的时候的类型根据目的表字段类型转换
            String metaTable = metaDbTablePair.getRight();
            this.resultSetMetaData = DBUtil.getColumnMetaData(dbBufferList.get(0).getConnection(), metaTable, StringUtils.join(this.columns, ","));

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
            for(RuleWriterDbBuffer ruleWriterDbBuffer : dbBufferList) {
                DBUtil.closeDBResources(null, null, ruleWriterDbBuffer.getConnection());
            }
        }

        private void calcRuleAndAddBuffer(Record record) {
            Map<String, Object> recordMap = convertRecord2Map(record);
            RuleWriterDbBuffer rightBuffer = null;

            //如果dbRule为空,则通过tableName反查出来dbName
            if(dbRuleExecutor == null && tableRuleExecutor != null) {
                String tableName = tableRuleExecutor.executeRule(recordMap);
                for(RuleWriterDbBuffer ruleWriterDbBuffer : dbBufferList) {
                     if(ruleWriterDbBuffer.getTableBuffer().keySet().contains(tableName)) {
                         rightBuffer = ruleWriterDbBuffer;
                         break;
                     }
                }
                if(rightBuffer == null) {
                    throw DataXException.asDataXException(
                            DBUtilErrorCode.WRITE_DATA_ERROR, "通过规则计算出来的tableName查找对应的db不存在，tableName=" + tableName + ", 请检查您配置的规则.");
                }
                rightBuffer.addRecord(record, tableName);
            } else if(dbRuleExecutor != null && tableRuleExecutor != null) {//两个规则都不为空，需要严格匹配计算出来的dbName和tableName
                String dbName = dbRuleExecutor.executeRule(recordMap);
                String tableName = tableRuleExecutor.executeRule(recordMap);
                for(RuleWriterDbBuffer ruleWriterDbBuffer : dbBufferList) {
                    if(StringUtils.equals(ruleWriterDbBuffer.getDbName(), dbName) && ruleWriterDbBuffer.getTableBuffer().keySet().contains(tableName)) {
                        rightBuffer = ruleWriterDbBuffer;
                        break;
                    }
                }
                if(rightBuffer == null) {
                    throw DataXException.asDataXException(
                            DBUtilErrorCode.WRITE_DATA_ERROR, "通过规则计算出来的db和table不存在，算出的dbName=" + dbName + ",tableName="+ tableName +", 请检查您配置的规则.");
                }
                rightBuffer.addRecord(record, tableName);
            } else if(dbRuleExecutor != null && tableRuleExecutor == null) {// 只存在dbRule，那么只能是多个分库的所有的table名称都相同
                String dbName = dbRuleExecutor.executeRule(recordMap);
                for(RuleWriterDbBuffer ruleWriterDbBuffer : dbBufferList) {
                    if(StringUtils.equals(ruleWriterDbBuffer.getDbName(), dbName)) {
                        rightBuffer = ruleWriterDbBuffer;
                        break;
                    }
                }
                if(rightBuffer == null) {
                    throw DataXException.asDataXException(
                            DBUtilErrorCode.WRITE_DATA_ERROR, "通过规则计算出来的db不存在，算出的dbName=" + dbName +", 请检查您配置的规则.");
                }

                if(rightBuffer.getTableBuffer().keySet().size() != 1) {
                    throw DataXException.asDataXException(
                            DBUtilErrorCode.WRITE_DATA_ERROR, "通过规则计算出来的dbName[" + dbName +"], 存在多张分表，请配置您的分表规则.");
                }
                String tableName = (String)rightBuffer.getTableBuffer().keySet().toArray()[0];
                rightBuffer.addRecord(record, tableName);
            }
        }

        public void calcRuleAndDoBatchInsert(List<Record> recordBuffer) throws SQLException {
            //calcRule add all record
            for(Record record : recordBuffer) {
                calcRuleAndAddBuffer(record);
            }

            //do batchInsert
            for(RuleWriterDbBuffer dbBuffer : dbBufferList) {
                Connection connection = dbBuffer.getConnection();
                PreparedStatement preparedStatement = null;
                try {
                    connection.setAutoCommit(false);
                    for (Map.Entry<String, List<Record>> tableBufferEntry : dbBuffer.getTableBuffer().entrySet()) {
                        String tableName = tableBufferEntry.getKey();
                        List<Record> recordList = tableBufferEntry.getValue();
                        String writeRecordSql = tableWriteSqlMap.get(tableName);
                        try {
                            preparedStatement = connection.prepareStatement(writeRecordSql);
                            for (Record record : recordList) {
                                preparedStatement = fillPreparedStatement(preparedStatement, record);
                                preparedStatement.addBatch();
                            }
                            preparedStatement.executeBatch();
                        } finally {
                            DBUtil.closeDBResources(preparedStatement, null);
                        }
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
