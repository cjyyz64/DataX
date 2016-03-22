package com.alibaba.datax.plugin.writer.adswriter.insert;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.writer.adswriter.util.Constant;
import com.alibaba.datax.plugin.writer.adswriter.util.Key;
import com.mysql.jdbc.JDBC4PreparedStatement;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;


public class AdsInsertProxy {

    private static final Logger LOG = LoggerFactory
            .getLogger(AdsInsertProxy.class);

    private String table;
    private List<String> columns;
    private TaskPluginCollector taskPluginCollector;
    private Configuration configuration;
    private Boolean emptyAsNull;
    private String sqlPrefix;

    private Triple<List<String>, List<Integer>, List<String>> resultSetMetaData;

    public AdsInsertProxy(String table, List<String> columns, Configuration configuration, TaskPluginCollector taskPluginCollector) {
        this.table = table;
        this.columns = columns;
        this.configuration = configuration;
        this.taskPluginCollector = taskPluginCollector;
        this.emptyAsNull = configuration.getBool(Key.EMPTY_AS_NULL, false);
        this.sqlPrefix = "insert into " + this.table + "("
                + StringUtils.join(columns, ",") + ") values";
    }

    public void startWriteWithConnection(RecordReceiver recordReceiver,
                                                Connection connection,
                                                int columnNumber) {
        //目前 ads 新建的表 如果未插入数据  不能通过select colums from table where 1=2，获取列信息。
//        this.resultSetMetaData = DBUtil.getColumnMetaData(connection,
//                this.table, StringUtils.join(this.columns, ","));

        this.resultSetMetaData = AdsInsertUtil.getColumnMetaData(configuration, columns);

        int batchSize = this.configuration.getInt(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_SIZE);
        List<Record> writeBuffer = new ArrayList<Record>(batchSize);
        try {
            Record record;
            while ((record = recordReceiver.getFromReader()) != null) {
                if (record.getColumnNumber() != columnNumber) {
                    // 源头读取字段列数与目的表字段写入列数不相等，直接报错
                    throw DataXException
                            .asDataXException(
                                    DBUtilErrorCode.CONF_ERROR,
                                    String.format(
                                            "列配置信息有错误. 因为您配置的任务中，源头读取字段数:%s 与 目的表要写入的字段数:%s 不相等. 请检查您的配置并作出修改.",
                                            record.getColumnNumber(),
                                            columnNumber));
                }

                writeBuffer.add(record);

                if (writeBuffer.size() >= batchSize) {
                    //doOneInsert(connection, writeBuffer);
                    doBatchInsertWithOneStatement(connection, writeBuffer);
                    writeBuffer.clear();
                }
            }
            if (!writeBuffer.isEmpty()) {
                doOneInsert(connection, writeBuffer);
                writeBuffer.clear();
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    DBUtilErrorCode.WRITE_DATA_ERROR, e);
        } finally {
            writeBuffer.clear();
            DBUtil.closeDBResources(null, null, connection);
        }
    }

    //warn: ADS 无法支持事物，这里面的roll back都是不管用的吧
    protected void doBatchInsert(Connection connection, List<Record> buffer) throws SQLException {
        Statement statement = null;
        try {
            connection.setAutoCommit(false);
            statement = connection.createStatement();

            for (Record record : buffer) {
                String sql = generateInsertSql(connection, record);
                statement.addBatch(sql);
            }
            statement.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            LOG.warn("回滚此次写入, 采用每次写入一行方式提交. 因为:" + e.getMessage());
            connection.rollback();
            doOneInsert(connection, buffer);
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    DBUtilErrorCode.WRITE_DATA_ERROR, e);
        } finally {
            DBUtil.closeDBResources(statement, null);
        }
    }
    
    private void doBatchInsertWithOneStatement(Connection connection,
            List<Record> buffer) throws SQLException {
        Statement statement = null;
        String sql = null;
        try {
            connection.setAutoCommit(true);
            statement = connection.createStatement();
            int bufferSize = buffer.size();
            if (buffer.isEmpty()) {
                return;
            }
            StringBuilder sqlSb = new StringBuilder();
            sqlSb.append(this.generateInsertSql(connection, buffer.get(0)));
            for (int i = 1; i < bufferSize; i++) {
                Record record = buffer.get(i);
                this.appendInsertSqlValues(connection, record, sqlSb);
            }
            try {
                sql = sqlSb.toString();
                LOG.debug(sql);
                int status = statement.executeUpdate(sql);
                sql = null;
            } catch (SQLException e) {
                LOG.error("sql: " + sql, e.getMessage());
                // warn: 无法明确得知具体那一条是脏数据了
                doOneInsert(connection, buffer);
                // for (Record eachRecord : buffer) {
                // this.taskPluginCollector.collectDirtyRecord(eachRecord, e);
                // }
            }
        } catch (Exception e) {
            LOG.error("插入异常, sql: " + sql);
            throw DataXException.asDataXException(
                    DBUtilErrorCode.WRITE_DATA_ERROR, e);
        } finally {
            DBUtil.closeDBResources(statement, null);
        }
    }

    protected void doOneInsert(Connection connection, List<Record> buffer) {
        Statement statement = null;
        String sql = null;
        try {
            connection.setAutoCommit(true);
            statement = connection.createStatement();
            
            for (Record record : buffer) {
                try {
                    sql = generateInsertSql(connection, record);
                    LOG.debug(sql);
                    int status = statement.executeUpdate(sql);
                    sql = null;
                } catch (SQLException e) {
                    LOG.error("sql: " + sql, e.getMessage());
                    this.taskPluginCollector.collectDirtyRecord(record, e);
                }
            }
        } catch (Exception e) {
            LOG.error("插入异常, sql: " + sql);
            throw DataXException.asDataXException(
                    DBUtilErrorCode.WRITE_DATA_ERROR, e);
        } finally {
            DBUtil.closeDBResources(statement, null);
        }
    }

    private String generateInsertSql(Connection connection, Record record) throws SQLException {
        StringBuilder sqlSb = new StringBuilder(this.sqlPrefix + "(");
        for (int i = 0; i < columns.size(); i++) {
            if((i+1) != columns.size()) {
                sqlSb.append("?,");
            } else {
                sqlSb.append("?");
            }
        }
        sqlSb.append(")");
        PreparedStatement statement = connection.prepareStatement(sqlSb.toString());
        for (int i = 0; i < columns.size(); i++) {
            int columnSqltype = this.resultSetMetaData.getMiddle().get(i);
            checkColumnType(statement, columnSqltype, record.getColumn(i), i);
        }
        String sql = ((JDBC4PreparedStatement) statement).asSql();
        DBUtil.closeDBResources(statement, null);
        return sql;
    }
    
    private void appendInsertSqlValues(Connection connection, Record record, StringBuilder sqlSb) throws SQLException { 
        String sqlResult = this.generateInsertSql(connection, record);
        sqlSb.append(",");
        sqlSb.append(sqlResult.substring(this.sqlPrefix.length()));
    }

    private void checkColumnType(PreparedStatement statement, int columnSqltype, Column column, int columnIndex) throws SQLException {
        java.util.Date utilDate;
        switch (columnSqltype) {
            case Types.CHAR:
            case Types.NCHAR:
            case Types.CLOB:
            case Types.NCLOB:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
                String strValue = column.asString();
                statement.setString(columnIndex + 1, strValue);
                break;

            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.NUMERIC:
            case Types.DECIMAL:
            case Types.REAL:
                String numValue = column.asString();
                if(emptyAsNull && "".equals(numValue) || numValue == null){
                    statement.setLong(columnIndex + 1, (Long) null);
                } else{
                    statement.setLong(columnIndex + 1, column.asLong());
                }
                break;
                
            case Types.FLOAT:
            case Types.DOUBLE:
                String floatValue = column.asString();
                if(emptyAsNull && "".equals(floatValue) || floatValue == null){
                    statement.setDouble(columnIndex + 1, (Double) null);
                } else{
                    statement.setDouble(columnIndex + 1, column.asDouble());
                }
                break;

            //tinyint is a little special in some database like mysql {boolean->tinyint(1)}
            case Types.TINYINT:
                Long longValue = column.asLong();
                statement.setLong(columnIndex + 1, longValue);
                break;

            case Types.DATE:
                java.sql.Date sqlDate = null;
                try {
                    if("".equals(column.getRawData())) {
                        utilDate = null;
                    } else {
                        utilDate = column.asDate();
                    }
                } catch (DataXException e) {
                    throw new SQLException(String.format(
                            "Date 类型转换错误：[%s]", column));
                }
                
                if (null != utilDate) {
                    sqlDate = new java.sql.Date(utilDate.getTime());
                } 
                statement.setDate(columnIndex + 1, sqlDate);
                break;

            case Types.TIME:
                java.sql.Time sqlTime = null;
                try {
                    if("".equals(column.getRawData())) {
                        utilDate = null;
                    } else {
                        utilDate = column.asDate();
                    }
                } catch (DataXException e) {
                    throw new SQLException(String.format(
                            "TIME 类型转换错误：[%s]", column));
                }

                if (null != utilDate) {
                    sqlTime = new java.sql.Time(utilDate.getTime());
                }
                statement.setTime(columnIndex + 1, sqlTime);
                break;

            case Types.TIMESTAMP:
                java.sql.Timestamp sqlTimestamp = null;
                try {
                    if("".equals(column.getRawData())) {
                        utilDate = null;
                    } else {
                        utilDate = column.asDate();
                    }
                } catch (DataXException e) {
                    throw new SQLException(String.format(
                            "TIMESTAMP 类型转换错误：[%s]", column));
                }

                if (null != utilDate) {
                    sqlTimestamp = new java.sql.Timestamp(
                            utilDate.getTime());
                }
                statement.setTimestamp(columnIndex + 1, sqlTimestamp);
                break;

            case Types.BOOLEAN:
            case Types.BIT:
                statement.setBoolean(columnIndex + 1, column.asBoolean());
                break;
            default:
                throw DataXException
                        .asDataXException(
                                DBUtilErrorCode.UNSUPPORTED_TYPE,
                                String.format(
                                        "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库写入这种字段类型. 字段名:[%s], 字段类型:[%d], 字段Java类型:[%s]. 请修改表中该字段的类型或者不同步该字段.",
                                        this.resultSetMetaData.getLeft()
                                                .get(columnIndex),
                                        this.resultSetMetaData.getMiddle()
                                                .get(columnIndex),
                                        this.resultSetMetaData.getRight()
                                                .get(columnIndex)));
        }
    }
}
