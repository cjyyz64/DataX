package com.alibaba.datax.plugin.writer.mysqlwriter;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.writer.mysqlwriter.util.MysqlWriterUtil;
import com.alibaba.datax.plugin.writer.mysqlwriter.util.OriginalConfPretreatmentUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MysqlWriter extends Writer {

    public static class Master extends Writer.Master {
        private static final Logger LOG = LoggerFactory
                .getLogger(MysqlWriter.Master.class);

        private static final boolean IS_DEBUG = LOG.isDebugEnabled();

        private Configuration originalConfig = null;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            OriginalConfPretreatmentUtil.doPretreatment(this.originalConfig);
            if (IS_DEBUG) {
                LOG.debug("After master init(), originalConfig now is:[\n{}\n]",
                        this.originalConfig.toJSON());
            }
        }

        // 一般来说，是需要推迟到 slave 中进行pre 的执行（单表情况例外）
        @Override
        public void prepare() {
            int tableNumber = this.originalConfig.getInt(Constant.TABLE_NUMBER_MARK).intValue();
            if (tableNumber == 1) {
                String username = this.originalConfig.getString(Key.USERNAME);
                String password = this.originalConfig.getString(Key.PASSWORD);

                List<Object> conns = this.originalConfig.getList(Constant.CONN_MARK,
                        Object.class);
                Configuration connConf = Configuration.from(conns.get(0).toString());
                String jdbcUrl = connConf.getString(Key.JDBC_URL);
                jdbcUrl = MysqlWriterUtil.appendJDBCSuffix(jdbcUrl);
                this.originalConfig.set(Key.JDBC_URL, jdbcUrl);

                String table = connConf.getList(Key.TABLE, String.class).get(0);
                this.originalConfig.set(Key.TABLE, table);

                List<String> preSqls = this.originalConfig.getList(Key.PRE_SQL, String.class);
                List<String> renderedPreSqls = MysqlWriterUtil.renderPreOrPostSqls(preSqls, table);

                this.originalConfig.remove(Constant.CONN_MARK);
                if (null != renderedPreSqls && !renderedPreSqls.isEmpty()) {
                    //说明有 preSql 配置，则此处删除掉
                    this.originalConfig.remove(Key.PRE_SQL);

                    Connection conn = DBUtil.getConnection(DataBaseType.MySql, jdbcUrl, username, password);
                    LOG.info("Begin to execute preSqls:[{}]. context info:{}.", StringUtils.join(renderedPreSqls, ";"),
                            jdbcUrl);

                    MysqlWriterUtil.executeSqls(conn, renderedPreSqls, jdbcUrl);
                    DBUtil.closeDBResources(null, null, conn);
                }
            }

            if (IS_DEBUG) {
                LOG.debug("After master prepare(), originalConfig now is:[\n{}\n]",
                        this.originalConfig.toJSON());
            }
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            return MysqlWriterUtil.doSplit(this.originalConfig, adviceNumber);
        }

        // 一般来说，是需要推迟到 slave 中进行post 的执行（单表情况例外）
        @Override
        public void post() {
            int tableNumber = this.originalConfig.getInt(Constant.TABLE_NUMBER_MARK).intValue();
            if (tableNumber == 1) {
                String username = this.originalConfig.getString(Key.USERNAME);
                String password = this.originalConfig.getString(Key.PASSWORD);

                //已经由 prepare 进行了appendJDBCSuffix处理
                String jdbcUrl = this.originalConfig.getString(Key.JDBC_URL);

                String table = this.originalConfig.getString(Key.TABLE);

                List<String> postSqls = this.originalConfig.getList(Key.POST_SQL, String.class);
                List<String> renderedPostSqls = MysqlWriterUtil.renderPreOrPostSqls(postSqls, table);

                if (null != renderedPostSqls && !renderedPostSqls.isEmpty()) {
                    //说明有 postSql 配置，则此处删除掉
                    this.originalConfig.remove(Key.POST_SQL);

                    Connection conn = DBUtil.getConnection(DataBaseType.MySql, jdbcUrl, username, password);

                    LOG.info("Begin to execute postSqls:[{}]. context info:{}.", StringUtils.join(renderedPostSqls, ";"),
                            jdbcUrl);
                    MysqlWriterUtil.executeSqls(conn, renderedPostSqls, jdbcUrl);
                    DBUtil.closeDBResources(null, null, conn);
                }
            }
        }

        @Override
        public void destroy() {
        }

    }

    public static class Slave extends Writer.Slave {
        private static final Logger LOG = LoggerFactory
                .getLogger(MysqlWriter.Slave.class);

        private final static boolean IS_DEBUG = LOG.isDebugEnabled();

        private Configuration writerSliceConfig;

        private String username;
        private String password;
        private String jdbcUrl;
        private String table;
        private List<String> columns;
        private List<String> preSqls;
        private List<String> postSqls;
        private int batchSize;
        private int columnNumber = 0;

        // 作为日志显示信息时，需要附带的通用信息。比如信息所对应的数据库连接等信息，针对哪个表做的操作
        private static String BASIC_MESSAGE;

        private static String INSERT_OR_REPLACE_TEMPLATE;

        private String writeRecordSql;
        private ResultSetMetaData resultSetMetaData;

        @Override
        public void init() {
            this.writerSliceConfig = super.getPluginJobConf();
            this.username = this.writerSliceConfig.getString(Key.USERNAME);
            this.password = this.writerSliceConfig.getString(Key.PASSWORD);
            this.jdbcUrl = this.writerSliceConfig.getString(Key.JDBC_URL);
            this.table = this.writerSliceConfig.getString(Key.TABLE);

            this.columns = this.writerSliceConfig.getList(Key.COLUMN, String.class);
            this.columnNumber = this.columns.size();

            this.preSqls = this.writerSliceConfig.getList(Key.PRE_SQL, String.class);
            this.postSqls = this.writerSliceConfig.getList(Key.POST_SQL, String.class);
            this.batchSize = this.writerSliceConfig.getInt(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_SIZE);

            INSERT_OR_REPLACE_TEMPLATE = this.writerSliceConfig
                    .getString(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK);

            this.writeRecordSql = String.format(INSERT_OR_REPLACE_TEMPLATE, this.table);

            BASIC_MESSAGE = String.format("jdbcUrl:[%s], table:[%s]",
                    this.jdbcUrl, this.table);
        }

        @Override
        public void prepare() {
            Connection connection = DBUtil.getConnection(DataBaseType.MySql, this.jdbcUrl, username,
                    password);

            dealSessionConf(connection, this.writerSliceConfig.getList(Key.SESSION, String.class));

            int tableNumber = this.writerSliceConfig.getInt(Constant.TABLE_NUMBER_MARK).intValue();
            if (tableNumber != 1) {
                LOG.info("Begin to execute preSqls:[{}]. context info:{}.", StringUtils.join(this.preSqls, ";"),
                        BASIC_MESSAGE);
                MysqlWriterUtil.executeSqls(connection, this.preSqls, BASIC_MESSAGE);
            }


            //用于写入数据的时候的类型根据目的表字段类型转换
            this.resultSetMetaData = DBUtil.getColumnMetaData(connection, this.table, StringUtils.join(this.columns, ","));

            DBUtil.closeDBResources(null, null, connection);
        }

        //TODO 改用连接池，确保每次获取的连接都是可用的（注意：连接可能需要每次都初始化其 session）
        public void startWrite(RecordReceiver recordReceiver) {
            Connection connection = DBUtil.getConnection(DataBaseType.MySql, this.jdbcUrl, username,
                    password);

            List<Record> writeBuffer = new ArrayList<Record>(this.batchSize);
            try {
                Record record = null;
                while ((record = recordReceiver.getFromReader()) != null) {
                    if (record.getColumnNumber() != this.columnNumber) {
                        //源头读取字段列数与目的 Mysql 表字段写入列数不相等，直接报错
                        throw DataXException.asDataXException(MysqlWriterErrorCode.CONF_ERROR,
                                String.format("您配置的任务中，源头读取字段数:%s 与 目的 Mysql 表要写入的字段数:%s 不相等. 请检查您的配置字段.",
                                        record.getColumnNumber(), this.columnNumber));
                    }

                    writeBuffer.add(record);

                    if (writeBuffer.size() >= batchSize) {
                        doBatchInsert(connection, writeBuffer);
                        writeBuffer.clear();
                    }
                }
                if (!writeBuffer.isEmpty()) {
                    doBatchInsert(connection, writeBuffer);
                    writeBuffer.clear();
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(MysqlWriterErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                writeBuffer.clear();
                DBUtil.closeDBResources(null, null, connection);
            }
        }

        @Override
        public void post() {
            Connection connection = DBUtil.getConnection(DataBaseType.MySql, this.jdbcUrl, username,
                    password);

            int tableNumber = this.writerSliceConfig.getInt(Constant.TABLE_NUMBER_MARK).intValue();
            if (tableNumber != 1) {
                LOG.info("Begin to execute postSqls:[{}]. context info:{}.", StringUtils.join(this.preSqls, ";"),
                        BASIC_MESSAGE);
                MysqlWriterUtil.executeSqls(connection, this.postSqls, BASIC_MESSAGE);
            }
            DBUtil.closeDBResources(null, null, connection);
        }

        @Override
        public void destroy() {
        }


        private void doBatchInsert(Connection connection, List<Record> buffer) throws SQLException {
            PreparedStatement preparedStatement = null;
            try {
                connection.setAutoCommit(false);
                preparedStatement = connection.prepareStatement(this.writeRecordSql);

                for (Record record : buffer) {
                    preparedStatement = fillPreparedStatement(preparedStatement, record);
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
                connection.commit();
            } catch (SQLException e) {
                LOG.warn("回滚此次写入, 采用每次写入一行方式提交. 因为:" + e.getMessage());
                connection.rollback();
                doOneInsert(connection, buffer);
            } catch (Exception e) {
                throw DataXException.asDataXException(MysqlWriterErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                DBUtil.closeDBResources(preparedStatement, null);
            }
        }

        private void doOneInsert(Connection connection, List<Record> buffer) {
            PreparedStatement preparedStatement = null;
            try {
                connection.setAutoCommit(true);
                preparedStatement = connection.prepareStatement(this.writeRecordSql);

                for (Record record : buffer) {
                    try {
                        preparedStatement = fillPreparedStatement(preparedStatement, record);
                        preparedStatement.execute();
                    } catch (SQLException e) {
                        if (IS_DEBUG) {
                            LOG.debug(e.toString());
                        }

                        this.getSlavePluginCollector().collectDirtyRecord(record, e);
                    } finally {
                        preparedStatement.close();
                    }
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(MysqlWriterErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                DBUtil.closeDBResources(preparedStatement, null);
            }
        }

        private static void dealSessionConf(Connection conn, List<String> sessions) {
            if (null == sessions || sessions.isEmpty()) {
                return;
            }

            Statement stmt;
            try {
                stmt = conn.createStatement();
            } catch (SQLException e) {
                throw DataXException.asDataXException(MysqlWriterErrorCode.SESSION_ERROR,
                        String.format("执行 session 设置失败. 上下文信息是:[%s] .", BASIC_MESSAGE), e);
            }

            for (String sessionSql : sessions) {
                LOG.info("execute sql:[{}]", sessionSql);
                try {
                    DBUtil.executeSqlWithoutResultSet(stmt, sessionSql);
                } catch (SQLException e) {
                    throw DataXException.asDataXException(MysqlWriterErrorCode.SESSION_ERROR,
                            String.format("执行 session 设置失败. 上下文信息是:[%s] .", BASIC_MESSAGE), e);
                }
            }

            DBUtil.closeDBResources(stmt, null);
        }

        //直接使用了两个类变量：columnNumber,resultSetMetaData
        //TODO 时间类型
        private PreparedStatement fillPreparedStatement(PreparedStatement preparedStatement,
                                                        Record record) throws SQLException {
            for (int i = 0; i < this.columnNumber; i++) {
                switch (this.resultSetMetaData.getColumnType(i + 1)) {

                    case Types.CHAR:
                    case Types.NCHAR:
                    case Types.CLOB:
                    case Types.NCLOB:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.NVARCHAR:
                    case Types.LONGNVARCHAR:
                        preparedStatement.setString(i + 1, record.getColumn(i).asString());
                        break;
                    case Types.SMALLINT:
                    case Types.TINYINT:
                    case Types.INTEGER:
                    case Types.BIGINT:
                        preparedStatement.setLong(i + 1, record.getColumn(i).asLong());
                        break;
                    case Types.NUMERIC:
                    case Types.DECIMAL:
                    case Types.FLOAT:
                    case Types.REAL:
                    case Types.DOUBLE:
                        preparedStatement.setDouble(i + 1, record.getColumn(i).asDouble());
                        break;
//                    case Types.TIME:
//                        preparedStatement.setTime(i + 1, record.getColumn(i).asDate());
//                        break;
//                    case Types.DATE:
//                        preparedStatement.setDate(i + 1, record.getColumn(i).asDate());
//                        break;
//                    case Types.TIMESTAMP:
//                        preparedStatement.setDate(i + 1, record.getColumn(i).asDate());
//                        break;
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.BLOB:
                    case Types.LONGVARBINARY:
                        preparedStatement.setBytes(i + 1, record.getColumn(i).asBytes());
                        break;
                    case Types.BOOLEAN:
                    case Types.BIT:
                        preparedStatement.setBoolean(i + 1, record.getColumn(i).asBoolean());
                        break;
                    default:
                        throw DataXException.asDataXException(DBUtilErrorCode.UNSUPPORTED_TYPE,
                                String.format(
                                        "DataX 不支持数据库写入这种字段类型. ColumnName:[%s], ColumnType:[%s], ColumnClassName:[%s].",
                                        this.resultSetMetaData.getColumnName(i),
                                        this.resultSetMetaData.getColumnType(i),
                                        this.resultSetMetaData.getColumnClassName(i)));
                }
            }
            
            return preparedStatement;
        }
    }
}
