package com.alibaba.datax.plugin.writer.mysqlwriter;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.writer.mysqlwriter.util.MysqlWriterUtil;
import com.alibaba.datax.plugin.writer.mysqlwriter.util.OriginalConfPretreatmentUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
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
                LOG.debug("after master init(), originalConfig now is:[\n{}\n]",
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
                LOG.debug("after master prepare(), originalConfig now is:[\n{}\n]",
                        this.originalConfig.toJSON());
            }
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            return MysqlWriterUtil.doSplit(this.originalConfig,
                    adviceNumber);
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
        private List<String> preSqls;
        private List<String> postSqls;
        private int batchSize;
        private int columnNumber = 0;

        // 作为日志显示信息时，需要附带的通用信息。比如信息所对应的数据库连接等信息，针对哪个表做的操作
        private static String BASIC_MESSAGE;

        private static String INSERT_OR_REPLACE_TEMPLATE;

        private String writeRecordSql;

        @Override
        public void init() {
            this.writerSliceConfig = super.getPluginJobConf();
            this.username = this.writerSliceConfig.getString(Key.USERNAME);
            this.password = this.writerSliceConfig.getString(Key.PASSWORD);
            this.jdbcUrl = this.writerSliceConfig.getString(Key.JDBC_URL);
            this.table = this.writerSliceConfig.getString(Key.TABLE);

            this.columnNumber = this.writerSliceConfig.getList(Key.COLUMN, String.class).size();

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
                throw new DataXException(MysqlWriterErrorCode.WRITE_DATA_ERROR, e);
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
                    for (int i = 0; i < this.columnNumber; i++) {
                        preparedStatement = buildPreparedStatement(preparedStatement, record.getColumn(i),
                                i + 1);
                    }
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
                connection.commit();
            } catch (SQLException e) {
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
                        for (int i = 0; i < this.columnNumber; i++) {
                            preparedStatement = buildPreparedStatement(preparedStatement, record.getColumn(i),
                                    i + 1);
                        }

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
                LOG.error("error while createStatement, {}.", BASIC_MESSAGE);
                throw new DataXException(MysqlWriterErrorCode.SESSION_ERROR, e);
            }

            for (String sessionSql : sessions) {
                LOG.info("execute sql:[{}]", sessionSql);
                try {
                    DBUtil.executeSqlWithoutResultSet(stmt, sessionSql);
                } catch (SQLException e) {
                    LOG.error("execute sql:[{}] failed, {}.", sessionSql,
                            BASIC_MESSAGE);
                    throw new DataXException(
                            MysqlWriterErrorCode.SESSION_ERROR, e);
                }
            }

            DBUtil.closeDBResources(stmt, null);
        }

        private PreparedStatement buildPreparedStatement(PreparedStatement preparedStatement, Column tempColumn,
                                                         int index) throws SQLException {

            preparedStatement.setString(index, tempColumn.asString());

            return preparedStatement;

//            Column.Type type = tempColumn.getType();
//            switch (type) {
//                case STRING:
//                case LONG:
//                case DOUBLE:
//                    preparedStatement.setString(index, tempColumn.asString());
//                    break;
//
//                case DATE:
//                    preparedStatement.setObject(index, tempColumn.asDate());
//                    break;
//
//                case BYTES:
//                    preparedStatement.setBytes(index, tempColumn.asBytes());
//                    break;
//
//                case BOOL:
//                    preparedStatement.setBoolean(index, tempColumn.asBoolean());
//                    break;
//                case NULL:
//                    preparedStatement.setObject(index, null);
//                    break;
//                default:
//                    throw new DataXException(MysqlWriterErrorCode.UNSUPPORTED_DATA_TYPE,
//                            String.format("Unsupported data type=[%s].", type));
//            }
//
//            return preparedStatement;
        }

    }

}
