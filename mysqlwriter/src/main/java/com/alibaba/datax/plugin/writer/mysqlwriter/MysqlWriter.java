package com.alibaba.datax.plugin.writer.mysqlwriter;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.writer.mysqlwriter.util.MysqlWriterSplitUtil;
import com.alibaba.datax.plugin.writer.mysqlwriter.util.OriginalConfPretreatmentUtil;
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
            this.originalConfig = getPluginJobConf();
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
                String jdbcUrl = this.originalConfig.getString(Key.JDBC_URL);
                List<String> preSqls = this.originalConfig.getList(Key.PRE_SQL, String.class);

                Connection conn = DBUtil.getConnection(DataBaseType.MySql, jdbcUrl, username, password);
                MysqlWriterSplitUtil.executeSqls(conn, preSqls, jdbcUrl);
            }
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            return MysqlWriterSplitUtil.doSplit(this.originalConfig,
                    adviceNumber);
        }

        // 一般来说，是需要推迟到 slave 中进行post 的执行（单表情况例外）
        @Override
        public void post() {
            int tableNumber = this.originalConfig.getInt(Constant.TABLE_NUMBER_MARK).intValue();
            if (tableNumber == 1) {
                String username = this.originalConfig.getString(Key.USERNAME);
                String password = this.originalConfig.getString(Key.PASSWORD);
                String jdbcUrl = this.originalConfig.getString(Key.JDBC_URL);
                List<String> postSqls = this.originalConfig.getList(Key.POST_SQL, String.class);

                Connection conn = DBUtil.getConnection(DataBaseType.MySql, jdbcUrl, username, password);
                MysqlWriterSplitUtil.executeSqls(conn, postSqls, jdbcUrl);
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
        private Connection conn;
        private int columnNumber = 0;

        // 作为日志显示信息时，需要附带的通用信息。比如信息所对应的数据库连接等信息，针对哪个表做的操作
        private static String BASIC_MESSAGE;

        private static String INSERT_OR_REPLACE_TEMPLATE;

        private String writeRecordSql;

        @Override
        public void init() {
            this.writerSliceConfig = getPluginJobConf();
            this.username = this.writerSliceConfig.getString(Key.USERNAME);
            this.password = this.writerSliceConfig.getString(Key.PASSWORD);
            this.jdbcUrl = this.writerSliceConfig.getString(Key.JDBC_URL);
            this.table = this.writerSliceConfig.getString(Key.TABLE);

            this.columnNumber = this.writerSliceConfig.getList(Key.COLUMN, String.class).size();

            this.preSqls = this.writerSliceConfig.getList(Key.PRE_SQL, String.class);
            this.postSqls = this.writerSliceConfig.getList(Key.POST_SQL, String.class);
            this.batchSize = this.writerSliceConfig.getInt(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_SIZE);

            this.conn = DBUtil.getConnection(DataBaseType.MySql, this.jdbcUrl, username,
                    password);

            INSERT_OR_REPLACE_TEMPLATE = this.writerSliceConfig
                    .getString(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK);

            this.writeRecordSql = String.format(INSERT_OR_REPLACE_TEMPLATE, this.table);

            BASIC_MESSAGE = String.format("jdbcUrl:[%s], table:[%s]",
                    this.jdbcUrl, this.table);
        }

        @Override
        public void prepare() {
            dealSessionConf(conn,
                    this.writerSliceConfig.getList(Key.SESSION, String.class));

            MysqlWriterSplitUtil.executeSqls(this.conn, this.preSqls, BASIC_MESSAGE);
        }

        public void startWrite(RecordReceiver recordReceiver) {
            List<Record> writeBuffer = new ArrayList<Record>(this.batchSize);
            try {
                Record record = null;
                while ((record = recordReceiver.getFromReader()) != null) {
                    writeBuffer.add(record);
                    if (writeBuffer.size() >= batchSize) {
                        doBatchInsert(conn, writeBuffer);
                        writeBuffer.clear();
                    }
                }
                if (writeBuffer.size() != 0) {
                    doBatchInsert(conn, writeBuffer);
                    writeBuffer.clear();
                }
            } catch (Exception e) {
                throw new DataXException(MysqlWriterErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                writeBuffer.clear();
            }
        }

        @Override
        public void post() {
            MysqlWriterSplitUtil.executeSqls(this.conn, this.postSqls, BASIC_MESSAGE);
        }

        @Override
        public void destroy() {
        }


        private void doBatchInsert(Connection connection, List<Record> buffer) throws SQLException {
            PreparedStatement pstmt = null;
            try {
                connection.setAutoCommit(false);
                pstmt = connection.prepareStatement(this.writeRecordSql);

                for (Record record : buffer) {
                    for (int i = 0; i < this.columnNumber; i++) {
                        pstmt = buildPreparedStatement(record.getColumn(i),
                                pstmt, i + 1);
                    }
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
                connection.commit();
            } catch (Exception e) {
                connection.rollback();
                doBadInsert(connection, buffer);
            } finally {
                DBUtil.closeDBResources(pstmt, null);
            }
        }

        private void doBadInsert(Connection connection, List<Record> buffer) {
            PreparedStatement pstmt = null;
            try {
                connection.setAutoCommit(true);
                for (Record record : buffer) {
                    try {
                        pstmt = connection.prepareStatement(this.writeRecordSql);
                        for (int i = 0; i < this.columnNumber; i++) {
                            pstmt = buildPreparedStatement(record.getColumn(i),
                                    pstmt, i + 1);
                        }

                        pstmt.execute();
                        pstmt.close();
                    } catch (Exception e) {
                        if (IS_DEBUG) {
                            LOG.debug(e.toString());
                        }

                        this.getSlavePluginCollector().collectDirtyRecord(record, e);
                    } finally {
                        DBUtil.closeDBResources(pstmt, null);
                    }
                }
            } catch (Exception e) {
                throw new DataXException(MysqlWriterErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                DBUtil.closeDBResources(pstmt, null);
            }
        }

        private static void dealSessionConf(Connection conn, List<String> sessionConfs) {
            if (null == sessionConfs || sessionConfs.isEmpty()) {
                return;
            }

            Statement stmt;
            try {
                stmt = conn.createStatement();
            } catch (SQLException e) {
                LOG.error("error while createStatement, {}.", BASIC_MESSAGE);
                throw new DataXException(MysqlWriterErrorCode.SESSION_ERROR, e);
            }

            for (String sessionSql : sessionConfs) {
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

        private PreparedStatement buildPreparedStatement(Column tempColumn, PreparedStatement pstmt,
                                                         int index) throws Exception {
            if (tempColumn instanceof StringColumn
                    || tempColumn instanceof LongColumn
                    || tempColumn instanceof DoubleColumn) {
                pstmt.setString(index, tempColumn.asString());
            } else if (tempColumn instanceof BytesColumn) {
                pstmt.setBytes(index, tempColumn.asBytes());
            } else if (tempColumn instanceof DateColumn) {
                pstmt.setObject(index, tempColumn.asDate());
            } else if (tempColumn instanceof BoolColumn) {
                pstmt.setBoolean(index, tempColumn.asBoolean());
            }
            return pstmt;
        }

    }

}
