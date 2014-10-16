package com.alibaba.datax.plugin.reader.mysqlreader;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.SqlFormatUtil;
import com.alibaba.datax.plugin.reader.mysqlreader.util.MysqlReaderSplitUtil;
import com.alibaba.datax.plugin.reader.mysqlreader.util.OriginalConfPretreatmentUtil;

public class MysqlReader extends Reader {

    public static class Master extends Reader.Master {
        private static final Logger LOG = LoggerFactory
                .getLogger(MysqlReader.Master.class);

        private static final boolean IS_DEBUG = LOG.isDebugEnabled();

        private Configuration originalConfig = null;

        @Override
        public void init() {
            this.originalConfig = getPluginJobConf();

            OriginalConfPretreatmentUtil.doPretreatment(this.originalConfig);

            if (IS_DEBUG) {
                LOG.debug("After init(), the originalConfig now is:[\n{}\n]",
                        this.originalConfig.toJSON());
            }
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            return MysqlReaderSplitUtil.doSplit(this.originalConfig,
                    adviceNumber);
        }

        @Override
        public void post() {
            LOG.debug("post()");
        }

        @Override
        public void destroy() {
            LOG.debug("destroy()");
        }

    }

    public static class Slave extends Reader.Slave {
        private static final Logger LOG = LoggerFactory
                .getLogger(MysqlReader.Slave.class);

        private Configuration readerSliceConfig;

        private String username;

        private String password;

        private String jdbcUrl;

        // 作为日志显示信息时，需要附带的通用信息。比如信息所对应的数据库连接等信息，针对哪个表做的操作
        private static String BASIC_MESSAGE;

        @Override
        public void init() {
            this.readerSliceConfig = getPluginJobConf();

			/* for database connection */

            this.username = this.readerSliceConfig.getString(Key.USERNAME);
            this.password = this.readerSliceConfig.getString(Key.PASSWORD);
            this.jdbcUrl = this.readerSliceConfig.getString(Key.JDBC_URL);

            BASIC_MESSAGE = String.format("jdbcUrl:[%s]", this.jdbcUrl);
        }

        @Override
        public void startRead(RecordSender recordSender) {
            String querySql = this.readerSliceConfig.getString(Key.QUERY_SQL);
            String formattedSql = null;

            try {
                formattedSql = SqlFormatUtil.format(querySql);
            } catch (Exception unused) {
                // ignore it
            }
            LOG.info("\nSql [{}] \nTo jdbcUrl:[{}].",
                    null != formattedSql ? formattedSql : querySql, jdbcUrl);

            Connection conn = DBUtil.getConnection(DataBaseType.MySql, jdbcUrl, username,
                    password);

            int columnNumber = 0;
            ResultSet rs = null;
            try {
                rs = DBUtil.query(conn, querySql, Integer.MIN_VALUE);
                ResultSetMetaData metaData = rs.getMetaData();
                columnNumber = rs.getMetaData().getColumnCount();

                while (rs.next()) {
                    transportOneRecord(recordSender, rs, metaData, columnNumber);
                }

            } catch (Exception e) {
                LOG.error("Origin cause:[{}].", e.getMessage());
                throw new DataXException(MysqlReaderErrorCode.READ_RECORD_FAIL,
                        e);
            } finally {
                DBUtil.closeDBResources(null, conn);
            }
        }

        private void transportOneRecord(RecordSender recordSender,
                                        ResultSet rs, ResultSetMetaData metaData, int columnNumber) {
            Record record = recordSender.createRecord();

            try {
                for (int i = 1; i <= columnNumber; i++) {
                    switch (metaData.getColumnType(i)) {

                        case Types.CHAR:
                        case Types.NCHAR:
                        case Types.CLOB:
                        case Types.NCLOB:
                        case Types.VARCHAR:
                        case Types.LONGVARCHAR:
                        case Types.NVARCHAR:
                        case Types.LONGNVARCHAR:
                            record.addColumn(new StringColumn(rs.getString(i)));
                            break;

                        case Types.SMALLINT:
                        case Types.TINYINT:
                        case Types.INTEGER:
                        case Types.BIGINT:
                            record.addColumn(new LongColumn(rs.getLong(i)));
                            break;

                        case Types.NUMERIC:
                        case Types.DECIMAL:
                            record.addColumn(new DoubleColumn(rs.getString(i)));
                            break;

                        case Types.FLOAT:
                        case Types.REAL:
                        case Types.DOUBLE:
                            record.addColumn(new DoubleColumn(rs.getDouble(i)));
                            break;

                        case Types.TIME:
                            record.addColumn(new DateColumn(rs.getTime(i)));
                            break;

                        case Types.DATE:
                            record.addColumn(new DateColumn(rs.getDate(i)));
                            break;

                        case Types.TIMESTAMP:
                            record.addColumn(new DateColumn(rs.getTimestamp(i)));
                            break;

                        case Types.BINARY:
                        case Types.VARBINARY:
                        case Types.BLOB:
                        case Types.LONGVARBINARY:
                            record.addColumn(new BytesColumn(rs.getBytes(i)));
                            break;

                        // TODO  可能要把 bit 删除
                        case Types.BOOLEAN:
                        case Types.BIT:
                            record.addColumn(new BoolColumn(rs.getBoolean(i)));
                            break;

                        // TODO 添加BASIC_MESSAGE
                        default:
                            throw new Exception(
                                    String.format(
                                            "Unsupported Mysql Data Type. ColumnName:[%s], ColumnType:[%s], ColumnClassName:[%s].",
                                            metaData.getColumnName(i),
                                            metaData.getColumnType(i),
                                            metaData.getColumnClassName(i)));
                    }

                }
                recordSender.sendToWriter(record);
            } catch (Exception e) {
                this.getSlavePluginCollector().collectDirtyRecord(record, e);
            }
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }

    }

}
