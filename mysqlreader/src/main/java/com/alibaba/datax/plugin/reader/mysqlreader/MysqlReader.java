package com.alibaba.datax.plugin.reader.mysqlreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.SlavePluginCollector;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.SqlFormatUtil;
import com.alibaba.datax.plugin.reader.mysqlreader.util.MysqlReaderSplitUtil;
import com.alibaba.datax.plugin.reader.mysqlreader.util.OriginalConfPretreatmentUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.List;

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

            Connection conn = DBUtil.getConnection(DataBaseType.MySql, jdbcUrl,
                    username, password);

            int columnNumber = 0;
            ResultSet rs = null;
            try {
                rs = DBUtil.query(conn, querySql, Integer.MIN_VALUE);
                ResultSetMetaData metaData = rs.getMetaData();
                columnNumber = rs.getMetaData().getColumnCount();

                SlavePluginCollector slavePluginCollector = this.getSlavePluginCollector();
                while (rs.next()) {
                    MysqlReaderProxy.transportOneRecord(recordSender, rs, metaData, columnNumber,
                            slavePluginCollector);
                }

            } catch (Exception e) {
                LOG.error("Origin cause:[{}].", e.getMessage());
                throw new DataXException(MysqlReaderErrorCode.READ_RECORD_FAIL,
                        e);
            } finally {
                DBUtil.closeDBResources(null, conn);
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
