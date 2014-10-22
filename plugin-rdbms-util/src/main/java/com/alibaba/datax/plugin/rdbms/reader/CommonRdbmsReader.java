package com.alibaba.datax.plugin.rdbms.reader;


import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.SlavePluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.StrUtil;
import com.alibaba.datax.plugin.rdbms.reader.util.OriginalConfPretreatmentUtil;
import com.alibaba.datax.plugin.rdbms.reader.util.ReaderSplitUtil;
import com.alibaba.datax.plugin.rdbms.reader.util.SingleTableSplitUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.SqlFormatUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.List;

public class CommonRdbmsReader {

    private static DataBaseType DATABASE_TYPE;

    public static class Master {
        private static final Logger LOG = LoggerFactory
                .getLogger(CommonRdbmsReader.Master.class);

        private static final boolean IS_DEBUG = LOG.isDebugEnabled();

        public Master(DataBaseType dataBaseType) {
            DATABASE_TYPE = dataBaseType;
            OriginalConfPretreatmentUtil.DATABASE_TYPE = dataBaseType;
            SingleTableSplitUtil.DATABASE_TYPE = dataBaseType;
        }

        public void init(Configuration originalConfig) {

            OriginalConfPretreatmentUtil.doPretreatment(originalConfig);

            if (IS_DEBUG) {
                LOG.debug("After master init, job config now is:[\n{}\n]",
                        originalConfig.toJSON());
            }
        }

        public List<Configuration> split(Configuration originalConfig, int adviceNumber) {
            return ReaderSplitUtil.doSplit(originalConfig, adviceNumber);
        }

        public void post(Configuration originalConfig) {
            //do nothing
        }

        public void destroy(Configuration originalConfig) {
            //do nothing
        }

    }

    public static class Slave {
        private static final Logger LOG = LoggerFactory
                .getLogger(CommonRdbmsReader.Slave.class);

        private String username;
        private String password;
        private String jdbcUrl;

        // 作为日志显示信息时，需要附带的通用信息。比如信息所对应的数据库连接等信息，针对哪个表做的操作
        private static String BASIC_MESSAGE;

        public Slave(DataBaseType dataBaseType) {
            DATABASE_TYPE = dataBaseType;
        }

        public void init(Configuration readerSliceConfig) {

			/* for database connection */

            this.username = readerSliceConfig.getString(Key.USERNAME);
            this.password = readerSliceConfig.getString(Key.PASSWORD);
            this.jdbcUrl = readerSliceConfig.getString(Key.JDBC_URL);

            BASIC_MESSAGE = String.format("jdbcUrl:[%s]", this.jdbcUrl);
        }

        public void startRead(Configuration readerSliceConfig, RecordSender recordSender,
                              SlavePluginCollector slavePluginCollector, int fetchSize) {
            String querySql = readerSliceConfig.getString(Key.QUERY_SQL);
            String formattedSql = null;

            try {
                formattedSql = SqlFormatUtil.format(querySql);
            } catch (Exception unused) {
                // ignore it
            }
            LOG.info("\nSql [{}] \nTo jdbcUrl:[{}].",
                    null != formattedSql ? formattedSql : querySql, jdbcUrl);

            Connection conn = DBUtil.getConnection(DATABASE_TYPE, jdbcUrl,
                    username, password);

            int columnNumber = 0;
            ResultSet rs = null;
            try {
                rs = DBUtil.query(conn, querySql, fetchSize);
                ResultSetMetaData metaData = rs.getMetaData();
                columnNumber = rs.getMetaData().getColumnCount();

                while (rs.next()) {
                    ResultSetReadProxy.transportOneRecord(recordSender, rs, metaData, columnNumber,
                            slavePluginCollector);
                }

            } catch (Exception e) {
                String businessMessage = String.format("Read record failed, %s, detail:[%s]",
                        BASIC_MESSAGE, e.getMessage());
                String message = StrUtil.buildOriginalCauseMessage(businessMessage, e);
                LOG.error(message);

                throw new DataXException(DBUtilErrorCode.READ_RECORD_FAIL, e);
            } finally {
                DBUtil.closeDBResources(null, conn);
            }
        }

        public void post(Configuration originalConfig) {
            //do nothing
        }

        public void destroy(Configuration originalConfig) {
            //do nothing
        }

    }

}

