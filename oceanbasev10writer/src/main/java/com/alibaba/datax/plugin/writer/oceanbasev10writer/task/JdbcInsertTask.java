package com.alibaba.datax.plugin.writer.oceanbasev10writer.task;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.Config;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.ext.ObClientConnHolder;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.ext.ServerConnectInfo;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.util.ObWriterUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Queue;

public class JdbcInsertTask extends AbstractInsertTask {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcInsertTask.class);
    private String writeRecordSql;

    // 失败重试次数
    private int failTryCount = Config.DEFAULT_FAIL_TRY_COUNT;

    public JdbcInsertTask(long taskId, Queue<List<Record>> recordsQueue, Configuration config,
                          ServerConnectInfo connectInfo) {
        super(taskId, recordsQueue, config, connectInfo);
        this.writeRecordSql = writer.getRewriteRecordSql();
        this.failTryCount = config.getInt(Config.FAIL_TRY_COUNT, Config.DEFAULT_FAIL_TRY_COUNT);
    }

    @Override
    public void initConnHolder() {
        this.connHolder = new ObClientConnHolder(config, connInfo.jdbcUrl, connInfo.getFullUserName(), connInfo.password);
        this.connHolder.initConnection();
    }

    @Override
    protected void write(List<Record> records) {
        checkMemstore();
        Connection conn = connHolder.getConn();
        boolean success = false;
        long cost = 0;
        long startTime = 0;
        try {
            for (int i = 0; i < failTryCount; ++i) {
                if (i > 0) {
                    conn = connHolder.getConn();
                    LOG.info("retry {}, start do batch insert, size={}", i, records.size());
                    checkMemstore();
                }
                startTime = System.currentTimeMillis();
                PreparedStatement ps = null;
                try {
                    conn.setAutoCommit(false);
                    ps = conn.prepareStatement(writeRecordSql);
                    for (Record record : records) {
                        ps = writerTask.fillStatement(ps, record);
                        ps.addBatch();
                    }
                    ps.executeBatch();
                    conn.commit();
                    success = true;
                    cost = System.currentTimeMillis() - startTime;
                    calStatistic(cost);
                    break;
                } catch (SQLException e) {
                    LOG.warn("Insert fatal error SqlState ={}, errorCode = {}, {}", e.getSQLState(), e.getErrorCode(), e);
                    if (LOG.isDebugEnabled() && (i == 0 || i > 10)) {
                        for (Record record : records) {
                            LOG.warn("ERROR : record {}", record);
                        }
                    }
                    // 按照错误码分类，分情况处理
                    // 如果是OB系统级异常,则需要重建连接
                    boolean fatalFail = ObWriterUtils.isFatalError(e);
                    if (fatalFail) {
                        ObWriterUtils.sleep(300000);
                        connHolder.reconnect();
                        // 如果是可恢复的异常,则重试
                    } else if (ObWriterUtils.isRecoverableError(e)) {
                        conn.rollback();
                        ObWriterUtils.sleep(60000);
                    } else {// 其它异常直接退出,采用逐条写入方式
                        conn.rollback();
                        ObWriterUtils.sleep(1000);
                        break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    LOG.warn("Insert error unexpected {}", e);
                } finally {
                    DBUtil.closeDBResources(ps, null);
                }
            }
        } catch (SQLException e) {
            LOG.warn("ERROR:retry failSql State ={}, errorCode = {}, {}", e.getSQLState(), e.getErrorCode(), e);
        }

        if (!success) {
            LOG.info("do one insert");
            conn = connHolder.reconnect();
            writerTask.doOneInsert(conn, records);
            cost = System.currentTimeMillis() - startTime;
            calStatistic(cost);
        }
    }

    private void checkMemstore() {
        if (writerTask.isShouldSlow()) {
            ObWriterUtils.sleep(100);
        } else {
            while (writerTask.isShouldPause()) {
                ObWriterUtils.sleep(100);
            }
        }
    }
}
