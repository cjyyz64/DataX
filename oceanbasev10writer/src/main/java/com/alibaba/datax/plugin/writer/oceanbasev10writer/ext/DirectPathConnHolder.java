package com.alibaba.datax.plugin.writer.oceanbasev10writer.ext;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.Config;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.common.Table;

import com.alipay.oceanbase.rpc.protocol.payload.impl.direct_load.ObLoadDupActionType;
import com.oceanbase.directpath.jdbc.DirectPathConnection;

import java.sql.Connection;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class DirectPathConnHolder extends AbstractConnHolder {
    /**
     * The server side timeout.
     */
    private static final long SERVER_TIMEOUT = 24L * 60 * 60 * 1000 * 1000;

    private static final ConcurrentHashMap<Table, DirectPathConnection> cache = new ConcurrentHashMap<>();

    private String tableName;
    private String host;
    private int rpcPort;
    private String tenantName;
    private String databaseName;
    private int blocks;
    private int threads;
    private int maxErrors;
    private ObLoadDupActionType duplicateKeyAction;

    public DirectPathConnHolder(Configuration config, ServerConnectInfo connectInfo, String tableName, int threads) {
        super(config, connectInfo.jdbcUrl, connectInfo.userName, connectInfo.password);
        this.host = connectInfo.host;
        this.rpcPort = connectInfo.rpcPort;
        this.tenantName = connectInfo.tenantName;
        this.databaseName = connectInfo.databaseName;
        this.tableName = tableName;
        this.threads = threads;
        this.blocks = config.getInt(Config.BLOCKS_COUNT);
        this.maxErrors = config.getInt(Config.MAX_ERRORS, 0);
        this.duplicateKeyAction = "insert".equalsIgnoreCase(config.getString(Config.OB_WRITE_MODE)) ? ObLoadDupActionType.IGNORE : ObLoadDupActionType.REPLACE;
    }

    @Override
    public Connection initConnection() {
        synchronized (cache) {
            conn = cache.computeIfAbsent(new Table(databaseName, tableName), e -> {
                try {
                    return new DirectPathConnection.Builder().host(host) //
                            .port(rpcPort) //
                            .tenant(tenantName) //
                            .user(userName) //
                            .password(Optional.ofNullable(password).orElse("")) //
                            .schema(databaseName) //
                            .table(tableName) //
                            .blocks(blocks) //
                            .parallel(threads) //
                            .maxErrorCount(maxErrors) //
                            .duplicateKeyAction(duplicateKeyAction) //
                            .serverTimeout(SERVER_TIMEOUT) //
                            .build();
                } catch (Exception ex) {
                    throw DataXException.asDataXException(DBUtilErrorCode.CONN_DB_ERROR, ex);
                }
            });
        }
        return conn;
    }

    @Override
    public String getJdbcUrl() {
        return null;
    }

    @Override
    public String getUserName() {
        return null;
    }

    @Override
    public void destroy() {

    }

    @Override
    public void doCommit() {

    }
}
