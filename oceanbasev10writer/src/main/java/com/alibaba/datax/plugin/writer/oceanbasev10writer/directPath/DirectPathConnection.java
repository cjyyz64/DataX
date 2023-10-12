package com.alibaba.datax.plugin.writer.oceanbasev10writer.directPath;

import static com.google.common.base.Preconditions.checkArgument;

import com.alipay.oceanbase.rpc.property.Property;
import com.alipay.oceanbase.rpc.protocol.payload.impl.direct_load.ObLoadDupActionType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.direct_load.ObTableLoadClientStatus;
import com.alipay.oceanbase.rpc.table.ObDirectLoadBucket;
import com.alipay.oceanbase.rpc.table.ObDirectLoadParameter;
import com.alipay.oceanbase.rpc.table.ObTable;
import com.alipay.oceanbase.rpc.table.ObTableDirectLoad;
import com.google.common.base.Stopwatch;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public final class DirectPathConnection extends AbstractRestrictedConnection {
    private static final Logger LOG = LoggerFactory.getLogger(DirectPathConnection.class);

    private static final int BUSY_WAIT_TIME_MILLIS = 50;
    private static final int OB_DIRECT_PATH_DEFAULT_BLOCKS = 1;
    private static final int OB_DIRECT_PATH_COMMIT_TIMEOUT = Integer.MAX_VALUE;
    private static final int OB_DIRECT_PATH_HEART_BEAT_TIMEOUT = 30000000;
    private static final long OB_DIRECT_PATH_COMMIT_TIMEOUT_NANOS = TimeUnit.MILLISECONDS.toNanos(OB_DIRECT_PATH_COMMIT_TIMEOUT);

    private static final String OB_DIRECT_PATH_RPC_CONNECT_TIMEOUT = "3000";
    private static final String OB_DIRECT_PATH_RPC_EXECUTE_TIMEOUT = "10000";
    private static final String OB_DIRECT_PATH_RPC_CONNECT_TRY_TIMES = "5";

    private DirectPathConnection.State state;
    private int commiters;

    private final int blocks;
    private final ObTableDirectLoad load;
    private final Object lock = new Object();

    /**
     * Construct a new instance.
     *
     * @param load
     * @param blocks
     */
    private DirectPathConnection(ObTableDirectLoad load, int blocks) {
        this.load = load;
        this.blocks = blocks;
    }

    /**
     * Begin a new {@link DirectPathConnection }
     *
     * @return DirectPathConnection
     * @throws SQLException
     */
    DirectPathConnection begin() throws SQLException {
        synchronized (lock) {
            if (state == null || state == DirectPathConnection.State.CLOSED) {
                try {
                    this.load.begin();
                    this.state = DirectPathConnection.State.BEGIN;
                } catch (Exception ex) {
                    throw new SQLException(ex);
                }
            } else {
                throw new IllegalStateException("Begin transaction failed as connection state is already BEGIN");
            }
        }
        return this;
    }

    /**
     * Commit buffered data with MAXIMUM timeout.
     *
     * @throws SQLException
     */
    @Override
    public void commit() throws SQLException {
        commit(Integer.MAX_VALUE);
    }

    /**
     * Commit buffered data with specified timeout.
     *
     * @param timeout
     * @throws SQLException
     */
    public void commit(int timeout) throws SQLException {
        synchronized (lock) {
            if (state == DirectPathConnection.State.BEGIN) {
                this.commiters++;
                if (commiters == blocks) {
                    try {
                        this.load.commit();
                        this.waitServerCommit(timeout);
                        state = DirectPathConnection.State.FINISHED;
                    } catch (Exception ex) {
                        throw new SQLException(ex);
                    }
                } else if (commiters > blocks) {
                    throw new IllegalStateException("Your commit have exceed the limit. (" + commiters + ">" + blocks + ")");
                }
            } else {
                throw new IllegalStateException("Commit transaction failed as connection state is not BEGIN");
            }
        }
    }

    /**
     * Rollback if error occurred.
     *
     * @throws SQLException
     */
    @Override
    public void rollback() throws SQLException {
        synchronized (lock) {
            if (state == DirectPathConnection.State.BEGIN) {
                try {
                    this.load.abort();
                } catch (Exception ex) {
                    throw new SQLException(ex);
                }
            } else {
                throw new IllegalStateException("Rollback transaction failed as connection state is not BEGIN");
            }
        }
    }

    /**
     * Close this connection.
     */
    @Override
    public void close() {
        synchronized (lock) {
            // Closed only if state is BEGIN
            if (state != null && state != DirectPathConnection.State.CLOSED) {
                try {
                    ObTableLoadClientStatus status = this.load.getStatus();
                    if (status == ObTableLoadClientStatus.RUNNING || status == ObTableLoadClientStatus.ERROR) {
                        LOG.warn("Unexpected status: {}, aborting load task on table \"{}\"...", this.load.getStatus(), getTableName());
                        this.load.abort();
                    }
                } catch (Exception e) {
                    LOG.error("Close direct load connection failed. Error: ", e);
                }
                this.load.getTable().close();
                this.state = DirectPathConnection.State.CLOSED;
            }
        }
    }

    /**
     * @return DirectPathPreparedStatement
     */
    @Override
    public DirectPathPreparedStatement createStatement() throws SQLException {
        return this.prepareStatement(null);
    }

    /**
     * A new batch need create a new {@link DirectPathPreparedStatement }.
     * The {@link DirectPathPreparedStatement } can not be reuse, otherwise it may cause duplicate records.
     *
     * @return DirectPathStatement
     */
    @Override
    public DirectPathPreparedStatement prepareStatement(String sql) throws SQLException {
        if (state == DirectPathConnection.State.BEGIN) {
            return new DirectPathPreparedStatement(this);
        } else {
            throw new IllegalStateException("Create statement failed as connection state is not BEGIN");
        }
    }

    /**
     * Return the schema name of this connection instance.
     *
     * @return String
     */
    @Override
    public String getSchema() {
        if (state == DirectPathConnection.State.BEGIN) {
            return this.load.getTable().getDatabase();
        } else {
            throw new IllegalStateException("Get schema failed as connection state is not BEGIN");
        }
    }

    /**
     * Return the table name of this connection instance.
     *
     * @return String
     */
    public String getTableName() {
        if (state == DirectPathConnection.State.BEGIN) {
            return this.load.getTableName();
        } else {
            throw new IllegalStateException("Get table failed as connection state is not BEGIN");
        }
    }

    /**
     * Return whether this connection is closed.
     *
     * @return boolean
     */
    @Override
    public boolean isClosed() {
        synchronized (lock) {
            return this.state == DirectPathConnection.State.CLOSED;
        }
    }

    public boolean isFinished() {
        return this.state.equals(DirectPathConnection.State.FINISHED);
    }

    /**
     * Insert bucket data into buffer.
     *
     * @param bucket
     * @return int[]
     * @throws SQLException
     */
    int[] insert(ObDirectLoadBucket bucket) throws SQLException {
        try {
            this.load.insert(bucket);
            int[] result = new int[bucket.getRowCount()];
            Arrays.fill(result, 1);
            return result;
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
    }

    /**
     * Block and wait server committing the loaded data.
     *
     * @param timeoutMillis Client timeout. (TimeUnit.MILLIS_SECONDS)
     * @throws SQLException
     */
    private void waitServerCommit(long timeoutMillis) throws SQLException {
        try {
            Stopwatch stopwatch = Stopwatch.createStarted();
            long beginNanos = System.nanoTime();
            long timeoutNanos = OB_DIRECT_PATH_COMMIT_TIMEOUT_NANOS;
            if (timeoutMillis > 0) {
                timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            }
            LOG.info("Wait server committing. It may cost some time.");
            ObTableLoadClientStatus status;

            round_robin:
            while (true) {

                status = this.load.getStatus();

                switch (status) {

                    case ABORT: {
                        // Load failed, Server has already abort.
                        throw new IllegalStateException("Load aborted with error code: " + this.load.getErrorCode());
                    }

                    case ERROR:
                    case COMMITTING: {
                        break;
                    }

                    case COMMIT:
                        break round_robin;

                    default: {
                        // Unexpected state.
                        this.load.abort();
                        break round_robin;
                    }
                } // end of switch

                // Load failed. Server will abort and state will change to ABORT
                if (beginNanos > 0 && ((System.nanoTime() - beginNanos) >= timeoutNanos)) {
                    throw new IllegalStateException("Wait server abort timeout. (>" + timeoutNanos + "ms)");
                }

                try {
                    // Avoid busy wait....
                    Thread.sleep(BUSY_WAIT_TIME_MILLIS);
                    // sleep time is not include as timeout
                    beginNanos += TimeUnit.MILLISECONDS.toNanos(BUSY_WAIT_TIME_MILLIS);
                } catch (InterruptedException e) {
                    this.load.abort();
                    break; // listen the interrupted
                }
            }

            LOG.info("Wait server commit finished. State: {}. Total elapsed: {}", this.load.getStatus(), stopwatch.stop());
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
    }


    /**
     * Indicates the state of {@link DirectPathConnection }
     */
    enum State {

        /**
         * Begin transaction
         */
        BEGIN,
        /**
         * Transaction is finished, ready to close.
         */
        FINISHED,

        /**
         * Transaction is closed.
         */
        CLOSED;

    }

    /**
     * This builder used to build a new {@link DirectPathConnection }
     */
    public static class Builder {

        private String host;
        private int port;

        private String user;
        private String tenant;
        private String password;

        private String schema;
        private String table;

        /**
         * Client job count.
         */
        private int blocks = OB_DIRECT_PATH_DEFAULT_BLOCKS;

        /**
         * Server threads used to sort.
         */
        private int parallel;

        private long maxErrorCount;

        private ObLoadDupActionType duplicateKeyAction;

        // Used for load data
        private long serverTimeout;

        private Properties rpcProperties;

        public DirectPathConnection.Builder host(String host) {
            this.host = host;
            return this;
        }

        public DirectPathConnection.Builder port(int port) {
            this.port = port;
            return this;
        }

        public DirectPathConnection.Builder user(String user) {
            this.user = user;
            return this;
        }

        public DirectPathConnection.Builder tenant(String tenant) {
            this.tenant = tenant;
            return this;
        }

        public DirectPathConnection.Builder password(String password) {
            this.password = password;
            return this;
        }

        public DirectPathConnection.Builder schema(String schema) {
            this.schema = schema;
            return this;
        }

        public DirectPathConnection.Builder table(String table) {
            this.table = table;
            return this;
        }

        public DirectPathConnection.Builder blocks(int blocks) {
            this.blocks = blocks;
            return this;
        }

        public DirectPathConnection.Builder parallel(int parallel) {
            this.parallel = parallel;
            return this;
        }

        public DirectPathConnection.Builder maxErrorCount(long maxErrorCount) {
            this.maxErrorCount = maxErrorCount;
            return this;
        }

        public DirectPathConnection.Builder duplicateKeyAction(ObLoadDupActionType duplicateKeyAction) {
            this.duplicateKeyAction = duplicateKeyAction;
            return this;
        }

        public DirectPathConnection.Builder serverTimeout(long serverTimeout) {
            this.serverTimeout = serverTimeout;
            return this;
        }

        public DirectPathConnection.Builder rpcProperties(Properties rpcProperties) {
            this.rpcProperties = rpcProperties;
            return this;
        }

        /**
         * Build a new {@link DirectPathConnection }
         *
         * @return DirectPathConnection
         */
        public DirectPathConnection build() throws Exception {
            return createConnection(host, port, user, tenant, password, schema, table, //
                    blocks, parallel, maxErrorCount, duplicateKeyAction, serverTimeout, rpcProperties).begin();
        }

        /**
         * Create a new {@link DirectPathConnection }
         *
         * @param host
         * @param port
         * @param user
         * @param tenant
         * @param password
         * @param schema
         * @param table
         * @param parallel
         * @param maxErrorCount
         * @param action
         * @param serverTimeout
         * @return DirectPathConnection
         * @throws Exception
         */
        DirectPathConnection createConnection(String host, int port, String user, String tenant, String password, String schema, String table, //
                                              int blocks, int parallel, long maxErrorCount, ObLoadDupActionType action, long serverTimeout, Properties rpcProperties) throws Exception {

            checkArgument(StringUtils.isNotBlank(host), "Host is null.(host=%s)", host);
            checkArgument((port > 0 && port < 65535), "Port is invalid.(port=%s)", port);
            checkArgument(StringUtils.isNotBlank(user), "User Name is null.(user=%s)", user);
            checkArgument(StringUtils.isNotBlank(tenant), "Tenant Name is null.(tenant=%s)", tenant);
            checkArgument(StringUtils.isNotBlank(schema), "Schema Name is null.(schema=%s)", schema);
            checkArgument(StringUtils.isNotBlank(table), "Table Name is null.(table=%s)", table);

            checkArgument(blocks > 0, "Client Blocks is invalid.(blocks=%s)", blocks);
            checkArgument(parallel > 0, "Server Parallel is invalid.(parallel=%s)", parallel);
            checkArgument(maxErrorCount > -1, "MaxErrorCount is invalid.(maxErrorCount=%s)", maxErrorCount);
            checkArgument(action != null, "ObLoadDupActionType is null.(obLoadDupActionType=%s)", action);
            checkArgument(serverTimeout > 0, "Server timeout is invalid.(timeout=%s)", serverTimeout);

            ObDirectLoadParameter parameter = new ObDirectLoadParameter();
            parameter.setParallel(parallel);
            parameter.setMaxErrorRowCount(maxErrorCount);
            parameter.setDupAction(action);
            parameter.setTimeout(serverTimeout);
            parameter.setHeartBeatTimeout(OB_DIRECT_PATH_HEART_BEAT_TIMEOUT);

            if (rpcProperties == null) {
                rpcProperties = new Properties();
            }
            Properties props = initRpcServerProperties(parallel, rpcProperties);
            ObTable obTable = new ObTable.Builder(host, port).setLoginInfo(tenant, user, password, schema).setProperties(props).build();
            return new DirectPathConnection(new ObTableDirectLoad(obTable, table, parameter, /* forceCreate */ true), blocks);
        }

        /**
         * Hard code now... Refactor it later.
         *
         * @param parallel
         * @return Properties
         */
        private Properties initRpcServerProperties(int parallel, Properties rpcProperties) {
            final Properties props = new Properties();
            props.putAll(rpcProperties);
            props.setProperty(Property.RPC_CONNECT_TIMEOUT.getKey(), rpcProperties.getProperty(Constants.RPC_CONNECT_TIMEOUT, OB_DIRECT_PATH_RPC_CONNECT_TIMEOUT));
            props.setProperty(Property.RPC_EXECUTE_TIMEOUT.getKey(), rpcProperties.getProperty(Constants.RPC_EXECUTE_TIMEOUT, OB_DIRECT_PATH_RPC_EXECUTE_TIMEOUT));
            props.setProperty(Property.RPC_CONNECT_TRY_TIMES.getKey(), rpcProperties.getProperty(Constants.RPC_CONNECT_TRY_TIMES, OB_DIRECT_PATH_RPC_CONNECT_TRY_TIMES));
            props.setProperty(Property.SERVER_CONNECTION_POOL_SIZE.getKey(), rpcProperties.getProperty(Constants.SERVER_CONNECTION_POOL_SIZE, String.valueOf(parallel)));
            return props;
        }
    }
}
