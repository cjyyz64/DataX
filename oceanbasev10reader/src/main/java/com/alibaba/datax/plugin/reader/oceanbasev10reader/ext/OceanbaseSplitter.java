package com.alibaba.datax.plugin.reader.oceanbasev10reader.ext;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;
import static org.apache.commons.lang3.StringUtils.EMPTY;

import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.SplitedSlice;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.Config;

import com.alibaba.datax.plugin.reader.oceanbasev10reader.util.ExecutorTemplate;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.util.ObReaderUtils;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.util.PartInfo;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.util.PartType;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.stream.Collectors;

public abstract class OceanbaseSplitter {
    private static final Logger LOG = LoggerFactory.getLogger(OceanbaseSplitter.class);

    private static final long DEFAULT_SPLIT_SIZE = 1000000;

    protected String where;
    protected String compatibleMode;
    protected boolean forcePrecision;
    protected boolean enableNanos;
    protected boolean enableHighPrecision;
    protected boolean convertToNullIfYearValueIsNull;
    protected String mandatoryEncoding;
    protected long splitSize;
    protected int fetchSize;

    public OceanbaseSplitter(Configuration conf) {
        this.where = conf.getString(Key.WHERE, EMPTY);
        this.forcePrecision = conf.getBool("forcePrecision", true);
        this.enableNanos = conf.getBool("enableNanos", true);
        this.enableHighPrecision = conf.getBool("enableHighPrecision", false);
        this.convertToNullIfYearValueIsNull = conf.getBool(Config.CONVERT_TO_NULL_IF_YEAR_VALUE_IS_NULL, true);
        this.mandatoryEncoding = conf.getString(Key.MANDATORY_ENCODING, "");
        this.splitSize = conf.getLong(Config.SPLIT_SIZE, DEFAULT_SPLIT_SIZE);
        this.fetchSize = conf.getInt(Constant.FETCH_SIZE, Integer.MIN_VALUE);
    }

    /**
     * split partitioned table by index(including pk or uk or any other index) in per partition
     * split non-partitioned table by index(including pk or uk or any other index) in entair table
     *
     * @param configuration
     * @param adviceNum
     * @return List<Configuration>
     */
    public List<Configuration> doSplit(Configuration configuration, int adviceNum) {
        List<Object> conns = configuration.getList(Constant.CONN_MARK, Object.class);
        ExecutorTemplate<List<Configuration>> queryTemplate = new ExecutorTemplate<>("split-table-task-", adviceNum);
        String userName = configuration.getString(Key.USERNAME);
        String password = configuration.getString(Key.PASSWORD);
        for (Object connInfo : conns) {
            Configuration curDbConf = Configuration.from(connInfo.toString());
            String jdbcUrl = curDbConf.getString(Key.JDBC_URL);
            String dbName = ObReaderUtils.getDbNameFromJdbcUrl(jdbcUrl);
            List<String> tableList = curDbConf.getList(Key.TABLE, String.class);
            int eachTableShouldSplittedNumber = adviceNum;
            if (tableList.size() == 1) {
                Integer splitFactor = configuration.getInt(Key.SPLIT_FACTOR, Constant.SPLIT_FACTOR);
                eachTableShouldSplittedNumber = eachTableShouldSplittedNumber * splitFactor;
            }
            Configuration confWithoutConn = configuration.clone();
            confWithoutConn.remove(Constant.CONN_MARK);
            confWithoutConn.set(Key.JDBC_URL, jdbcUrl);
            for (String table : tableList) {
                Configuration tableConf = confWithoutConn.clone();
                tableConf.set(Key.TABLE, table);
                PartInfo partInfo = new PartInfo(PartType.NONPARTITION);
                IndexSchema splitIndexSchema = IndexSchema.EMPTY_INDEX_SCHEMA;
                try (Connection conn = ObReaderUtils.getConnection(jdbcUrl, userName, password, tableConf)) {
                    ObReaderUtils.getObVersion(conn);
                    partInfo = queryPartitions(table, dbName, conn);
                    splitIndexSchema = findBestSplitIndex(conn, dbName, table);
                } catch (Exception e) {
                    LOG.warn("failed to find the split index of table:[{}] failed. reason: {}", table, e.getMessage(), e);
                }
                final IndexSchema finalSplitIndexSchema = splitIndexSchema;
                if (partInfo.isPartitionTable()) {
                    for (String part : partInfo.getPartList()) {
                        while (true) {
                            int finalEachTableShouldSplittedNumber = eachTableShouldSplittedNumber;
                            try {
                                queryTemplate.submit(() -> {
                                    try {
                                        return generateConfiguration(part, table, finalSplitIndexSchema, tableConf, splitSize, finalEachTableShouldSplittedNumber);
                                    } catch (Throwable e) {
                                        LOG.warn("generate split slice configuration for partitioned table: [{}-{}] failed. reason:{}", table, part, e.getMessage(), e);
                                    }
                                    Configuration sliceConf = tableConf.clone();
                                    sliceConf.set(Key.PARTITION_NAME, part);
                                    return Lists.newArrayList(sliceConf);
                                });
                                break;
                            } catch (RejectedExecutionException e) {
                                LOG.debug("submit split task of partitioned table [{}-{}] failed, retry to submit", table, part);
                            }
                        }
                    }
                } else {
                    while (true) {
                        try {
                            int finalEachTableShouldSplittedNumber = eachTableShouldSplittedNumber;
                            queryTemplate.submit(() -> generateConfiguration("", table, finalSplitIndexSchema, tableConf, splitSize, finalEachTableShouldSplittedNumber));
                            break;
                        } catch (RejectedExecutionException e) {
                            LOG.debug("submit split task of non-partitioned table [{}] failed, retry to submit", table);
                        }
                    }
                }
            }
        }
        return queryTemplate.waitForResult().stream().flatMap(Collection::stream).collect(Collectors.toList());
    }

    /**
     * If user did not set where clause, the priority order is primary, unique key(including is null split), the shortest index(including is null split);
     * Otherwise, choose the shortest index containing the most columns in the where clause, which may generate is null split.
     *
     * @param conn
     * @param dbName
     * @param table
     * @return IndexSchema
     */
    protected IndexSchema findBestSplitIndex(Connection conn, String dbName, String table) {
        Set<String> columnsInConditions = ObReaderUtils.fetchColumnsInCondition(where, conn, table);
        Map<String, IndexSchema> allIndex = ObReaderUtils.getAllIndex(conn, dbName, table, compatibleMode);
        IndexSchema indexSchema = ObReaderUtils.getIndexName(allIndex, columnsInConditions, false);
        if (indexSchema != null) {
            LOG.info("choose index:[{}] to split table:[{}]", indexSchema, wrapName(dbName) + "." + wrapName(table));
        } else {
            LOG.info("no index was chosen to split table:[{}]", wrapName(dbName) + "." + wrapName(table));
            indexSchema = IndexSchema.EMPTY_INDEX_SCHEMA;
        }
        return indexSchema;
    }

    /**
     * @param rs
     * @param columns
     * @return List<String>
     * @throws SQLException
     */
    protected List<String> fetchValuesFromRs(ResultSet rs, List<String> columns) throws Exception {
        List<String> values = new ArrayList<>(columns.size());
        ResultSetMetaData metaData = rs.getMetaData();
        for (int i = 1; i < columns.size() + 1; i++) {
            switch (metaData.getColumnType(i)) {
                case Types.CHAR:
                case Types.NCHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR:
                    String rawData;
                    if (StringUtils.isBlank(mandatoryEncoding)) {
                        rawData = rs.getString(i);
                    } else {
                        rawData = new String((rs.getBytes(i) == null ? EMPTY_BYTE_ARRAY : rs.getBytes(i)), mandatoryEncoding);
                    }
                    values.add(rawData);
                    break;

                case Types.SMALLINT:
                case Types.TINYINT:
                case Types.INTEGER:
                case Types.BIGINT:
                case Types.CLOB:
                case Types.NCLOB:
                case Types.BOOLEAN:
                case Types.BIT:
                case Types.FLOAT:
                case Types.REAL:
                case Types.DOUBLE:
                case Types.TIME:
                    values.add(rs.getString(i));
                    break;

                case Types.NUMERIC:
                case Types.DECIMAL:
                    if (isOracleMode() && this.forcePrecision) {
                        int precision = metaData.getScale(i);
                        String valueFormat = String.format("%%.%df", precision);
                        values.add(String.format(valueFormat, Double.valueOf(rs.getString(i))));
                    } else {
                        values.add(rs.getString(i));
                    }
                    break;

                // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
                case Types.DATE:
                    if ("year".equalsIgnoreCase(metaData.getColumnTypeName(i))) {
                        int value = rs.getInt(i);
                        if (!this.convertToNullIfYearValueIsNull) {
                            values.add(new LongColumn(value).asString());
                            break;
                        }
                        LongColumn column = null;
                        if (value == 0 && rs.getObject(i) == null) {
                            column = new LongColumn();
                        } else {
                            column = new LongColumn(value);
                        }
                        values.add(column.asString());
                    } else {
                        values.add((new DateColumn(rs.getDate(i))).asString());
                    }
                    break;

                case Types.TIMESTAMP:
                case Types.TIME_WITH_TIMEZONE:
                case Types.TIMESTAMP_WITH_TIMEZONE:
                case -101:
                case -102:
                    DateColumn dateColumn;
                    dateColumn = new DateColumn(rs.getTimestamp(i));
                    values.add(dateColumn.asString());
                    break;

                // 实际上只有oracle模式下的RAW类型可以作为索引，用16进制编码的String表示即可
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.BLOB:
                case Types.LONGVARBINARY:
                    values.add(new BytesColumn(rs.getBytes(i)).asString());
                    break;

                case Types.NULL:
                    String stringData = null;
                    if (rs.getObject(i) != null) {
                        stringData = rs.getObject(i).toString();
                    }
                    values.add(stringData);
                    break;

                default:
                    String msg =
                            "Error in the column configuration information in your configuration file. DataX does not allow databases to read this field type. The field name: [%s], field type: "
                                    + "[%s], and field Java type: [%s]. Please try converting the field into a type supported by DataX using database functions, or you may choose to drop this field"
                                    + " from synchronization.";
                    throw DataXException.asDataXException(
                            DBUtilErrorCode.UNSUPPORTED_TYPE, String.format(msg, metaData.getColumnName(i), metaData.getColumnType(i), metaData.getColumnClassName(i)));
            }
        }
        return values;
    }

    /**
     * Assemble the slice range using vector form
     *
     * @param beginValues
     * @param endValues
     * @param indexColumns
     * @return String
     */
    protected String assembleRange(List<String> beginValues, List<String> endValues, List<String> indexColumns) {
        boolean noBegin = beginValues == null || beginValues.isEmpty();
        boolean noEnd = endValues == null || endValues.isEmpty();
        if (noBegin && noEnd) {
            return EMPTY;
        }
        StringBuilder range = new StringBuilder();
        String indexStr = indexColumns.stream().map(this::wrapName).collect(Collectors.joining(","));
        range.append("(").append(indexStr).append(")");
        if (noBegin) {
            range.append("<=(").append(endValues.stream().map(e -> "'" + e + "'").collect(Collectors.joining(","))).append(")");
        } else if (noEnd) {
            range.append(">(").append(beginValues.stream().map(e -> "'" + e + "'").collect(Collectors.joining(","))).append(")");
        } else {
            range.append(">(").append(beginValues.stream().map(e -> "'" + e + "'").collect(Collectors.joining(","))).append(")");
            range.append(" and ");
            range.append("(").append(indexStr).append(")").append("<=(").append(endValues.stream().map(e -> "'" + e + "'").collect(Collectors.joining(","))).append(")");
        }
        String isNotNull = indexColumns.stream().map(e -> e + " is not null").collect(Collectors.joining(" and "));
        range.append(" and ").append(isNotNull);
        return range.toString();
    }

    /**
     * @param part
     * @param table
     * @param splitIndexSchema
     * @param tableConf
     * @param splitSize
     * @param adviceNum
     * @return List<Configuration>
     */
    protected List<Configuration> generateConfiguration(String part, String table, IndexSchema splitIndexSchema, Configuration tableConf, long splitSize, int adviceNum) {
        List<SplitedSlice> sliceList = null;
        try (Connection conn = ObReaderUtils.getConnection(tableConf)) {
            int queryTimeoutSeconds = 60 * 60 * 48;
            String setQueryTimeout = "set ob_query_timeout=" + (queryTimeoutSeconds * 1000 * 1000L);
            String setTrxTimeout = "set ob_trx_timeout=" + ((queryTimeoutSeconds + 5) * 1000 * 1000L);
            List<String> sessionConf = tableConf.getList(Key.SESSION, new ArrayList<>(), String.class);
            List<String> newSessionConf = Lists.newArrayList(setQueryTimeout, setTrxTimeout);
            newSessionConf.addAll(sessionConf);
            tableConf.set(Key.SESSION, newSessionConf);
            DBUtil.dealWithSessionConfig(conn, tableConf, ObReaderUtils.DATABASE_TYPE, EMPTY);
            sliceList = splitSlices(part, table, conn, splitIndexSchema.getColumns(), splitSize, adviceNum);
        } catch (Exception e) {
            LOG.warn("generate split slice configuration of table:[{}-{}] failed. reason:{}", table, part, e.getMessage(), e);
            Configuration sliceConf = tableConf.clone();
            sliceConf.set(Key.PARTITION_NAME, part);
            return Lists.newArrayList(sliceConf);
        }

        if (sliceList.isEmpty()) {
            Configuration sliceConf = tableConf.clone();
            sliceConf.set(Key.PARTITION_NAME, part);
            return Lists.newArrayList(sliceConf);
        } else {
            List<Configuration> configurations = new ArrayList<>();
            boolean hasWhere = StringUtils.isNotEmpty(where);
            for (SplitedSlice slice : sliceList) {
                String newWhereClause = hasWhere ? where + " and " : EMPTY;
                newWhereClause += " (" + slice.getRange() + ")";
                Configuration sliceConf = tableConf.clone();
                sliceConf.set(Key.WHERE, newWhereClause);
                sliceConf.set(Key.PARTITION_NAME, part);
                configurations.add(sliceConf);
            }
            // add whee clause indicating index value is null
            String isNull = splitIndexSchema.getColumns().stream().map(e -> e + " is null").collect(Collectors.joining(" or "));
            String newWhereClause = hasWhere ? where + " and " : EMPTY;
            newWhereClause += " (" + isNull + ")";
            Configuration sliceConf = tableConf.clone();
            sliceConf.set(Key.WHERE, newWhereClause);
            sliceConf.set(Key.PARTITION_NAME, part);
            configurations.add(sliceConf);
            return configurations;
        }
    }

    /**
     * @param partName
     * @param table
     * @param conn
     * @param indexColumns
     * @param splitSize
     * @param adviceNum
     * @return List<SplitedSlice>
     * @throws SQLException
     */
    protected List<SplitedSlice> splitSlices(String partName, String table, Connection conn, List<String> indexColumns, long splitSize, int adviceNum) throws Exception {
        List<SplitedSlice> sliceList = new ArrayList<>();
        if (indexColumns == null || indexColumns.isEmpty() || adviceNum <= 2) {
            return sliceList;
        }

        long realSplitSize = splitSize < 0 ? DEFAULT_SPLIT_SIZE : splitSize;
        String firstQuerySql = getQuerySql(indexColumns, table, partName, realSplitSize, true);
        List<String> preValues;
        try (PreparedStatement ps = conn.prepareStatement(firstQuerySql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            ps.setFetchSize(fetchSize);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    preValues = fetchValuesFromRs(rs, indexColumns);
                    String range = assembleRange(null, preValues, indexColumns);
                    SplitedSlice slice = new SplitedSlice(null, null, range);
                    sliceList.add(slice);
                } else {
                    return sliceList;
                }
            }
        }

        boolean hasMoreSlice = true;
        String appendSql = getQuerySql(indexColumns, table, partName, realSplitSize, false);
        try (PreparedStatement ps = conn.prepareStatement(appendSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            ps.setFetchSize(fetchSize);
            while (hasMoreSlice && sliceList.size() < adviceNum - 1) {
                ps.clearParameters();
                for (int i = 0; i < preValues.size(); i++) {
                    ps.setObject(i + 1, preValues.get(i));
                }
                try (ResultSet rs = ps.executeQuery()) {
                    List<String> endValues = null;
                    if (rs.next()) {
                        endValues = fetchValuesFromRs(rs, indexColumns);
                    } else {
                        hasMoreSlice = false;
                    }
                    String range = assembleRange(preValues, endValues, indexColumns);
                    sliceList.add(new SplitedSlice(null, null, range));
                    preValues.clear();
                    if (endValues != null) {
                        preValues.addAll(endValues);
                    }
                }
            }
            if (hasMoreSlice) {
                String range = assembleRange(preValues, null, indexColumns);
                sliceList.add(new SplitedSlice(null, null, range));
                preValues.clear();
            }
        }
        return sliceList;
    }

    protected abstract boolean isOracleMode();

    /**
     * mysql mode: quote char is `
     * oracle mode: quote char is "
     *
     * @param column
     * @return String
     */
    protected abstract String wrapName(String column);

    /**
     * @param table
     * @param dbName
     * @param conn
     * @return PartInfo
     */
    protected abstract PartInfo queryPartitions(String table, String dbName, Connection conn);

    /**
     * @param splitIndex
     * @param table
     * @param partName
     * @param rangeSize
     * @param first
     * @return String
     */
    protected abstract String getQuerySql(List<String> splitIndex, String table, String partName, long rangeSize, boolean first);
}
