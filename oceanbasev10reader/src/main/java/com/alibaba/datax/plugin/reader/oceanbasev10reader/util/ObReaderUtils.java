package com.alibaba.datax.plugin.reader.oceanbasev10reader.util;

import static org.apache.commons.lang3.StringUtils.EMPTY;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.DataXCaseEnvUtil;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.reader.util.ObVersion;
import com.alibaba.datax.plugin.rdbms.reader.util.SingleTableSplitUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.ext.Constant;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.ext.IndexSchema;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author johnrobbet
 */
public class ObReaderUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ObReaderUtils.class);
    private static final String MYSQL_KEYWORDS =
            "ACCESSIBLE,ACCOUNT,ACTION,ADD,AFTER,AGAINST,AGGREGATE,ALGORITHM,ALL,ALTER,ALWAYS,ANALYSE,AND,ANY,AS,ASC,ASCII,ASENSITIVE,AT,AUTO_INCREMENT,AUTOEXTEND_SIZE,AVG,AVG_ROW_LENGTH,BACKUP,"
                    + "BEFORE,BEGIN,BETWEEN,BIGINT,BINARY,BINLOG,BIT,BLOB,BLOCK,BOOL,BOOLEAN,BOTH,BTREE,BY,BYTE,CACHE,CALL,CASCADE,CASCADED,CASE,CATALOG_NAME,CHAIN,CHANGE,CHANGED,CHANNEL,CHAR,"
                    + "CHARACTER,CHARSET,CHECK,CHECKSUM,CIPHER,CLASS_ORIGIN,CLIENT,CLOSE,COALESCE,CODE,COLLATE,COLLATION,COLUMN,COLUMN_FORMAT,COLUMN_NAME,COLUMNS,COMMENT,COMMIT,COMMITTED,COMPACT,"
                    + "COMPLETION,COMPRESSED,COMPRESSION,CONCURRENT,CONDITION,CONNECTION,CONSISTENT,CONSTRAINT,CONSTRAINT_CATALOG,CONSTRAINT_NAME,CONSTRAINT_SCHEMA,CONTAINS,CONTEXT,CONTINUE,"
                    + "CONVERT,CPU,CREATE,CROSS,CUBE,CURRENT,CURRENT_DATE,CURRENT_TIME,CURRENT_TIMESTAMP,CURRENT_USER,CURSOR,CURSOR_NAME,DATA,DATABASE,DATABASES,DATAFILE,DATE,DATETIME,DAY,DAY_HOUR,"
                    + "DAY_MICROSECOND,DAY_MINUTE,DAY_SECOND,DEALLOCATE,DEC,DECIMAL,DECLARE,DEFAULT,DEFAULT_AUTH,DEFINER,DELAY_KEY_WRITE,DELAYED,DELETE,DES_KEY_FILE,DESC,DESCRIBE,DETERMINISTIC,"
                    + "DIAGNOSTICS,DIRECTORY,DISABLE,DISCARD,DISK,DISTINCT,DISTINCTROW,DIV,DO,DOUBLE,DROP,DUAL,DUMPFILE,DUPLICATE,DYNAMIC,EACH,ELSE,ELSEIF,ENABLE,ENCLOSED,ENCRYPTION,END,ENDS,"
                    + "ENGINE,ENGINES,ENUM,ERROR,ERRORS,ESCAPE,ESCAPED,EVENT,EVENTS,EVERY,EXCHANGE,EXECUTE,EXISTS,EXIT,EXPANSION,EXPIRE,EXPLAIN,EXPORT,EXTENDED,EXTENT_SIZE,FAST,FAULTS,FETCH,FIELDS,"
                    + "FILE,FILE_BLOCK_SIZE,FILTER,FIRST,FIXED,FLOAT,FLOAT4,FLOAT8,FLUSH,FOLLOWS,FOR,FORCE,FOREIGN,FORMAT,FOUND,FROM,FULL,FULLTEXT,FUNCTION,GENERAL,GENERATED,GEOMETRY,"
                    + "GEOMETRYCOLLECTION,GET,GET_FORMAT,GLOBAL,GRANT,GRANTS,GROUP,GROUP_REPLICATION,HANDLER,HASH,HAVING,HELP,HIGH_PRIORITY,HOST,HOSTS,HOUR,HOUR_MICROSECOND,HOUR_MINUTE,HOUR_SECOND,"
                    + "IDENTIFIED,IF,IGNORE,IGNORE_SERVER_IDS,IMPORT,IN,INDEX,INDEXES,INFILE,INITIAL_SIZE,INNER,INOUT,INSENSITIVE,INSERT,INSERT_METHOD,INSTALL,INSTANCE,INT,INT1,INT2,INT3,INT4,INT8,"
                    + "INTEGER,INTERVAL,INTO,INVOKER,IO,IO_AFTER_GTIDS,IO_BEFORE_GTIDS,IO_THREAD,IPC,IS,ISOLATION,ISSUER,ITERATE,JOIN,JSON,KEY,KEY_BLOCK_SIZE,KEYS,KILL,LANGUAGE,LAST,LEADING,LEAVE,"
                    + "LEAVES,LEFT,LESS,LEVEL,LIKE,LIMIT,LINEAR,LINES,LINESTRING,LIST,LOAD,LOCAL,LOCALTIME,LOCALTIMESTAMP,LOCK,LOCKS,LOGFILE,LOGS,LONG,LONGBLOB,LONGTEXT,LOOP,LOW_PRIORITY,MASTER,"
                    + "MASTER_AUTO_POSITION,MASTER_BIND,MASTER_CONNECT_RETRY,MASTER_DELAY,MASTER_HEARTBEAT_PERIOD,MASTER_HOST,MASTER_LOG_FILE,MASTER_LOG_POS,MASTER_PASSWORD,MASTER_PORT,"
                    + "MASTER_RETRY_COUNT,MASTER_SERVER_ID,MASTER_SSL,MASTER_SSL_CA,MASTER_SSL_CAPATH,MASTER_SSL_CERT,MASTER_SSL_CIPHER,MASTER_SSL_CRL,MASTER_SSL_CRLPATH,MASTER_SSL_KEY,"
                    + "MASTER_SSL_VERIFY_SERVER_CERT,MASTER_TLS_VERSION,MASTER_USER,MATCH,MAX_CONNECTIONS_PER_HOUR,MAX_QUERIES_PER_HOUR,MAX_ROWS,MAX_SIZE,MAX_STATEMENT_TIME,MAX_UPDATES_PER_HOUR,"
                    + "MAX_USER_CONNECTIONS,MAXVALUE,MEDIUM,MEDIUMBLOB,MEDIUMINT,MEDIUMTEXT,MEMORY,MERGE,MESSAGE_TEXT,MICROSECOND,MIDDLEINT,MIGRATE,MIN_ROWS,MINUTE,MINUTE_MICROSECOND,MINUTE_SECOND,"
                    + "MOD,MODE,MODIFIES,MODIFY,MONTH,MULTILINESTRING,MULTIPOINT,MULTIPOLYGON,MUTEX,MYSQL_ERRNO,NAME,NAMES,NATIONAL,NATURAL,NCHAR,NDB,NDBCLUSTER,NEVER,NEW,NEXT,NO,NO_WAIT,"
                    + "NO_WRITE_TO_BINLOG,NODEGROUP,NONBLOCKING,NONE,NOT,NULL,NUMBER,NUMERIC,NVARCHAR,OFFSET,OLD_PASSWORD,ON,ONE,ONLY,OPEN,OPTIMIZE,OPTIMIZER_COSTS,OPTION,OPTIONALLY,OPTIONS,OR,"
                    + "ORDER,OUT,OUTER,OUTFILE,OWNER,PACK_KEYS,PAGE,PARSE_GCOL_EXPR,PARSER,PARTIAL,PARTITION,PARTITIONING,PARTITIONS,PASSWORD,PHASE,PLUGIN,PLUGIN_DIR,PLUGINS,POINT,POLYGON,PORT,"
                    + "PRECEDES,PRECISION,PREPARE,PRESERVE,PREV,PRIMARY,PRIVILEGES,PROCEDURE,PROCESSLIST,PROFILE,PROFILES,PROXY,PURGE,QUARTER,QUERY,QUICK,RANGE,READ,READ_ONLY,READ_WRITE,READS,REAL,"
                    + "REBUILD,RECOVER,REDO_BUFFER_SIZE,REDOFILE,REDUNDANT,REFERENCES,REGEXP,RELAY,RELAY_LOG_FILE,RELAY_LOG_POS,RELAY_THREAD,RELAYLOG,RELEASE,RELOAD,REMOVE,RENAME,REORGANIZE,REPAIR,"
                    + "REPEAT,REPEATABLE,REPLACE,REPLICATE_DO_DB,REPLICATE_DO_TABLE,REPLICATE_IGNORE_DB,REPLICATE_IGNORE_TABLE,REPLICATE_REWRITE_DB,REPLICATE_WILD_DO_TABLE,"
                    + "REPLICATE_WILD_IGNORE_TABLE,REPLICATION,REQUIRE,RESET,RESIGNAL,RESTORE,RESTRICT,RESUME,RETURN,RETURNED_SQLSTATE,RETURNS,REVERSE,REVOKE,RIGHT,RLIKE,ROLLBACK,ROLLUP,ROTATE,"
                    + "ROUTINE,ROW,ROW_COUNT,ROW_FORMAT,ROWS,RTREE,SAVEPOINT,SCHEDULE,SCHEMA,SCHEMA_NAME,SCHEMAS,SECOND,SECOND_MICROSECOND,SECURITY,SELECT,SENSITIVE,SEPARATOR,SERIAL,SERIALIZABLE,"
                    + "SERVER,SESSION,SET,SHARE,SHOW,SHUTDOWN,SIGNAL,SIGNED,SIMPLE,SLAVE,SLOW,SMALLINT,SNAPSHOT,SOCKET,SOME,SONAME,SOUNDS,SOURCE,SPATIAL,SPECIFIC,SQL,SQL_AFTER_GTIDS,"
                    + "SQL_AFTER_MTS_GAPS,SQL_BEFORE_GTIDS,SQL_BIG_RESULT,SQL_BUFFER_RESULT,SQL_CACHE,SQL_CALC_FOUND_ROWS,SQL_NO_CACHE,SQL_SMALL_RESULT,SQL_THREAD,SQL_TSI_DAY,SQL_TSI_HOUR,"
                    + "SQL_TSI_MINUTE,SQL_TSI_MONTH,SQL_TSI_QUARTER,SQL_TSI_SECOND,SQL_TSI_WEEK,SQL_TSI_YEAR,SQLEXCEPTION,SQLSTATE,SQLWARNING,SSL,STACKED,START,STARTING,STARTS,STATS_AUTO_RECALC,"
                    + "STATS_PERSISTENT,STATS_SAMPLE_PAGES,STATUS,STOP,STORAGE,STORED,STRAIGHT_JOIN,STRING,SUBCLASS_ORIGIN,SUBJECT,SUBPARTITION,SUBPARTITIONS,SUPER,SUSPEND,SWAPS,SWITCHES,TABLE,"
                    + "TABLE_CHECKSUM,TABLE_NAME,TABLES,TABLESPACE,TEMPORARY,TEMPTABLE,TERMINATED,TEXT,THAN,THEN,TIME,TIMESTAMP,TIMESTAMPADD,TIMESTAMPDIFF,TINYBLOB,TINYINT,TINYTEXT,TO,TRAILING,"
                    + "TRANSACTION,TRIGGER,TRIGGERS,TRUNCATE,TYPE,TYPES,UNCOMMITTED,UNDEFINED,UNDO,UNDO_BUFFER_SIZE,UNDOFILE,UNICODE,UNINSTALL,UNION,UNIQUE,UNKNOWN,UNLOCK,UNSIGNED,UNTIL,UPDATE,"
                    + "UPGRADE,USAGE,USE,USE_FRM,USER,USER_RESOURCES,USING,UTC_DATE,UTC_TIME,UTC_TIMESTAMP,VALIDATION,VALUE,VALUES,VARBINARY,VARCHAR,VARCHARACTER,VARIABLES,VARYING,VIEW,VIRTUAL,"
                    + "WAIT,WARNINGS,WEEK,WEIGHT_STRING,WHEN,WHERE,WHILE,WITH,WITHOUT,WORK,WRAPPER,WRITE,X509,XA,XID,XML,XOR,YEAR,YEAR_MONTH,ZEROFILL,FALSE,TRUE";
    private static final String ORACLE_KEYWORDS =
            "ACCESS,ADD,ALL,ALTER,AND,ANY,ARRAYLEN,AS,ASC,AUDIT,BETWEEN,BY,CHAR,CHECK,CLUSTER,COLUMN,COMMENT,COMPRESS,CONNECT,CREATE,CURRENT,DATE,DECIMAL,DEFAULT,DELETE,DESC,DISTINCT,DROP,ELSE,"
                    + "EXCLUSIVE,EXISTS,FILE,FLOAT,FOR,FROM,GRANT,GROUP,HAVING,IDENTIFIED,IMMEDIATE,IN,INCREMENT,INDEX,INITIAL,INSERT,INTEGER,INTERSECT,INTO,IS,LEVEL,LIKE,LOCK,LONG,MAXEXTENTS,"
                    + "MINUS,MODE,MODIFY,NOAUDIT,NOCOMPRESS,NOT,NOTFOUND,NOWAIT,NUMBER,OF,OFFLINE,ON,ONLINE,OPTION,OR,ORDER,PCTFREE,PRIOR,PRIVILEGES,PUBLIC,RAW,RENAME,RESOURCE,REVOKE,ROW,ROWID,"
                    + "ROWLABEL,ROWNUM,ROWS,SELECT,SESSION,SET,SHARE,SIZE,SMALLINT,SQLBUF,START,SUCCESSFUL,SYNONYM,TABLE,THEN,TO,TRIGGER,UID,UNION,UNIQUE,UPDATE,USER,VALIDATE,VALUES,VARCHAR,"
                    + "VARCHAR2,VIEW,WHENEVER,WHERE,WITH,KEY,NAME,VALUE,TYPE";

    private static Set<String> databaseKeywords;
    final static public String OB_COMPATIBLE_MODE = "obCompatibilityMode";
    final static public String OB_COMPATIBLE_MODE_ORACLE = "ORACLE";
    final static public String OB_COMPATIBLE_MODE_MYSQL = "MYSQL";

    public static String compatibleMode = OB_COMPATIBLE_MODE_MYSQL;

    public static DataBaseType DATABASE_TYPE = DataBaseType.OceanBase;

    public static ObVersion obVersion;

    private static final String TABLE_SCHEMA_DELIMITER = ".";

    private static final Pattern JDBC_PATTERN = Pattern.compile("jdbc:(oceanbase|mysql)://([\\w\\.-]+:\\d+)/([\\w\\.-]+)");

    private static Set<String> keywordsFromString2HashSet(final String keywords) {
        return new HashSet<>(Arrays.asList(keywords.split(",")));
    }

    public static String escapeDatabaseKeyword(String keyword) {
        if (databaseKeywords == null) {
            if (isOracleMode(compatibleMode)) {
                databaseKeywords = keywordsFromString2HashSet(ORACLE_KEYWORDS);
            } else {
                databaseKeywords = keywordsFromString2HashSet(MYSQL_KEYWORDS);
            }
        }
        char escapeChar = isOracleMode(compatibleMode) ? '"' : '`';
        if (databaseKeywords.contains(keyword.toUpperCase())) {
            keyword = escapeChar + keyword + escapeChar;
        }
        return keyword;
    }

    public static void escapeDatabaseKeyword(List<String> ids) {
        if (ids != null && ids.size() > 0) {
            for (int i = 0; i < ids.size(); i++) {
                ids.set(i, escapeDatabaseKeyword(ids.get(i)));
            }
        }
    }

    public static Boolean isEscapeMode(String keyword) {
        if (isOracleMode(compatibleMode)) {
            return keyword.startsWith("\"") && keyword.endsWith("\"");
        } else {
            return keyword.startsWith("`") && keyword.endsWith("`");
        }
    }

    public static void initConn4Reader(Connection conn, long queryTimeoutSeconds) {
        String setQueryTimeout = "set ob_query_timeout=" + (queryTimeoutSeconds * 1000 * 1000L);
        String setTrxTimeout = "set ob_trx_timeout=" + ((queryTimeoutSeconds + 5) * 1000 * 1000L);
        Statement stmt = null;
        try {
            conn.setAutoCommit(true);
            stmt = conn.createStatement();
            stmt.execute(setQueryTimeout);
            stmt.execute(setTrxTimeout);
            LOG.warn("setAutoCommit=true;" + setQueryTimeout + ";" + setTrxTimeout + ";");
        } catch (Throwable e) {
            LOG.warn("initConn4Reader fail", e);
        } finally {
            DBUtil.closeDBResources(stmt, null);
        }
    }

    public static void sleep(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
        }
    }

    /**
     * Add pageQuery columns if they are not in selected projection column
     *
     * @param context
     */
    public static void matchPkIndexs(TaskContext context) {
        IndexSchema pageQuerySchema = context.getPageQuerySchema();
        if (pageQuerySchema == null || pageQuerySchema.getNotNullColumns() == null) {
            LOG.warn("table=" + context.getTable() + " has no primary key or not null unique index");
            return;
        }
        boolean isOracleMode = isOracleMode(context.getCompatibleMode());
        List<String> columns = context.getColumns();
        // 最后参与排序的索引列
        String[] pageQueryColumns = pageQuerySchema.getNotNullColumns().toArray(new String[0]);
        context.setPkColumns(pageQueryColumns);
        int[] pkIndexs = new int[pageQueryColumns.length];
        for (int i = 0, n = pageQueryColumns.length; i < n; i++) {
            String pkc = pageQueryColumns[i];
            String escapedPkc = wrapName(pkc, isOracleMode);
            int j = 0;
            for (int k = columns.size(); j < k; j++) {
                // 如果用户定义的 columns中 带有 ``,也不影响,
                // 最多只是在select里多加了几列PK column
                if (StringUtils.equalsIgnoreCase(pkc, columns.get(j)) || StringUtils.equalsIgnoreCase(escapedPkc, columns.get(j))) {
                    pkIndexs[i] = j;
                    pageQueryColumns[i] = columns.get(j);
                    break;
                }
            }
            // 到这里 说明主键列不在columns中,则主动追加到尾部
            if (j == columns.size()) {
                columns.add(pkc);
                pkIndexs[i] = columns.size() - 1;
            }
        }
        context.setPkIndexs(pkIndexs);
    }

    public static StringBuilder buildQueryTemplate(TaskContext context) {
        String indexName = context.getIndexName();
        String partInfo = StringUtils.isNotBlank(context.getPartitionName()) ? String.format(" partition(%s) ", context.getPartitionName()) : EMPTY;
        StringBuilder sb = new StringBuilder();
        sb.append("select ");
        boolean weakRead = context.getWeakRead();
        if (StringUtils.isNotEmpty(indexName)) {
            String weakReadHint = weakRead ? "+READ_CONSISTENCY(WEAK)," : "+";
            sb.append(" /*").append(weakReadHint).append("index(").append(context.getTable()).append(" ").append(indexName).append(")*/ ");
        } else if (weakRead) {
            sb.append(" /*+READ_CONSISTENCY(WEAK)*/ ");
        }
        sb.append(StringUtils.join(context.getColumns(), ','));
        sb.append(" from ").append(context.getTable()).append(partInfo);

        if (StringUtils.isNotEmpty(context.getWhere())) {
            sb.append(" where (").append(context.getWhere()).append(")");
        }
        return sb;
    }

    /**
     * 首次查的SQL
     *
     * @param context
     * @return
     */
    public static String buildFirstQuerySql(TaskContext context, StringBuilder queryTemplate) {
        String userSavePoint = context.getUserSavePoint();
        String sql = queryTemplate.toString();
        if (userSavePoint != null && userSavePoint.length() != 0) {
            userSavePoint = userSavePoint.replace("=", ">");
            sql += (StringUtils.isNotEmpty(context.getWhere()) ? " and " : " where ") + userSavePoint;
        }

        sql += " order by " + StringUtils.join(context.getPkColumns(), ',') + " asc";

        // Using sub-query to apply rownum < readBatchSize since where has higher priority than order by
        if (ObReaderUtils.isOracleMode(context.getCompatibleMode()) && context.getReadBatchSize() != -1) {
            sql = String.format("select * from (%s) where rownum <= %d", sql, context.getReadBatchSize());
        }
        return sql;
    }

    /**
     * 增量查的SQL
     *
     * @param context
     * @param queryTemplate
     * @return String
     */
    public static String buildAppendQuerySql(TaskContext context, StringBuilder queryTemplate) {
        String sql = queryTemplate.toString();
        String append = "(" + StringUtils.join(context.getPkColumns(), ',') + ") > (" + buildPlaceHolder(context.getPkColumns().length) + ")";
        if (StringUtils.isNotEmpty(context.getWhere())) {
            sql += " and ";
        } else {
            sql += " where ";
        }
        sql = String.format("%s %s order by %s asc", sql, append, StringUtils.join(context.getPkColumns(), ','));

        // Using sub-query to apply rownum < readBatchSize since where has higher priority than order by
        if (ObReaderUtils.isOracleMode(context.getCompatibleMode()) && context.getReadBatchSize() != -1) {
            sql = String.format("select * from (%s) where rownum <= %d", sql, context.getReadBatchSize());
        }
        return sql;
    }

    /**
     * check if the userSavePoint is valid
     *
     * @param context
     * @return true - valid, false - invalid
     */
    public static boolean isUserSavePointValid(TaskContext context) {
        String userSavePoint = context.getUserSavePoint();
        if (userSavePoint == null || userSavePoint.length() == 0) {
            LOG.info("user save point is empty!");
            return false;
        }

        LOG.info("validating user save point: " + userSavePoint);

        final String patternString = "(.+)=(.+)";
        Pattern parttern = Pattern.compile(patternString);
        Matcher matcher = parttern.matcher(userSavePoint);
        if (!matcher.find()) {
            LOG.error("user save point format is not correct: " + userSavePoint);
            return false;
        }

        List<String> columnsInUserSavePoint = getColumnsFromUserSavePoint(userSavePoint);
        List<String> valuesInUserSavePoint = getValuesFromUserSavePoint(userSavePoint);
        if (columnsInUserSavePoint.size() == 0 || valuesInUserSavePoint.size() == 0 ||
                columnsInUserSavePoint.size() != valuesInUserSavePoint.size()) {
            LOG.error("number of columns and values in user save point are different:" + userSavePoint);
            return false;
        }

        String where = context.getWhere();
        if (StringUtils.isNotEmpty(where)) {
            for (String column : columnsInUserSavePoint) {
                if (where.contains(column)) {
                    LOG.error("column " + column + " is conflict with where: " + where);
                    return false;
                }
            }
        }

        // Columns in userSavePoint must be the selected index.
        String[] pkColumns = context.getPkColumns();
        if (pkColumns.length != columnsInUserSavePoint.size()) {
            LOG.error("user save point is not on the selected index.");
            return false;
        }

        for (String column : columnsInUserSavePoint) {
            boolean found = false;
            for (String pkCol : pkColumns) {
                if (pkCol.equals(column)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                LOG.error("column " + column + " is not on the selected index.");
                return false;
            }
        }

        return true;
    }

    private static String removeBracket(String str) {
        final char leftBracket = '(';
        final char rightBracket = ')';
        if (str != null && str.contains(String.valueOf(leftBracket)) && str.contains(String.valueOf(rightBracket)) &&
                str.indexOf(leftBracket) < str.indexOf(rightBracket)) {
            return str.substring(str.indexOf(leftBracket) + 1, str.indexOf(rightBracket));
        }
        return str;
    }

    private static List<String> getColumnsFromUserSavePoint(String userSavePoint) {
        return Arrays.asList(removeBracket(userSavePoint.split("=")[0]).split(","));
    }

    private static List<String> getValuesFromUserSavePoint(String userSavePoint) {
        return Arrays.asList(removeBracket(userSavePoint.split("=")[1]).split(","));
    }

    /**
     * 先解析成where, 再判断是否存在索引
     *
     * @param conn
     * @param context
     * @return
     */
    public static void initPageQueryIndex(Connection conn, TaskContext context) {
        Set<String> columnsInCondition = fetchColumnsInCondition(context.getWhere(), conn, context.getTable());
        Map<String, IndexSchema> allIndex = ObReaderUtils.getAllIndex(conn, context.getDbName(), context.getTable(), context.getCompatibleMode());
        IndexSchema indexSchema = getIndexName(allIndex, columnsInCondition, true);
        LOG.info("possible index:" + indexSchema + ",where:" + context.getWhere());
        if (indexSchema != null) {
            LOG.info("choose index name:" + indexSchema.getIndexName() + ",columns:" + indexSchema.getNotNullColumns());
            context.setPageQuerySchema(indexSchema);
        }
    }

    public static Set<String> fetchColumnsInCondition(String where, Connection conn, String table) {
        if (StringUtils.isEmpty(where)) {
            return null;
        }
        SQLExpr expr = SQLUtils.toSQLExpr(where, "mysql");
        List<String> allColumnsInTab = getAllColumnFromTab(conn, table);
        List<String> allColNames = getColNames(allColumnsInTab, expr);

        if (allColNames == null) {
            return null;
        }

        // Remove the duplicated column names
        return new TreeSet<>(allColNames);
    }

    private static List<String> getAllColumnFromTab(Connection conn, String tableName) {
        String sql = "show columns from " + tableName;
        Statement stmt = null;
        ResultSet rs = null;
        List<String> allColumns = new ArrayList<String>();
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                allColumns.add(rs.getString("Field").toUpperCase());
            }
        } catch (Exception e) {
            LOG.warn("fail to get all columns from table " + tableName, e);
        } finally {
            close(rs, stmt, null);
        }

        LOG.info("all columns in tab: " + String.join(",", allColumns));
        return allColumns;
    }

    /**
     * 找出where条件中的列名，目前仅支持全部为and条件，并且操作符为大于、大约等于、等于、小于、小于等于和不等于的表达式。
     * <p>
     * test coverage: - c6 = 20180710 OR c4 = 320: no index selected - 20180710
     * = c6: correct index selected - 20180710 = c6 and c4 = 320 or c2 < 100: no
     * index selected
     *
     * @param expr
     * @return
     */
    private static List<String> getColNames(List<String> allColInTab, SQLExpr expr) {
        List<String> colNames = new ArrayList<String>();
        if (expr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr exp = (SQLBinaryOpExpr) expr;
            if (exp.getOperator() == SQLBinaryOperator.BooleanAnd) {
                List<String> leftColumns = getColNames(allColInTab, exp.getLeft());
                List<String> rightColumns = getColNames(allColInTab, exp.getRight());
                if (leftColumns == null || rightColumns == null) {
                    return null;
                }
                colNames.addAll(leftColumns);
                colNames.addAll(rightColumns);
            } else if (exp.getOperator() == SQLBinaryOperator.GreaterThan
                    || exp.getOperator() == SQLBinaryOperator.GreaterThanOrEqual
                    || exp.getOperator() == SQLBinaryOperator.Equality
                    || exp.getOperator() == SQLBinaryOperator.LessThan
                    || exp.getOperator() == SQLBinaryOperator.LessThanOrEqual
                    || exp.getOperator() == SQLBinaryOperator.NotEqual) {
                // only support simple comparison operators
                String left = SQLUtils.toMySqlString(exp.getLeft()).toUpperCase();
                String right = SQLUtils.toMySqlString(exp.getRight()).toUpperCase();
                LOG.debug("left: " + left + ", right: " + right);
                if (allColInTab.contains(left)) {
                    colNames.add(left);
                }

                if (allColInTab.contains(right)) {
                    colNames.add(right);
                }
            } else {
                // unsupported operators
                return null;
            }
        }

        return colNames;
    }

    /**
     * Find all indexes including pk, uk and other ordinary index with their all columns
     *
     * @param conn
     * @param dbName
     * @param tableName
     * @param compatibleMode
     * @return Map<String, IndexSchema>
     */
    public static Map<String, IndexSchema> getAllIndex(Connection conn, String dbName, String tableName, String compatibleMode) {
        Map<String, IndexSchema> allIndex = new HashMap<>();
        String sql = "show index from " + tableName;
        if (isOracleMode(compatibleMode)) {
            tableName = tableName.toUpperCase();
            sql =
                    "SELECT I.INDEX_NAME AS Key_name,I.COLUMN_NAME AS Column_name,CASE WHEN C.CONSTRAINT_TYPE = 'P' THEN -2 WHEN C.CONSTRAINT_TYPE = 'U' THEN -1 ELSE 1 END AS Non_unique,T.NULLABLE AS"
                            + " \"Null\" "
                            + "FROM all_ind_columns I "
                            + "LEFT JOIN ALL_CONSTRAINTS C ON I.TABLE_OWNER=C.OWNER AND I.TABLE_NAME=C.TABLE_NAME AND I.INDEX_NAME = C.CONSTRAINT_NAME "
                            + "INNER JOIN ALL_TAB_COLS T ON i.TABLE_OWNER=T.OWNER AND i.TABLE_NAME=T.TABLE_NAME AND i.COLUMN_NAME=T.COLUMN_NAME AND T.HIDDEN_COLUMN='NO' "
                            + "WHERE I.TABLE_NAME = '%s' AND I.TABLE_OWNER = '%s' ORDER BY I.COLUMN_POSITION";
            sql = String.format(sql, tableName.toUpperCase(), dbName.toUpperCase());
        }
        Statement stmt = null;
        ResultSet rs = null;

        try {
            LOG.info("running sql to get index: " + sql);
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                int constraintType = rs.getInt("Non_unique");
                String realIndexName = rs.getString("Key_name");
                String keyName = constraintType == -2 ? "PRIMARY" : realIndexName;
                String colName = rs.getString("Column_name").toUpperCase();
                boolean isNullable = "YES".equalsIgnoreCase(rs.getString("Null")) || "Y".equalsIgnoreCase(rs.getString("Null"));
                allIndex.computeIfAbsent(keyName, k -> new IndexSchema(realIndexName, constraintType)).addColumn(colName, isNullable);
            }
        } catch (Exception e) {
            LOG.error("fail to get all keys from table" + sql, e);
        } finally {
            close(rs, stmt, null);
        }

        LOG.info("all index: " + allIndex);
        return allIndex;
    }

    /**
     * 由于ObProxy存在bug,事务超时或事务被杀时,conn的close是没有响应的
     *
     * @param rs
     * @param stmt
     * @param conn
     */
    public static void close(final ResultSet rs, final Statement stmt, final Connection conn) {
        DBUtil.closeDBResources(rs, stmt, conn);
    }

    /**
     * Find the most suitable index which should meet the conditions below:
     * If unique constraint is needed,
     * 1. existing where clause: the shortest unique index(including pk) contains the most columns in where clause
     * 2. no where clause: choose pk first, if no pk, choose the shortest uk
     * <p>
     * If no need unique,
     * 1. existing where clause: the shortest index(including pk,uk,ordinary index) contains the most columns in where clause
     * 2. no where clause: choose pk first, if no pk, choose the shortest uk and then shortest ordinary index
     *
     * @param allIndex
     * @param colNamesInCondition
     * @param needUnique          If this method is used to split, needUnique is false; If used to page query, needUnique is true.
     * @return IndexSchema Return the most suitable index, if no index was chosen return null
     */
    public static IndexSchema getIndexName(Map<String, IndexSchema> allIndex, Set<String> colNamesInCondition, boolean needUnique) {
        if (colNamesInCondition == null || colNamesInCondition.size() == 0) {
            if (allIndex.containsKey("PRIMARY")) {
                return allIndex.get("PRIMARY");
            }
            IndexSchema chosenIndex = null;
            boolean hasUnique = false;
            for (IndexSchema indexSchema : allIndex.values()) {
                if (needUnique && !indexSchema.isValueUnique()) {
                    continue;
                }
                if (chosenIndex == null) {
                    chosenIndex = indexSchema;
                    hasUnique = chosenIndex.isUniqueIndex();
                } else if (indexSchema.isUniqueIndex()) {
                    hasUnique = true;
                    chosenIndex = (!chosenIndex.isUniqueIndex() || indexSchema.compareTo(chosenIndex) > 0) ? indexSchema : chosenIndex;
                } else if (!needUnique && !hasUnique) {
                    chosenIndex = indexSchema.compareTo(chosenIndex) > 0 ? indexSchema : chosenIndex;
                }
            }
            return chosenIndex;
        }

        LOG.info("columNamesInConditions: " + String.join(",", colNamesInCondition));
        int score = 0;
        IndexSchema chosenIndex = null;
        for (IndexSchema indexSchema : allIndex.values()) {
            if (needUnique && !indexSchema.isValueUnique()) {
                continue;
            }
            int tempScore = colNamesInCondition.stream().mapToInt(e -> indexSchema.containColumn(e) ? 1 : 0).sum();
            if (tempScore > score) {
                score = tempScore;
                chosenIndex = indexSchema;
            } else if (tempScore == score) {
                chosenIndex = (chosenIndex == null || indexSchema.compareTo(chosenIndex) > 0) ? indexSchema : chosenIndex;
            }
        }
        return chosenIndex;
    }

    public static String buildPlaceHolder(int n) {
        if (n <= 0) {
            return "";
        }
        StringBuilder str = new StringBuilder(2 * n);
        str.append('?');
        for (int i = 1; i < n; i++) {
            str.append(",?");
        }
        return str.toString();
    }

    public static void binding(PreparedStatement ps, List<Column> list) throws SQLException {
        for (int i = 0, n = list.size(); i < n; i++) {
            Column c = list.get(i);
            if (c instanceof BoolColumn) {
                ps.setLong(i + 1, ((BoolColumn) c).asLong());
            } else if (c instanceof BytesColumn) {
                ps.setBytes(i + 1, ((BytesColumn) c).asBytes());
            } else if (c instanceof DateColumn) {
                ps.setTimestamp(i + 1, new Timestamp(((DateColumn) c).asDate().getTime()));
            } else if (c instanceof DoubleColumn) {
                ps.setDouble(i + 1, ((DoubleColumn) c).asDouble());
            } else if (c instanceof LongColumn) {
                ps.setLong(i + 1, ((LongColumn) c).asLong());
            } else if (c instanceof StringColumn) {
                ps.setString(i + 1, ((StringColumn) c).asString());
            } else {
                ps.setObject(i + 1, c.getRawData());
            }
        }
    }

    public static List<Column> buildPoint(Record savePoint, int[] pkIndexs) {
        List<Column> result = new ArrayList<Column>(pkIndexs.length);
        for (int i = 0, n = pkIndexs.length; i < n; i++) {
            result.add(savePoint.getColumn(pkIndexs[i]));
        }
        return result;
    }

    public static String getCompatibleMode(Connection conn) {
        String compatibleMode = OB_COMPATIBLE_MODE_MYSQL;
        String getCompatibleModeSql = "SHOW VARIABLES LIKE 'ob_compatibility_mode'";
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(getCompatibleModeSql);
            if (rs.next()) {
                compatibleMode = rs.getString("VALUE");
            }
        } catch (Exception e) {
            LOG.error("fail to get ob compatible mode, using mysql as default: " + e.getMessage());
        } finally {
            DBUtil.closeDBResources(rs, stmt, conn);
        }

        LOG.info("ob compatible mode is " + compatibleMode);
        return compatibleMode;
    }

    public static boolean isOracleMode(String mode) {
        return OB_COMPATIBLE_MODE_ORACLE.equalsIgnoreCase(mode);
    }

    public static String getDbNameFromJdbcUrl(String jdbcUrl) {
        Matcher matcher = JDBC_PATTERN.matcher(jdbcUrl);
        if (matcher.find()) {
            return matcher.group(3);
        } else {
            LOG.error("jdbc url {} is not valid.", jdbcUrl);
        }

        return null;
    }

    public static String buildQuerySql(boolean weakRead, String column, String table, String where) {
        if (weakRead) {
            return buildWeakReadQuerySql(column, table, where);
        } else {
            return SingleTableSplitUtil.buildQuerySql(column, table, where);
        }
    }

    public static String buildWeakReadQuerySql(String column, String table, String where) {
        String querySql;

        if (StringUtils.isBlank(where)) {
            querySql = String.format(Constant.WEAK_READ_QUERY_SQL_TEMPLATE_WITHOUT_WHERE, column, table);
        } else {
            querySql = String.format(Constant.WEAK_READ_QUERY_SQL_TEMPLATE, column, table, where);
        }

        return querySql;
    }

    /**
     * compare two ob versions
     *
     * @param version1
     * @param version2
     * @return 0 when the two versions are the same
     * -1 when version1 is smaller (earlier) than version2
     * 1 when version is bigger (later) than version2
     */
    public static int compareObVersion(String version1, String version2) {
        if (version1 == null || version2 == null) {
            throw new RuntimeException("can not compare null version");
        }
        ObVersion v1 = new ObVersion(version1);
        ObVersion v2 = new ObVersion(version2);
        return v1.compareTo(v2);
    }

    /**
     * @param conn
     * @param sql
     * @return
     */
    public static List<String> getResultsFromSql(Connection conn, String sql) {
        List<String> list = new ArrayList<>();
        Statement stmt = null;
        ResultSet rs = null;

        LOG.info("executing sql: " + sql);

        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                list.add(rs.getString(1));
            }
        } catch (Exception e) {
            LOG.error("error when executing sql: " + e.getMessage());
        } finally {
            DBUtil.closeDBResources(rs, stmt, null);
        }

        return list;
    }

    /**
     * get obversion, try ob_version first, and then try version if failed
     *
     * @param conn
     * @return
     */
    public static ObVersion getObVersion(Connection conn) {
        if (obVersion != null) {
            return obVersion;
        }
        List<String> results = getResultsFromSql(conn, "select ob_version()");
        if (results.size() == 0) {
            results = getResultsFromSql(conn, "select version()");
        }
        obVersion = new ObVersion(results.get(0));

        LOG.info("obVersion: " + obVersion);
        return obVersion;
    }

    public static Connection getConnection(Configuration connConf) {
        String userName = connConf.getString(Key.USERNAME);
        String password = connConf.getString(Key.PASSWORD);
        String jdbcUrl = connConf.getString(Key.JDBC_URL);
        return getConnection(jdbcUrl, userName, password, connConf);
    }

    public static Connection getConnection(String jdbcUrl, String username, String password, Configuration conf) {
        Connection connection = null;
        try {
            connection = RetryUtil.executeWithRetry(() -> {
                String user = username;
                String url = jdbcUrl;
                if (url.startsWith(com.alibaba.datax.plugin.rdbms.writer.Constant.OB10_SPLIT_STRING)) {
                    String[] ss = url.split(com.alibaba.datax.plugin.rdbms.writer.Constant.OB10_SPLIT_STRING_PATTERN);
                    if (ss.length != 3) {
                        throw DataXException.asDataXException(DBUtilErrorCode.JDBC_OB10_ADDRESS_ERROR, "JDBC OB10 format error.");
                    }
                    LOG.debug("this is ob1_0 jdbc url");
                    user = ss[1].trim() + ":" + user;
                    if (OB_COMPATIBLE_MODE_MYSQL.equalsIgnoreCase(compatibleMode)) {
                        url = ss[2];
                    } else {
                        url = ss[2].replace("mysql", "oceanbase");
                    }
                    LOG.debug("this is ob1_0 jdbc url. user=" + user + " :url=" + url);
                }

                Properties prop = new Properties();
                prop.put("user", user);
                prop.put("password", password);
                Class.forName(DATABASE_TYPE.getDriverClassName());
                LOG.debug("loading driver class: {}", DATABASE_TYPE.getDriverClassName());
                return DriverManager.getConnection(url, prop);
            }, DataXCaseEnvUtil.getRetryTimes(9), DataXCaseEnvUtil.getRetryInterval(1000L), DataXCaseEnvUtil.getRetryExponential(true));
        } catch (Exception e) {
            throw DataXException.asDataXException(DBUtilErrorCode.CONN_DB_ERROR, "Database connection failed.", e);
        }
        DBUtil.dealWithSessionConfig(connection, conf, DATABASE_TYPE, String.format("jdbcUrl:[%s]", jdbcUrl));
        return connection;
    }

    public static String wrapName(String name, boolean isOracleMode) {
        String quoteChar = isOracleMode ? "\"" : "`";
        return String.format("%s%s%s", quoteChar, name, quoteChar);
    }
}
