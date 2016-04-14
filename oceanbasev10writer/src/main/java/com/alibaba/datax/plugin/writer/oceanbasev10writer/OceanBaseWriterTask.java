package com.alibaba.datax.plugin.writer.oceanbasev10writer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.buffer.RuleWriterDbBuffer;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.groovy.GroovyRuleExecutor;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.util.OBUtils;

/**
 * 2016-04-07
 * <p>
 * 专门针对OceanBase1.0的Writer
 * 
 * @author biliang.wbl
 *
 */
public class OceanBaseWriterTask extends CommonRdbmsWriter.Task {

	private List<RuleWriterDbBuffer> dbBufferList = new ArrayList<RuleWriterDbBuffer>();

	private GroovyRuleExecutor dbRuleExecutor = null;

	private GroovyRuleExecutor tableRuleExecutor = null;

	private String dbNamePattern;

	private String dbRule;

	private String tableNamePattern;

	private String tableRule;

	private MutablePair<String, String> metaDbTablePair = new MutablePair<String, String>();;

	private Map<String, String> tableWriteSqlMap = new HashMap<String, String>();

	// memstore_total 与 memstore_limit 比例的阈值,一旦超过这个值,则暂停写入
	private double memstoreThreshold = Config.DEFAULT_MEMSTORE_THRESHOLD;

	// memstore检查的间隔
	private long memstoreCheckIntervalSecond = Config.DEFAULT_MEMSTORE_CHECK_INTERVAL_SECOND;

	// 失败重试次数
	private int failTryCount = Config.DEFAULT_FAIL_TRY_COUNT;

	public OceanBaseWriterTask(DataBaseType dataBaseType) {
		super(dataBaseType);
	}

	@Override
	public void init(Configuration config) {
		this.username = config.getString(Key.USERNAME);
		this.password = config.getString(Key.PASSWORD);
		// 获取规则
		this.dbNamePattern = config.getString(Key.DB_NAME_PATTERN);
		this.dbRule = config.getString(Key.DB_RULE);
		this.dbRule = (dbRule == null) ? "" : dbRule;
		this.tableNamePattern = config.getString(Key.TABLE_NAME_PATTERN);
		this.tableRule = config.getString(Key.TABLE_RULE);
		this.tableRule = (tableRule == null) ? "" : tableRule;
		this.memstoreThreshold = config.getDouble(Config.MEMSTORE_THRESHOLD, Config.DEFAULT_MEMSTORE_THRESHOLD);
		this.memstoreCheckIntervalSecond = config.getLong(Config.MEMSTORE_CHECK_INTERVAL_SECOND,
				Config.DEFAULT_MEMSTORE_CHECK_INTERVAL_SECOND);
		this.failTryCount = config.getInt(Config.FAIL_TRY_COUNT, Config.DEFAULT_FAIL_TRY_COUNT);

		if (StringUtils.isNotBlank(this.dbRule)) {
			dbRuleExecutor = new GroovyRuleExecutor(dbRule, dbNamePattern);
		}
		if (StringUtils.isNoneBlank(this.tableRule)) {
			tableRuleExecutor = new GroovyRuleExecutor(tableRule, tableNamePattern);
		}

		this.columns = config.getList(Key.COLUMN, String.class);
		this.columnNumber = this.columns.size();
		//
		this.batchSize = config.getInt(Key.BATCH_SIZE, 100);
		this.batchByteSize = config.getInt(Key.BATCH_BYTE_SIZE, Constant.DEFAULT_BATCH_BYTE_SIZE);
		writeMode = config.getString(Key.WRITE_MODE, "INSERT");
		emptyAsNull = config.getBool(Key.EMPTY_AS_NULL, true);
		INSERT_OR_REPLACE_TEMPLATE = config.getString(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK);

		// init writerbuffer map,每一个db对应一个buffer
		List<Object> conns = config.getList(Constant.CONN_MARK, Object.class);

		for (int i = 0; i < conns.size(); i++) {
			Configuration connConf = Configuration.from(conns.get(i).toString());
			String jdbcUrl = connConf.getString(Key.JDBC_URL);
			List<String> tableList = connConf.getList(Key.TABLE, String.class);
			RuleWriterDbBuffer writerBuffer = new RuleWriterDbBuffer();
			writerBuffer.setJdbcUrl(jdbcUrl);
			writerBuffer.initTableBuffer(tableList);
			String dbName = getDbNameFromJdbcUrl(jdbcUrl);
			writerBuffer.setDbName(dbName);
			dbBufferList.add(writerBuffer);
			// 确定获取meta元信息的db和table
			if (i == 0 && tableList.size() > 0) {
				metaDbTablePair.setLeft(dbName);
				metaDbTablePair.setRight(tableList.get(0));
			}
			// init每一个table的insert语句
			for (String tableName : tableList) {
				Connection conn  = DBUtil.getConnection(DataBaseType.MySql, jdbcUrl, username, password);
				try{
					tableWriteSqlMap.put(tableName, OBUtils.buildWriteSql(tableName,columns,
							conn,writeMode));
				}finally{
					DBUtil.closeDBResources(null, null, conn);
				}
			}
		}

		// 检查规则配置
		checkRule(dbBufferList);
	}

	private void checkRule(List<RuleWriterDbBuffer> dbBufferList) {
		// 如果配置的分表名完全不同， 则必须要填写tableRule规则
		List<String> allTableList = new ArrayList<String>();
		List<String> allDbList = new ArrayList<String>();
		for (RuleWriterDbBuffer ruleWriterDbBuffer : dbBufferList) {
			allTableList.addAll(ruleWriterDbBuffer.getTableBuffer().keySet());
			allDbList.add(ruleWriterDbBuffer.getDbName());
		}
		Set<String> allTableSet = new HashSet<String>(allTableList);
		Set<String> allDbSet = new HashSet<String>(allDbList);

		// 如果是多表，必须要配置table规则
		if (allTableList.size() == allTableSet.size() && allTableSet.size() > 1) {
			if (tableRuleExecutor == null) {
				throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR,
						"配置的tableList为多表，但未配置分表规则，请检查您的配置");
			}
			return;
		}
		// 如果分表在每个db下，有部分重复的情况，则dbRule和tableRule都要填写
		if (allTableList.size() != allTableSet.size() && allTableSet.size() > 1 && allDbSet.size() > 1) {
			if (tableRuleExecutor == null || dbRuleExecutor == null) {
				throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR,
						"配置的多库中的表名有重复的，但未配置分库规则和分表规则，请检查您的配置");
			}
			return;
		}
		// 如果分表名都相同，分库名不同，那么可以只填写分库规则
		if (allTableList.size() != allTableSet.size() && allTableSet.size() == 1 && allDbSet.size() > 1) {
			if (dbRuleExecutor == null) {
				throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, "配置的所有表名都相同，但未配置分库规则，请检查您的配置");
			}
			return;
		}
		// 如果分表名和分库名都相同，只是jdbcurl不同，这种不支持
		if (allDbSet.size() == 1 && allTableSet.size() == 1 && allTableList.size() > 1 && allDbList.size() > 1) {
			throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, "配置的table和db名称都相同，此种回流方式不支持");
		}
	}

	private String getDbNameFromJdbcUrl(String jdbcUrl) {
		if (jdbcUrl.contains("?")) {
			return jdbcUrl.substring(jdbcUrl.lastIndexOf("/") + 1, jdbcUrl.indexOf("?"));
		} else {
			return jdbcUrl.substring(jdbcUrl.lastIndexOf("/") + 1);
		}
	}

	private Map<String, Object> convertRecord2Map(Record record) {
		Map<String, Object> map = new HashMap<String, Object>();
		for (int i = 0; i < this.columnNumber; i++) {
			// 设置列名统一为小写，规则的#号内部的列名称也都要小写
			String columnName = this.resultSetMetaData.getLeft().get(i).toLowerCase();
			map.put(columnName, record.getColumn(i).getRawData());
		}
		return map;
	}

	@Override
	public void startWrite(RecordReceiver recordReceiver, Configuration writerSliceConfig,
			TaskPluginCollector taskPluginCollector) {
		for (RuleWriterDbBuffer dbBuffer : dbBufferList) {
			dbBuffer.initConnection(writerSliceConfig, username, password);
		}

		this.taskPluginCollector = taskPluginCollector;

		// 用于写入数据的时候的类型根据目的表字段类型转换
		String metaTable = metaDbTablePair.getRight();
		this.resultSetMetaData = DBUtil.getColumnMetaData(dbBufferList.get(0).getConnection(), metaTable,
				StringUtils.join(this.columns, ","));

		List<Record> writeBuffer = new ArrayList<Record>(this.batchSize);
		int bufferBytes = 0;
		try {
			Record record;
			while ((record = recordReceiver.getFromReader()) != null) {
				if (record.getColumnNumber() != this.columnNumber) {
					// 源头读取字段列数与目的表字段写入列数不相等，直接报错
					throw DataXException.asDataXException(
							DBUtilErrorCode.CONF_ERROR,
							String.format("列配置信息有错误. 因为您配置的任务中，源头读取字段数:%s 与 目的表要写入的字段数:%s 不相等. 请检查您的配置并作出修改.",
									record.getColumnNumber(), this.columnNumber));
				}

				writeBuffer.add(record);
				bufferBytes += record.getMemorySize();

				if (writeBuffer.size() >= batchSize || bufferBytes >= batchByteSize) {
					calcRuleAndDoBatchInsert(writeBuffer, false);
					writeBuffer.clear();
					bufferBytes = 0;
				}
			}
			// flush buffer
			calcRuleAndDoBatchInsert(writeBuffer, true);
		} catch (Exception e) {
			throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, e);
		} finally {
			writeBuffer.clear();
			closeBufferConn();
		}
	}

	public void closeBufferConn() {
		for (RuleWriterDbBuffer ruleWriterDbBuffer : dbBufferList) {
			DBUtil.closeDBResources(null, null, ruleWriterDbBuffer.getConnection());
		}
	}

	private void calcRuleAndAddBuffer(Record record) {
		Map<String, Object> recordMap = convertRecord2Map(record);
		RuleWriterDbBuffer rightBuffer = null;

		// 如果dbRule为空,则通过tableName反查出来dbName
		if (dbRuleExecutor == null && tableRuleExecutor != null) {
			String tableName = tableRuleExecutor.executeRule(recordMap);
			for (RuleWriterDbBuffer ruleWriterDbBuffer : dbBufferList) {
				if (ruleWriterDbBuffer.getTableBuffer().keySet().contains(tableName)) {
					rightBuffer = ruleWriterDbBuffer;
					break;
				}
			}
			if (rightBuffer == null) {
				throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR,
						"通过规则计算出来的tableName查找对应的db不存在，tableName=" + tableName + ", 请检查您配置的规则.");
			}
			rightBuffer.addRecord(record, tableName);
		} else if (dbRuleExecutor != null && tableRuleExecutor != null) {// 两个规则都不为空，需要严格匹配计算出来的dbName和tableName
			String dbName = dbRuleExecutor.executeRule(recordMap);
			String tableName = tableRuleExecutor.executeRule(recordMap);
			for (RuleWriterDbBuffer ruleWriterDbBuffer : dbBufferList) {
				if (StringUtils.equals(ruleWriterDbBuffer.getDbName(), dbName)
						&& ruleWriterDbBuffer.getTableBuffer().keySet().contains(tableName)) {
					rightBuffer = ruleWriterDbBuffer;
					break;
				}
			}
			if (rightBuffer == null) {
				throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR,
						"通过规则计算出来的db和table不存在，算出的dbName=" + dbName + ",tableName=" + tableName + ", 请检查您配置的规则.");
			}
			rightBuffer.addRecord(record, tableName);
		} else if (dbRuleExecutor != null && tableRuleExecutor == null) {// 只存在dbRule，那么只能是多个分库的所有的table名称都相同
			String dbName = dbRuleExecutor.executeRule(recordMap);
			for (RuleWriterDbBuffer ruleWriterDbBuffer : dbBufferList) {
				if (StringUtils.equals(ruleWriterDbBuffer.getDbName(), dbName)) {
					rightBuffer = ruleWriterDbBuffer;
					break;
				}
			}
			if (rightBuffer == null) {
				throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, "通过规则计算出来的db不存在，算出的dbName="
						+ dbName + ", 请检查您配置的规则.");
			}

			if (rightBuffer.getTableBuffer().keySet().size() != 1) {
				throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, "通过规则计算出来的dbName[" + dbName
						+ "], 存在多张分表，请配置您的分表规则.");
			}
			String tableName = (String) rightBuffer.getTableBuffer().keySet().toArray()[0];
			rightBuffer.addRecord(record, tableName);
		}
	}

	/**
	 * 
	 * @param recordBuffer
	 * @param isFlush
	 *            if true then flush buffer
	 * @throws SQLException
	 */
	public void calcRuleAndDoBatchInsert(List<Record> recordBuffer, boolean isFlush) throws SQLException {
		// calcRule add all record
		for (Record record : recordBuffer) {
			calcRuleAndAddBuffer(record);
		}

		// do batchInsert
		for (RuleWriterDbBuffer dbBuffer : dbBufferList) {
			Connection conn = dbBuffer.getConnection();
			checkMemstore(dbBuffer);
			for (Map.Entry<String, LinkedList<Record>> entry : dbBuffer.getTableBuffer().entrySet()) {
				String tableName = entry.getKey();
				LinkedList<Record> recordList = entry.getValue();
				// if size < batchSize then skip
				if (!isFlush && recordList.size() < batchSize) {
					continue;
				}
				List<Record> list = new ArrayList<Record>();
				for (int i = 0; i < batchSize; i++) {
					Record r = recordList.poll();
					if (r != null) {
						list.add(r);
					} else {
						break;
					}
				}
				int tryCount = write(conn, tableName, list);
				if (tryCount >= failTryCount) {
					String msg = "OB无法写入 重试超过10次,jdbc=" + dbBuffer.getJdbcUrl();
					LOG.error(msg);
					throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, msg);
				}
			}
		}
	}

	private int write(Connection conn, String tableName, List<Record> list) throws SQLException {
		String writeSql = tableWriteSqlMap.get(tableName);
		int tryCount = 0;
		while (tryCount < failTryCount) {
			try {
				conn.setAutoCommit(false);
				PreparedStatement ps = null;
				try {
					ps = conn.prepareStatement(writeSql);
					for (Record record : list) {
						ps = fillPreparedStatement(ps, record);
						ps.addBatch();
					}
					ps.executeBatch();
					conn.commit();
				} catch (SQLException e) {
					LOG.warn("回滚此次写入, 休眠 30 秒,采用每次写入一行方式提交. 因为:" + e.getMessage());
					conn.rollback();
					OBUtils.sleep(30);
					doRuleOneInsert(conn, tableName, list);
				} finally {
					DBUtil.closeDBResources(ps, null);
					list.clear();
				}
				return tryCount;
			} catch (Throwable t) {
				LOG.warn("write OB fail,try 30 second later", t);
				OBUtils.sleep(30);
			}
			tryCount++;
		}
		return tryCount;
	}

	private void doRuleOneInsert(Connection conn, String tableName, List<Record> list) {
		PreparedStatement ps = null;
		try {
			conn.setAutoCommit(true);
			for (Record record : list) {
				try {
					ps = fillPreparedStatement(ps, record);
					ps.execute();
				} catch (SQLException e) {
					LOG.error("写入表[" + tableName + "]存在脏数据, 写入异常为:" + e.toString());
					this.taskPluginCollector.collectDirtyRecord(record, e);
				} finally {
					// 最后不要忘了关闭 preparedStatement
					ps.clearParameters();
				}
			}
		} catch (Exception e) {
			throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, e);
		} finally {
			DBUtil.closeDBResources(ps, null);
		}
	}

	/**
	 * 检查当前DB的memstore使用状态
	 * <p>
	 * 若超过阈值,则休眠
	 * 
	 * @param conn
	 */
	private void checkMemstore(RuleWriterDbBuffer dbBuffer) {
		long now = System.currentTimeMillis();
		if (now - dbBuffer.getLastCheckMemstoreTime() > 1000 * memstoreCheckIntervalSecond) {
			return;
		}
		Connection conn = dbBuffer.getConnection();
		while (OBUtils.isMemstoreFull(conn, memstoreThreshold)) {
			LOG.warn("OB memstore is full,sleep 1 second, jdbc=" + dbBuffer.getJdbcUrl() + ",threshold="
					+ memstoreThreshold);
			OBUtils.sleep(1);
		}
		dbBuffer.setLastCheckMemstoreTime(now);
	}
}
