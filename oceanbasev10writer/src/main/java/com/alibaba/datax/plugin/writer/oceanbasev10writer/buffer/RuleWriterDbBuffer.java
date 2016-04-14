package com.alibaba.datax.plugin.writer.oceanbasev10writer.buffer;

import java.sql.Connection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;

/**
 * Date: 15/5/6 下午7:57
 *
 * @author liupeng <a href="mailto:liupengjava@gmail.com">Ricoul</a>
 */
public class RuleWriterDbBuffer {

	private String jdbcUrl;
	private String dbName;
	private Map<String, LinkedList<Record>> tableBuffer = new HashMap<String, LinkedList<Record>>();
	private Connection connection;
	private long lastCheckMemstoreTime;

	public void initConnection(Configuration writerSliceConfig, String userName, String password) {
		String BASIC_MESSAGE = String.format("jdbcUrl:[%s]", this.jdbcUrl);
		connection = DBUtil.getConnection(DataBaseType.MySql, jdbcUrl, userName, password);
		DBUtil.dealWithSessionConfig(connection, writerSliceConfig, DataBaseType.MySql, BASIC_MESSAGE);
	}

	public void initTableBuffer(List<String> tableList) {
		for (String table : tableList) {
			tableBuffer.put(table, new LinkedList<Record>());
		}
	}

	public void addRecord(Record record, String tableName) {
		LinkedList<Record> recordList = tableBuffer.get(tableName);
		if (recordList == null) {
			throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, "通过规则计算出来的table不存在, 算出的tableName="
					+ tableName + ",db=" + jdbcUrl + ", 请检查您配置的规则.");
		}
		recordList.add(record);
	}

	public Map<String, LinkedList<Record>> getTableBuffer() {
		return tableBuffer;
	}

	public void setJdbcUrl(String jdbcUrl) {
		this.jdbcUrl = jdbcUrl;
	}

	public Connection getConnection() {
		return connection;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public String getDbName() {
		return dbName;
	}

	public String getJdbcUrl() {
		return jdbcUrl;
	}

	public long getLastCheckMemstoreTime() {
		return lastCheckMemstoreTime;
	}

	public void setLastCheckMemstoreTime(long lastCheckMemstoreTime) {
		this.lastCheckMemstoreTime = lastCheckMemstoreTime;
	}
}
