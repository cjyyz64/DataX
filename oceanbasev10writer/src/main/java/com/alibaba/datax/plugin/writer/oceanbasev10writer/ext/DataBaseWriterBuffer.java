package com.alibaba.datax.plugin.writer.oceanbasev10writer.ext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;

/**
 * 
 * @author biliang.wbl
 *
 */
public class DataBaseWriterBuffer {
	private final ConnHolder connHolder;
	private final String dbName;
	private Map<String, LinkedList<Record>> tableBuffer = new HashMap<String, LinkedList<Record>>();
	private long lastCheckMemstoreTime;
	
	public DataBaseWriterBuffer(Configuration config,String jdbcUrl, String userName, String password,String dbName){
		this.connHolder = new ConnHolder(config, jdbcUrl, userName, password);
		this.dbName=dbName;
	}
	
	public ConnHolder getConnHolder(){
		return connHolder;
	}

	public void initTableBuffer(List<String> tableList) {
		for (String table : tableList) {
			tableBuffer.put(table, new LinkedList<Record>());
		}
	}
	
	public List<String> getTableList(){
		return new ArrayList<String>(tableBuffer.keySet());
	}

	public void addRecord(Record record, String tableName) {
		LinkedList<Record> recordList = tableBuffer.get(tableName);
		if (recordList == null) {
			throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, "通过规则计算出来的table不存在, 算出的tableName="
					+ tableName + ",db=" + connHolder.getJdbcUrl() + ", 请检查您配置的规则.");
		}
		recordList.add(record);
	}

	public Map<String, LinkedList<Record>> getTableBuffer() {
		return tableBuffer;
	}

	public String getDbName() {
		return dbName;
	}

	public long getLastCheckMemstoreTime() {
		return lastCheckMemstoreTime;
	}

	public void setLastCheckMemstoreTime(long lastCheckMemstoreTime) {
		this.lastCheckMemstoreTime = lastCheckMemstoreTime;
	}
}
