package com.alibaba.datax.plugin.reader.oceanbasev10reader.util;

import java.util.List;

import com.alibaba.datax.common.element.Record;

public class TaskContext {
	private final String table;
	private String querySql;
	private final String where;
	private final int fetchSize;

	// 断点续读的保存点
	private volatile Record savePoint;

	// pk在column中的index,用于绑定变量时从savePoint中读取值
	// 如果这个值为null,则表示 不是断点续读的场景
	private int[] pkIndexs;

	private final List<String> columns;

	private String[] pkColumns;

	private long cost;

	private final int transferColumnNumber;

	public TaskContext(String table, List<String> columns, String where, int fetchSize) {
		super();
		this.table = table;
		this.columns = columns;
		// 针对只有querySql的场景
		this.transferColumnNumber = columns == null ? -1 : columns.size();
		this.where = where;
		this.fetchSize = fetchSize;
	}

	public String getQuerySql() {
		return querySql;
	}

	public void setQuerySql(String querySql) {
		this.querySql = querySql;
	}

	public String getWhere() {
		return where;
	}

	public Record getSavePoint() {
		return savePoint;
	}

	public void setSavePoint(Record savePoint) {
		this.savePoint = savePoint;
	}

	public int[] getPkIndexs() {
		return pkIndexs;
	}

	public void setPkIndexs(int[] pkIndexs) {
		this.pkIndexs = pkIndexs;
	}

	public List<String> getColumns() {
		return columns;
	}

	public String[] getPkColumns() {
		return pkColumns;
	}

	public void setPkColumns(String[] pkColumns) {
		this.pkColumns = pkColumns;
	}

	public String getTable() {
		return table;
	}

	public int getFetchSize() {
		return fetchSize;
	}

	public long getCost() {
		return cost;
	}

	public void addCost(long cost) {
		this.cost += cost;
	}

	public int getTransferColumnNumber() {
		return transferColumnNumber;
	}
}
