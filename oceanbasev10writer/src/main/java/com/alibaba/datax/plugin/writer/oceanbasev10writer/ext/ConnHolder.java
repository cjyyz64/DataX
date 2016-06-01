package com.alibaba.datax.plugin.writer.oceanbasev10writer.ext;

import java.sql.Connection;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;

/**
 * 数据库连接代理对象,负责创建连接，重新连接
 * 
 * @author biliang.wbl
 *
 */
public class ConnHolder {
	private final Configuration config;
	private final String jdbcUrl;
	private final String userName;
	private final String password;

	private Connection conn;

	public ConnHolder(Configuration config, String jdbcUrl, String userName, String password) {
		super();
		this.config = config;
		this.jdbcUrl = jdbcUrl;
		this.userName = userName;
		this.password = password;
	}

	public Connection initConnection() {
		String BASIC_MESSAGE = String.format("jdbcUrl:[%s]", this.jdbcUrl);
		conn = DBUtil.getConnection(DataBaseType.MySql, jdbcUrl, userName, password);
		DBUtil.dealWithSessionConfig(conn, config, DataBaseType.MySql, BASIC_MESSAGE);
		return conn;
	}

	public Connection reconnect() {
		DBUtil.closeDBResources(null, conn);
		return initConnection();
	}

	public Connection getConn() {
		return conn;
	}

	public Configuration getConfig() {
		return config;
	}

	public String getJdbcUrl() {
		return jdbcUrl;
	}

	public String getUserName() {
		return userName;
	}
}
