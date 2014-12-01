package com.alibaba.datax.plugin.writer.oceanbasewriter.utils;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.writer.oceanbasewriter.Key;
import com.alipay.oceanbase.OceanbaseDataSourceProxy;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public final class OBDataSource {

	private static DataSource OB;

	public static synchronized void init(final Configuration configuration)
			throws Exception {
		if (OB != null) return;
        OB = new OceanbaseDataSourceProxy();
        ((OceanbaseDataSourceProxy)OB).setConfigURL(configuration.getString(Key.CONFIG_URL));
        ((OceanbaseDataSourceProxy)OB).init();
	}

	public static synchronized void destory() throws Exception {
		((OceanbaseDataSourceProxy) OB).destroy();
	}

	public static <T> T executeQuery(String sql, ResultSetHandler<T> handler) throws Exception {
		Connection connection = null;
		Statement statement = null;
		ResultSet result = null;
		try {
			connection = OB.getConnection();
			statement = connection.createStatement();
			result = statement.executeQuery(sql);
			return handler.callback(result);
		} finally {
			DBUtil.closeDBResources(result, statement, connection);
		}
	}
	
	public static void execute(ConnectionHandler handler) throws Exception {
		Connection connection = null;
		Statement statement = null;
		try {
			connection = OB.getConnection();
			statement = handler.callback(connection);
		} finally {
			connection.rollback();
			DBUtil.closeDBResources(statement, connection);
		}
	}

	public static <T> T executeJDBCQuery(String ip, String port, String sql, ResultSetHandler<T> handler) throws Exception {
		Connection connection = null;
		Statement statement = null;
		ResultSet result = null;
		try {
			connection = DriverManager.getConnection(String.format("jdbc:mysql://%s:%s", ip, port), "monitor", "ocenabasev5_monitor");
			statement = connection.createStatement();
			result = statement.executeQuery(sql);
			return handler.callback(result);
		} finally {
			DBUtil.closeDBResources(result, statement, connection);
		}
	}
}