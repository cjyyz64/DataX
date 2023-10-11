package com.alibaba.datax.plugin.writer.oceanbasev10writer.ext;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.util.ObWriterUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 数据库连接代理对象,负责创建连接，重新连接
 *
 * @author oceanbase
 *
 */
public class ObClientConnHolder extends AbstractConnHolder {
	private static final Logger LOG = LoggerFactory.getLogger(ObClientConnHolder.class);

	public ObClientConnHolder(Configuration config, String jdbcUrl, String userName, String password) {
		super(config, jdbcUrl,userName,password);
	}

	// Connect to ob with obclient and obproxy
	@Override
	public Connection initConnection() {
		String BASIC_MESSAGE = String.format("jdbcUrl:[%s]", this.jdbcUrl);
		DataBaseType dbType = DataBaseType.OceanBase;
		if (ObWriterUtils.isOracleMode()) {
		    // set up for writing timestamp columns
		    List<String> sessionConfig = config.getList(Key.SESSION, new ArrayList<String>(), String.class);
			sessionConfig.add("ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'");
			sessionConfig.add("ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF'");
			sessionConfig.add("ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT='YYYY-MM-DD HH24:MI:SS.FF TZR TZD'");
		    config.set(Key.SESSION, sessionConfig);
		}
		conn = DBUtil.getConnection(dbType, jdbcUrl, userName, password);
		DBUtil.dealWithSessionConfig(conn, config, dbType, BASIC_MESSAGE);
		return conn;
	}

	@Override
	public void destroy() {
		DBUtil.closeDBResources(null, conn);
	}

	@Override
	public void doCommit() {}
}
