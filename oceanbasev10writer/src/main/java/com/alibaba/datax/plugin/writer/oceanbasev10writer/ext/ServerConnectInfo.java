package com.alibaba.datax.plugin.writer.oceanbasev10writer.ext;

import static org.apache.commons.lang3.StringUtils.EMPTY;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ServerConnectInfo {
	
	public String clusterName;
	public String tenantName;
	// userName doesn't contain tenantName or clusterName
	public String userName;
	public String password;
	public String databaseName;
	public String ipPort;
	public String jdbcUrl;
	public String host;
	public boolean publicCloud;
	public int rpcPort;

	/**
	 *
	 * @param jdbcUrl format is jdbc:oceanbase//ip:port
	 * @param username format is cluster:tenant:username or username@tenant#cluster or user@tenant or user
	 * @param password
	 */
	public ServerConnectInfo(final String jdbcUrl, final String username, final String password) {
		this.jdbcUrl = jdbcUrl;
		this.password = password;
		parseJdbcUrl(jdbcUrl);
		parseFullUserName(username);
	}

	private void parseJdbcUrl(final String jdbcUrl) {
		Pattern pattern = Pattern.compile("//([\\w\\.\\-]+:\\d+)/([\\w-]+)\\?");
		Matcher matcher = pattern.matcher(jdbcUrl);
		if (matcher.find()) {
			String ipPort = matcher.group(1);
			String dbName = matcher.group(2);
			this.ipPort = ipPort;
			this.host = ipPort.split(":")[0];
			this.databaseName = dbName;
			this.publicCloud = host.endsWith("aliyuncs.com");
		} else {
			throw new RuntimeException("Invalid argument:" + jdbcUrl);
		}
	}

	private void parseFullUserName(final String fullUserName) {
		int tenantIndex = fullUserName.indexOf("@");
		int clusterIndex = fullUserName.indexOf("#");
		// 适用于jdbcUrl以||_dsc_ob10_dsc_开头的场景
		if (fullUserName.contains(":") && tenantIndex < 0) {
			String[] names = fullUserName.split(":");
			if (names.length != 3) {
				throw new RuntimeException("invalid argument: " + fullUserName);
			} else {
				this.clusterName = names[0];
				this.tenantName = names[1];
				this.userName = names[2];
			}
		} else if (tenantIndex < 0) {
			// 适用于short jdbcUrl，且username中不含租户名（主要是公有云场景，此场景下不计算分区）
			this.userName = fullUserName;
			this.clusterName = EMPTY;
			this.tenantName = EMPTY;
		} else {
			// 适用于short jdbcUrl，且username中含租户名
			this.userName = fullUserName.substring(0, tenantIndex);
			if (clusterIndex < 0) {
				this.clusterName = EMPTY;
				this.tenantName = fullUserName.substring(tenantIndex + 1);
			} else {
				this.clusterName = fullUserName.substring(clusterIndex + 1);
				this.tenantName = fullUserName.substring(tenantIndex + 1, clusterIndex);
			}
		}
	}

	@Override
	public String toString() {
		return "ServerConnectInfo{" +
				"clusterName='" + clusterName + '\'' +
				", tenantName='" + tenantName + '\'' +
				", userName='" + userName + '\'' +
				", password='" + password + '\'' +
				", databaseName='" + databaseName + '\'' +
				", ipPort='" + ipPort + '\'' +
				", jdbcUrl='" + jdbcUrl + '\'' +
				", publicCloud=" + publicCloud +
				", rpcPort=" + rpcPort +
				'}';
	}

	public String getFullUserName() {
		StringBuilder builder = new StringBuilder();
		builder.append(userName);
		if (publicCloud) {
			return builder.toString();
		}
		if (!EMPTY.equals(tenantName)) {
			builder.append("@").append(tenantName);
		}

		if (!EMPTY.equals(clusterName)) {
			builder.append("#").append(clusterName);
		}
		if (EMPTY.equals(this.clusterName) && EMPTY.equals(this.tenantName)) {
			return this.userName;
		}
		return builder.toString();
	}

	public void setRpcPort(int rpcPort) {
		this.rpcPort = rpcPort;
	}
}
