package com.alibaba.datax.plugin.reader.odpsreader.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.odpsreader.Constant;
import com.alibaba.datax.plugin.reader.odpsreader.Key;
import com.alibaba.datax.plugin.reader.odpsreader.OdpsReaderErrorCode;
import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;
import com.aliyun.odps.tunnel.TunnelException;

public final class OdpsSplitUtil {

	public static List<Configuration> doSplit(Configuration originalConfig,
			int adviceNum) {
		Odps odps = OdpsUtil.initOdps(originalConfig);
		String tableName = originalConfig.getString(Key.TABLE);

		Table table = OdpsUtil.getTable(odps, tableName);
		if (OdpsUtil.isPartitionedTable(table)) {
			// 分区表
			return splitPartitionedTable(originalConfig, table, adviceNum);
		} else {
			// 非分区表
			return splitForNonPartitionedTable(odps, adviceNum, originalConfig);
		}

	}

	private static List<Configuration> splitPartitionedTable(
			Configuration originalConfig, Table table, int adviceNum) {
		List<Configuration> splittedConfigs = new ArrayList<Configuration>();

		List<String> partitions = originalConfig.getList(Key.PARTITION,
				String.class);

		// TODO
		if (null == partitions || partitions.isEmpty()) {
			throw new DataXException(OdpsReaderErrorCode.NOT_SUPPORT_TYPE,
					"no partition to read.");
		}

		String splitMode = originalConfig.getString(Key.SPLIT_MODE);
		Configuration tempConfig = null;
		if (partitions.size() > adviceNum || "partition".equals(splitMode)) {
			// 此时不管 splitMode 是什么，都不需要再进行切分了
			for (String onePartition : partitions) {
				tempConfig = originalConfig.clone();
				tempConfig.set(Key.PARTITION, onePartition);
				splittedConfigs.add(tempConfig);
			}

			return splittedConfigs;
		} else {
			// 还需要计算对每个分区，切分份数等信息
			int eachPartitionShouldSplittedNumber = calculateEachPartitionShouldSplittedNumber(
					adviceNum, partitions.size());

			Odps odps = OdpsUtil.initOdps(originalConfig);

			for (String onePartition : partitions) {
				List<Configuration> configs = splitOnePartition(odps,
						onePartition, eachPartitionShouldSplittedNumber,
						originalConfig);
				splittedConfigs.addAll(configs);
			}

			return splittedConfigs;
		}
	}

	// TODO Mysqlreader 中有这个方法，考虑抽象
	private static int calculateEachPartitionShouldSplittedNumber(
			int adviceNumber, int partitionNumber) {
		double tempNum = 1.0 * adviceNumber / partitionNumber;

		return (int) Math.ceil(tempNum);
	}

	private static List<Configuration> splitForNonPartitionedTable(Odps odps,
			int adviceNum, Configuration sliceConfig) {
		List<Configuration> params = new ArrayList<Configuration>();

		String tunnelServer = sliceConfig.getString(Key.TUNNEL_SERVER);
		String tableName = sliceConfig.getString(Key.TABLE);

		DownloadSession session = getSessionForNonPartitionedTable(odps,
				tunnelServer, tableName);

		String id = session.getId();
		long count = session.getRecordCount();
		long start = 0;
		long end = 0;

		long step = count / adviceNum;
		while (end < count) {
			end = start + step;
			if (end > count) {
				end = count;
			}

			Configuration iParam = sliceConfig.clone();
			iParam.set(Constant.SESSION_ID, id);
			iParam.set(Constant.START_INDEX, start);
			iParam.set(Constant.STEP_COUNT, end - start);

			params.add(iParam);
			start = end;
		}
		return params;
	}

	private static List<Configuration> splitOnePartition(Odps odps,
			String onePartition, int adviceNum, Configuration sliceConfig) {
		List<Configuration> params = new ArrayList<Configuration>();

		String tunnelServer = sliceConfig.getString(Key.TUNNEL_SERVER);
		String tableName = sliceConfig.getString(Key.TABLE);

		DownloadSession session = getSessionForPartitionedTable(odps,
				tunnelServer, tableName, onePartition);

		String id = session.getId();
		long count = session.getRecordCount();
		long start = 0;
		long end = 0;

		long step = count / adviceNum;
		while (end < count) {
			end = start + step;
			if (end > count) {
				end = count;
			}

			Configuration iParam = sliceConfig.clone();
			iParam.set(Key.PARTITION, onePartition);
			iParam.set(Constant.SESSION_ID, id);
			iParam.set(Constant.START_INDEX, start);
			iParam.set(Constant.STEP_COUNT, end - start);

			params.add(iParam);
			start = end;
		}
		return params;
	}

	// TODO retry
	public static DownloadSession getSessionForNonPartitionedTable(Odps odps,
			String tunnelServer, String tableName) {

		TableTunnel tunnel = new TableTunnel(odps);
		if (StringUtils.isNoneBlank(tunnelServer)) {
			tunnel.setEndpoint(tunnelServer);
		}

		DownloadSession downloadSession = null;
		try {
			downloadSession = tunnel.createDownloadSession(
					odps.getDefaultProject(), tableName);
		} catch (TunnelException e) {
			throw new DataXException(OdpsReaderErrorCode.NOT_SUPPORT_TYPE, e);
		}

		return downloadSession;
	}

	// TODO retry
	public static DownloadSession getSessionForPartitionedTable(Odps odps,
			String tunnelServer, String tableName, String partition) {

		TableTunnel tunnel = new TableTunnel(odps);
		if (StringUtils.isNoneBlank(tunnelServer)) {
			tunnel.setEndpoint(tunnelServer);
		}

		DownloadSession downloadSession = null;

		PartitionSpec partitionSpec = new PartitionSpec(partition);

		try {
			downloadSession = tunnel.createDownloadSession(
					odps.getDefaultProject(), tableName, partitionSpec);
		} catch (TunnelException e) {
			throw new DataXException(OdpsReaderErrorCode.NOT_SUPPORT_TYPE, e);
		}

		return downloadSession;
	}
}
