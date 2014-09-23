package com.alibaba.datax.plugin.writer.odpswriter.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.odpswriter.OdpsWriterErrorCode;
import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;
import com.aliyun.odps.tunnel.TunnelException;

public final class OdpsSplitUtil {

	public static List<Configuration> doSplit(Configuration originalConfig,
			int mandatoryNumber) {

		List<Configuration> configs = new ArrayList<Configuration>();

		for (int i = 0; i < mandatoryNumber; i++) {
			configs.add(originalConfig.clone());
		}

		return configs;
	}

	// TODO retry
	public static UploadSession getSessionForNonPartitionedTable(Odps odps,
			String tunnelServer, String tableName) {

		TableTunnel tunnel = new TableTunnel(odps);
		if (StringUtils.isNoneBlank(tunnelServer)) {
			tunnel.setEndpoint(tunnelServer);
		}

		UploadSession uploadSession = null;
		try {
			uploadSession = tunnel.createUploadSession(
					odps.getDefaultProject(), tableName);
		} catch (TunnelException e) {
			throw new DataXException(OdpsWriterErrorCode.NOT_SUPPORT_TYPE, e);
		}

		return uploadSession;
	}

	// TODO retry
	public static UploadSession getSessionForPartitionedTable(Odps odps,
			String tunnelServer, String tableName, String partition) {

		TableTunnel tunnel = new TableTunnel(odps);
		if (StringUtils.isNoneBlank(tunnelServer)) {
			tunnel.setEndpoint(tunnelServer);
		}

		UploadSession uploadSession = null;

		PartitionSpec partitionSpec = new PartitionSpec(partition);

		try {
			uploadSession = tunnel.createUploadSession(
					odps.getDefaultProject(), tableName, partitionSpec);
		} catch (TunnelException e) {
			throw new DataXException(OdpsWriterErrorCode.NOT_SUPPORT_TYPE, e);
		}

		return uploadSession;
	}
}
