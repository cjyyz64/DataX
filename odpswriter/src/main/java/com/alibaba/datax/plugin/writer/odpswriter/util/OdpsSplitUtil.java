package com.alibaba.datax.plugin.writer.odpswriter.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.odpswriter.Constant;
import com.alibaba.datax.plugin.writer.odpswriter.OdpsWriterErrorCode;
import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;
import com.aliyun.odps.tunnel.TunnelException;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public final class OdpsSplitUtil {

    public static List<Configuration> doSplit(Configuration originalConfig, String uploadId, List<Long> blocks,
                                              int mandatoryNumber) {

        List<Configuration> configs = new ArrayList<Configuration>();

        Configuration tempConf = null;

        for (int i = 0; i < mandatoryNumber; i++) {
            blocks.add((long) i + 1);

            tempConf = originalConfig.clone();
            tempConf.set(Constant.UPLOAD_ID, uploadId);
            tempConf.set(Constant.BLOCK_ID, i + 1);
            configs.add(tempConf);
        }

        return configs;
    }

    // TODO retry
    public static UploadSession createSessionForNonPartitionedTable(Odps odps,
                                                                 String tunnelServer, String tableName) {

        TableTunnel tunnel = new TableTunnel(odps);
        if (StringUtils.isNoneBlank(tunnelServer)) {
            tunnel.setEndpoint(tunnelServer);
        }

        UploadSession uploadSession = null;
        try {
            uploadSession = tunnel.createUploadSession(odps.getDefaultProject(), tableName);
        } catch (Exception e) {
            throw new DataXException(OdpsWriterErrorCode.NOT_SUPPORT_TYPE, e);
        }

        return uploadSession;
    }

    // TODO retry
    public static UploadSession createSessionForPartitionedTable(Odps odps,
                                                              String tunnelServer, String tableName, String partition) {

        TableTunnel tunnel = new TableTunnel(odps);
        if (StringUtils.isNoneBlank(tunnelServer)) {
            tunnel.setEndpoint(tunnelServer);
        }

        UploadSession uploadSession = null;

        PartitionSpec partitionSpec = new PartitionSpec(partition);

        try {
            uploadSession = tunnel.createUploadSession(odps.getDefaultProject(), tableName, partitionSpec);
        } catch (TunnelException e) {
            throw new DataXException(OdpsWriterErrorCode.NOT_SUPPORT_TYPE, e);
        }

        return uploadSession;
    }

    // TODO retry
    public static UploadSession getSessionForNonPartitionedTable(Odps odps,
                                                                 String tunnelServer, String tableName, String uploadId) {

        TableTunnel tunnel = new TableTunnel(odps);
        if (StringUtils.isNoneBlank(tunnelServer)) {
            tunnel.setEndpoint(tunnelServer);
        }

        UploadSession uploadSession = null;
        try {
            uploadSession = tunnel.getUploadSession(odps.getDefaultProject(), tableName, uploadId);
        } catch (Exception e) {
            throw new DataXException(OdpsWriterErrorCode.NOT_SUPPORT_TYPE, e);
        }

        return uploadSession;
    }

    // TODO retry
    public static UploadSession getSessionForPartitionedTable(Odps odps,
                                                              String tunnelServer, String tableName, String partition, String uploadId) {

        TableTunnel tunnel = new TableTunnel(odps);
        if (StringUtils.isNoneBlank(tunnelServer)) {
            tunnel.setEndpoint(tunnelServer);
        }

        UploadSession uploadSession = null;

        PartitionSpec partitionSpec = new PartitionSpec(partition);

        try {
            uploadSession = tunnel.getUploadSession(odps.getDefaultProject(), tableName, partitionSpec, uploadId);
        } catch (TunnelException e) {
            throw new DataXException(OdpsWriterErrorCode.NOT_SUPPORT_TYPE, e);
        }

        return uploadSession;
    }
}
