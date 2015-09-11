package com.alibaba.datax.core.util;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.HostUtils;
import com.alibaba.datax.core.Engine;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.dataxservice.face.domain.LogReportInfo;
import com.alibaba.datax.dataxservice.face.domain.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * Created by liuyi on 15/9/11.
 */
public class LogReportUtil {

    private static final Logger LOG = LoggerFactory.getLogger(LogReportUtil.class);

    public static void reportDataxLog(Configuration userConf, Communication communication, long startTime, long endTime){
        try{
            boolean report = userConf.getBool(CoreConstant.DATAX_CORE_REPORT_DATAX_LOG, false);
            if(report){
                LogReportInfo info = buildReportInfo(userConf, communication, startTime, endTime);
                Result result = DataxServiceUtil.reportDataxLog(info);
                LOG.info("report datax log return code : " + result.getReturnCode());
            }
        }catch (Exception e){
            LOG.warn("report datax log fail, message : " + e.getMessage());
        }
    }

    private static LogReportInfo buildReportInfo(Configuration userConf, Communication communication, long startTime, long endTime){
        LogReportInfo info = new LogReportInfo();
        String skynetId = System.getenv("SKYNET_ID");
        long jobId = userConf.getLong(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, 0);
        info.setNodeId(skynetId == null ? jobId : Long.parseLong(skynetId));
        Long instId = jobId;
        if(jobId==0 && System.getenv("SKYNET_TASKID")!=null){
            instId = Long.parseLong(System.getenv("SKYNET_TASKID"));
        }
        info.setInstId(instId);
        info.setSrcType(userConf.getString(CoreConstant.DATAX_JOB_CONTENT_READER_NAME));
        Configuration srcConfig = userConf.getConfiguration(
                CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER).clone();
        info.setSrcConfig(Engine.filterSensitiveConfiguration(srcConfig).toJSON());
        info.setDstType(userConf.getString(CoreConstant.DATAX_JOB_CONTENT_WRITER_NAME));
        Configuration dstConfig = userConf.getConfiguration(
                CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER).clone();
        info.setDstConfig(Engine.filterSensitiveConfiguration(dstConfig).toJSON());
        info.setBeginTime(new Date(startTime));
        info.setEndTime(new Date(endTime));
        if(communication!=null){
            info.setTotalRecords(communication.getLongCounter(CommunicationTool.READ_SUCCEED_RECORDS));
            info.setTotalBytes(communication.getLongCounter(CommunicationTool.READ_SUCCEED_BYTES));
            info.setSpeedRecords(communication.getLongCounter(CommunicationTool.RECORD_SPEED));
            info.setSpeedBytes(communication.getLongCounter(CommunicationTool.BYTE_SPEED));
        }
        info.setHostAddress(HostUtils.IP);
        info.setJobMode(userConf.getString(CoreConstant.DATAX_CORE_CONTAINER_JOB_MODE, "standalone"));
        return info;
    }

}
