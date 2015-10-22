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

    private static boolean report;
    private static Long nodeId;
    private static Long instId;
    private static String jobMode;
    private static String srcType;
    private static String dstType;
    private static Integer taskNum;
    private static Integer channelNum;

    public static void initJobInfo(Configuration userConf){
        try{
            report = userConf.getBool(CoreConstant.DATAX_CORE_REPORT_DATAX_LOG, false);
            if(report){
                String skynetId = System.getenv("SKYNET_ID");
                long jobId = userConf.getLong(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, 0);
                nodeId = (skynetId == null ? jobId : Long.parseLong(skynetId));
                instId = jobId;
                if(jobId<=0 && System.getenv("SKYNET_TASKID")!=null){
                    instId = Long.parseLong(System.getenv("SKYNET_TASKID"));
                }
                jobMode = userConf.getString(CoreConstant.DATAX_CORE_CONTAINER_JOB_MODE, "standalone");
                srcType = userConf.getString(CoreConstant.DATAX_JOB_CONTENT_READER_NAME);
                dstType = userConf.getString(CoreConstant.DATAX_JOB_CONTENT_WRITER_NAME);
            }
        }catch (Exception e){
            LOG.warn("init log report info fail, message : " + e.getMessage());
        }
    }

    public static void initSplitInfo(Integer taskNum, Integer channelNum){
        LogReportUtil.taskNum = taskNum;
        LogReportUtil.channelNum = channelNum;
    }

    public static void reportDataxLog(Configuration srcConfig, Configuration dstConfig, long startTime, long endTime){
        try{
            reportDataxLog(srcConfig, dstConfig, null, startTime, endTime);
        }catch (Exception e){
            LOG.warn("report datax log fail, message : " + e.getMessage());
        }
    }

    public static void reportDataxLog(Configuration userConf, Communication communication, long startTime, long endTime){
        try{
            Configuration srcConfig = userConf.getConfiguration(
                    CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER);
            Configuration dstConfig = userConf.getConfiguration(
                    CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER);
            reportDataxLog(srcConfig, dstConfig, communication, startTime, endTime);
        }catch(Exception e){
            LOG.warn("report datax log fail, message : " + e.getMessage());
        }
    }

    private static void reportDataxLog(Configuration srcConfig, Configuration dstConfig, Communication communication,
        long startTime, long endTime){
        if(report){
            LogReportInfo info = buildReportInfo(communication, startTime, endTime, srcConfig, dstConfig);
            Result result = DataxServiceUtil.reportDataxLog(info);
            LOG.info("report datax log return code : " + result.getReturnCode());
        }else{
            LOG.info("report datax log is turn off");
        }
    }

    private static LogReportInfo buildReportInfo(Communication communication, long startTime, long endTime,
            Configuration srcConfig, Configuration dstConfig){
        LogReportInfo info = new LogReportInfo();
        info.setNodeId(nodeId);
        info.setInstId(instId);
        info.setSrcType(srcType);
        info.setSrcConfig(Engine.filterSensitiveConfiguration(srcConfig.clone()).toJSON());
        info.setDstType(dstType);
        info.setDstConfig(Engine.filterSensitiveConfiguration(dstConfig.clone()).toJSON());
        info.setBeginTime(new Date(startTime));
        info.setEndTime(new Date(endTime));
        info.setHostAddress(HostUtils.IP);
        info.setTaskNum(taskNum);
        info.setChannelNum(channelNum);
        info.setJobMode(jobMode);
        if(communication!=null){
            info.setTotalRecords(communication.getLongCounter(CommunicationTool.READ_SUCCEED_RECORDS));
            info.setTotalBytes(communication.getLongCounter(CommunicationTool.READ_SUCCEED_BYTES));
            info.setSpeedRecords(communication.getLongCounter(CommunicationTool.RECORD_SPEED));
            info.setSpeedBytes(communication.getLongCounter(CommunicationTool.BYTE_SPEED));
        }
        return info;
    }

}
