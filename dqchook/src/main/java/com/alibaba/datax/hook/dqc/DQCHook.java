package com.alibaba.datax.hook.dqc;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.spi.Hook;
import com.alibaba.datax.common.util.Configuration;
import com.taobao.dqc.common.entity.DataSourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.Map;

/**
 * Created by xiafei.qiuxf on 14/12/17.
 */
public class DQCHook implements Hook {

    private static final Logger LOG = LoggerFactory.getLogger(DQCHook.class);

    private final static String DATAX_HOME = System.getProperty("datax.home");

    @Override
    public String getName() {
        return "DQC-数据质量中心";
    }

    @Override
    public void invoke(Configuration jobConf, Map<String, Number> msg) {
        try {
            DQCCheckInfo info = getInfoForCheck(jobConf, msg);

            DQCChecker util = new DQCChecker(
                    new FileInputStream(DATAX_HOME + "/hook/dqc/dqc.properties"));
            boolean ok = util.doDQCCheck(info);
            if (ok) {
                LOG.info("DQC 检查通过.");
            } else {
                LOG.error("DQC 检查未通过!");
                throw DataXException.asDataXException(
                        CommonErrorCode.HOOK_INTERNAL_ERROR, "DQC 检查未通过!");
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    CommonErrorCode.HOOK_INTERNAL_ERROR, "DQC 检查运行时出错!");
        }
    }

    private static final String DATAX_JOB_CONTENT_WRITER_NAME = "job.content[0].writer.name";
    private static final String DATAX_JOB_CONTENT_WRITER_PROJECT = "job.content[0].writer.parameter.project";
    private static final String DATAX_JOB_CONTENT_WRITER_TABLE = "job.content[0].writer.parameter.table";
    private static final String DATAX_JOB_CONTENT_WRITER_PARTITION = "job.content[0].writer.parameter.partition";
    public static final String READ_SUCCEED_RECORDS = "readSucceedRecords";
    public static final String READ_FAILED_RECORDS = "readFailedRecords";
    public static final String WRITE_FAILED_RECORDS = "writeFailedRecords";


    private DQCCheckInfo getInfoForCheck(Configuration jobConf, Map<String, Number> msg) {
        DQCCheckInfo info = new DQCCheckInfo();
        Map<String, String> env = System.getenv();
        info.setSkynetId(Integer.valueOf(env.get("SKYNET_ID")));
        info.setSkynetBizDate(env.get("SKYNET_BIZDATE"));
        info.setSkynetOnDuty(env.get("SKYNET_ONDUTY"));
        info.setSkynetSysEnv(env.get("SKYNET_SYSTEM_ENV"));

        String writerType = jobConf.getString(DATAX_JOB_CONTENT_WRITER_NAME);
        if("odpswriter".equalsIgnoreCase(writerType)) {
            // 只支持ODPS
            info.setDataSourceType(DataSourceType.Odps);
        }

        info.setProject(jobConf.getString(DATAX_JOB_CONTENT_WRITER_PROJECT, ""));
        info.setTable(jobConf.getString(DATAX_JOB_CONTENT_WRITER_TABLE, ""));
        info.setPartition(jobConf.getString(DATAX_JOB_CONTENT_WRITER_PARTITION, ""));

        info.setTotalReadRecord(getLong(msg, READ_SUCCEED_RECORDS) + getLong(msg, READ_FAILED_RECORDS));
        info.setTotalFailedRecord(getLong(msg, READ_FAILED_RECORDS) + getLong(msg, WRITE_FAILED_RECORDS));
        return info;
    }

    private static long getLong(Map<String, Number> map, String key) {
        Number val = map.get(key);
        if (val != null) {
            return val.longValue();
        } else {
            return 0L;
        }
    }
}
