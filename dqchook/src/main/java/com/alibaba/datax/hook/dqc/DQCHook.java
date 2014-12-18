package com.alibaba.datax.hook.dqc;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.spi.Hook;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import org.apache.commons.collections.map.DefaultedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by xiafei.qiuxf on 14/12/17.
 */
public class DQCHook implements Hook {

    private static final Logger LOG = LoggerFactory.getLogger(DQCHook.class);

    @Override
    public String getName() {
        return "DQC";
    }

    @Override
    public void invoke(Configuration jobConf, Map<String, Number> msg) {
        Map<String, String> info = getInfoForCheck(jobConf, msg);
        String res = DQCUtils.doDQCCheck(info);
        boolean ok = parseDQCResult(res);
        if (ok) {
            LOG.info("DQC 检查通过.");
        } else {
            LOG.error("DQC 检查失败!");
            throw DataXException.asDataXException(CommonErrorCode.HOOK_INTERNAL_ERROR, "DQC 检查失败!");
        }
    }

    private static final String DATAX_JOB_CONTENT_READER_NAME = "job.content[0].writer.name";
    private static final String DATAX_JOB_CONTENT_WRITER_PROJECT = "job.content[0].writer.parameter.project";
    private static final String DATAX_JOB_CONTENT_WRITER_TABLE = "job.content[0].writer.parameter.table";
    private static final String DATAX_JOB_CONTENT_WRITER_PARTITION = "job.content[0].writer.parameter.partition";


    public static final String READ_SUCCEED_RECORDS = "readSucceedRecords";
    public static final String READ_SUCCEED_BYTES = "readSucceedBytes";

    public static final String READ_FAILED_RECORDS = "readFailedRecords";
    public static final String READ_FAILED_BYTES = "readFailedBytes";

    public static final String WRITE_FAILED_RECORDS = "writeFailedRecords";
    public static final String WRITE_FAILED_BYTES = "writeFailedBytes";


    private static final String WRITE_SUCCEED_RECORDS = "writeSucceedRecords";
    private static final String WRITE_SUCCEED_BYTES = "writeSucceedBytes";


    private Map<String, String> getInfoForCheck(Configuration jobConf, Map<String, Number> msg) {
        DefaultedMap map = new DefaultedMap(0);


        Map<String, String> res = new HashMap<String, String>();

        res.put("writerType", jobConf.getString(DATAX_JOB_CONTENT_READER_NAME).toLowerCase());
        res.put("project", jobConf.getString(DATAX_JOB_CONTENT_WRITER_PROJECT, ""));
        res.put("table", jobConf.getString(DATAX_JOB_CONTENT_WRITER_TABLE, ""));
        res.put("partition", jobConf.getString(DATAX_JOB_CONTENT_WRITER_PARTITION, ""));
        // 所有的读到的行
        res.put("totalReadUnits", String.valueOf(
                getInt(msg, READ_SUCCEED_RECORDS) + getInt(msg, READ_FAILED_RECORDS)));
        // 写成功的
        res.put("totalWriteUnits", String.valueOf(
                getInt(msg, WRITE_SUCCEED_RECORDS)));
        // 所有失败的记录数
        res.put("totalErrorWriteUnits", String.valueOf(
                getInt(msg, READ_FAILED_RECORDS) + getInt(msg, WRITE_FAILED_RECORDS)));
        // 所有的读到的bytes
        res.put("totalReadBytes", String.valueOf(
                getInt(msg, READ_SUCCEED_BYTES) + getInt(msg, READ_FAILED_BYTES)));
        // 写成功的bytes
        res.put("totalWriteBytes", String.valueOf(
                getInt(msg, WRITE_SUCCEED_BYTES)));
        return res;
    }

    private static int getInt(Map<String, Number> map, String key) {
        Number val = map.get(key);
        if (val != null) {
            return val.intValue();
        } else {
            return 0;
        }
    }

    private static boolean parseDQCResult(String dqcResult) {
        if (null == dqcResult) {
            return true;
        }

        Map map = new HashMap<String, Object>();
        map = JSON.parseObject(dqcResult, map.getClass());
        Object retValue = map.get("returnCode");
        if (null != retValue && retValue.toString().equals("0")) {
            return true;
        }
        return false;
    }
}
