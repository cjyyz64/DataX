package com.alibaba.datax.hook.dqc;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.fastjson.JSON;
import com.taobao.dqc.common.entity.*;
import com.taobao.dw.dqc.DqcSdk;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public final class DQCUtils {
    private static final Logger LOG = LoggerFactory.getLogger(DQCUtils.class);

    private static final boolean IS_DEBUG = LOG.isDebugEnabled();

    private static final String CONFIG_FILE = "dqc.properties";

    public static String DATAX_HOME = System.getProperty("datax.home");

    private static Properties CONF_PROP = null;

    public static String doDQCCheck(Map<String, String> infoForCheck) {
        String dqcResult = null;

        Map<String, String> env = System.getenv();
        infoForCheck.putAll(env);
        if (IS_DEBUG) {
            LOG.debug("infoForCheck=[{}]", infoForCheck);
        }

        if (needRunDQC(infoForCheck)) {
            CONF_PROP = getDqcConfig();

            LOG.info("DataX begin to runDQC ...");
            TriggerJson triggerJson = getTriggerJson(infoForCheck);

            DqcSdk.hostname = CONF_PROP.getProperty("url");
            DqcSdk dqc = new DqcSdk();
            int retryTime = -1;

            retryTime = Integer.parseInt(CONF_PROP.getProperty("maxRetryTimes"));

            // TODO  remove
            LOG.info("dqc HOST:  " + DqcSdk.hostname);

            if (-1 == retryTime) {
                dqcResult = dqc.runDqcTask(triggerJson);
            } else {
                dqcResult = dqc.runDqcTask(triggerJson, retryTime);
            }
            LOG.info("dqc result[{}]", dqcResult);

            LOG.info("DataX finished runDQC ...");
        }

        return dqcResult;

    }

    // writer type is hivewriter/odpswriter and has env SKYNET_ID and it's value
    // should be a number .
    private static boolean needRunDQC(Map<String, String> infoForCheck) {
        String skynetIDStr = infoForCheck.get("SKYNET_ID");
        if (StringUtils.isNotBlank(skynetIDStr)) {
            try {
                Integer.parseInt(skynetIDStr);
            } catch (NumberFormatException e) {
                return false;
            }

            String writerType = infoForCheck.get("writerType");
            if (writerType.equalsIgnoreCase("hivewriter")
                    || writerType.equalsIgnoreCase("odpswriter")) {
                return true;
            }
        }

        return false;
    }

    private static TriggerJson getTriggerJson(Map<String, String> infoForCheck) {
        String dqcFormatStr = "{\"dtsummary\":{\"INPUT\":\"\",\"OUTPUT\":\"%s\",\"TOTAL_RECORDS\":\"%s\",\"GOOD_RECORDS\":\"%s\",\"BAD_RECORDS\":\"%s\",\"TOTAL_BYTES\":\"%s\",\"GOOD_BYTES\":\"%s\",\"BAD_BYTES\":\"%s\",\"TOTAL_SLICES\":\"0\",\"BadType\":\"0\",\"GOOD_SLICES\":\"0\",\"BAD_SLICES\":\"0\",\"MoreField\":\"0\",\"LessField\":\"0\",\"OverLength\":\"0\"}}";

        String output = getDQCOutputFromJobConf(infoForCheck);

        String totalReadRecords = infoForCheck.get("totalReadUnits");
        String totalWriteRecords = infoForCheck.get("totalWriteUnits");
        String totalBadRecords = infoForCheck.get("totalErrorWriteUnits");

        String totalReadBytes = infoForCheck.get("totalReadBytes");
        String totalWriteBytes = infoForCheck.get("totalWriteBytes");
        String totalBadBytes = String.valueOf(Long.parseLong(infoForCheck
                .get("totalReadBytes"))
                - Long.parseLong(infoForCheck.get("totalWriteBytes")));

        String[] strValues = {output, totalReadRecords, totalWriteRecords,
                totalBadRecords, totalReadBytes, totalWriteBytes, totalBadBytes};

        String dqcParameterStr = String.format(dqcFormatStr, strValues);

        if (IS_DEBUG) {
            LOG.debug("dqcParameterStr=[{}].", dqcParameterStr);
        }
        net.sf.json.JSONObject hookJson = net.sf.json.JSONObject
                .fromObject(dqcParameterStr);

        String scheduleId = infoForCheck.get("SKYNET_ID");
        String scheduleBizdate = infoForCheck.get("SKYNET_BIZDATE");
        String scheduleOnduty = infoForCheck.get("SKYNET_ONDUTY");

        boolean lastSql = true;

        // always be DateType.YMD
        DateType dateType = DateType.YMD;

        TriggerType type = new TriggerType();

        if (isCWF2(infoForCheck)) {
            LOG.info("It is CWF2 job ...");
            type.setAccessSys(AccessSystemType.CWF2);
        } else {
            LOG.info("It is CWF1 job ...");
            type.setAccessSys(AccessSystemType.CWF1);
        }
        type.setTaskType(TaskType.DataTrasmission);

        if (isODPSWriterJob(infoForCheck)) {
            type.setDataSource(DataSourceType.Odps);
        } else {
            type.setDataSource(DataSourceType.Hive);
        }

        TriggerJson trigger = new TriggerJson(hookJson, scheduleId,
                scheduleBizdate, scheduleOnduty, lastSql, dateType, type);

        return trigger;
    }

    // only can be hivewriter/odpswriter job
    private static String getDQCOutputFromJobConf(
            Map<String, String> infoForCheck) {
        String writerName = infoForCheck.get("writerType");
        String result = null;

        String project = null;
        String table = infoForCheck.get("table");
        String partition = infoForCheck.get("partition");

        String outputTemplate = "%s://%s/%s";

        if (writerName.equalsIgnoreCase("hivewriter")) {
            project = getHiveProject(table);
            result = String.format(outputTemplate, "hive", project, table);
        } else if (writerName.equalsIgnoreCase("odpswriter")) {
            project = infoForCheck.get("project");
            result = String.format(outputTemplate, "odps", project, table);
        }

        if (StringUtils.isNotBlank(partition)) {
            result += ("/" + partition);
        }
        return result;
    }

    private static String getHiveProject(String hiveTable) {
        String mcURL = CONF_PROP.getProperty("mcURLTemplate") + hiveTable;

        String ret = doRequest(mcURL, 3);
        Map<String, String> map = new HashMap<String, String>();
        map = JSON.parseObject(ret, map.getClass());
        String guid = map.get("guid");
        return guid.split("\\.")[1];
    }

    private static boolean isODPSWriterJob(Map<String, String> infoForCheck) {
        return infoForCheck.get("writerType").equalsIgnoreCase("odpswriter");
    }

    private static boolean isCWF2(Map<String, String> infoForCheck) {

        return infoForCheck.containsKey("SKYNET_SYSTEM_ENV");
    }

    // has not setConnectionTimeout
    private static String doRequest(String requestURL, int retryTime) {
        if (IS_DEBUG) {
            LOG.debug("Request: " + requestURL);
        }

        HttpClient httpclient = new HttpClient();
        String result = null;
        for (int i = 1; i <= retryTime; i++) {
            GetMethod getMethod = new GetMethod(requestURL);
            try {
                int statusCode = httpclient.executeMethod(getMethod);
                result = getMethod.getResponseBodyAsString();

                if (IS_DEBUG) {
                    LOG.debug("Response: {}", result);
                }
                if (200 != statusCode) {
                    continue;
                }

                return result;
            } catch (Exception e) {
                try {
                    Thread.sleep((int) ((Math.pow(2, i) + Math.random()) * 1000));
                } catch (InterruptedException ignored) {
                }
                if (i >= retryTime) {
                    throw DataXException.asDataXException(CommonErrorCode.HOOK_INTERNAL_ERROR,
                            "Call DQC failed after try " + retryTime + " times.", e);
                }
            } finally {
                getMethod.releaseConnection();
            }
        }
        throw DataXException.asDataXException(CommonErrorCode.HOOK_INTERNAL_ERROR,
                "System error, cannot reach here !");
    }

    private static Properties getDqcConfig() {
        Properties prop = new Properties();
        try {
            prop.load(new FileInputStream(DATAX_HOME + "/hook/dqc/dqc.properties"));
        } catch (IOException e) {
            throw DataXException.asDataXException(CommonErrorCode.HOOK_INTERNAL_ERROR,
                    "Error when open DQC config file: " + CONFIG_FILE);
        }
        return prop;
    }

}
