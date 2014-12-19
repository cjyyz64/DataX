package com.alibaba.datax.hook.dqc;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.fastjson.JSON;
import com.taobao.dqc.common.entity.*;
import com.taobao.dw.dqc.DqcSdk;
import net.sf.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by xiafei.qiuxf on 14/12/19.
 */

/**
 * 不支持Hive
 */
public class DQCChecker {

    private static final Logger LOG = LoggerFactory.getLogger(DQCChecker.class);
    private static final boolean IS_DEBUG = LOG.isDebugEnabled();

    private String dqcUrl;
    private Integer maxRetry;

    public DQCChecker(InputStream in) {
        Properties conf = new Properties();
        try {
            conf.load(in);
            if (IS_DEBUG) {
                LOG.debug("DQC配置: " + conf);
            }

            dqcUrl = conf.getProperty("dqcUrl");
            maxRetry = Integer.valueOf(conf.getProperty("maxRetryTimes"));
            maxRetry = maxRetry < 1 ? 1 : maxRetry;
        } catch (IOException e) {
            throw DataXException.asDataXException(CommonErrorCode.HOOK_INTERNAL_ERROR,
                    "Error when loading DQC config", e);
        }
    }

    public boolean doDQCCheck(DQCCheckInfo info) {
        if (!needDQC(info)) {
            LOG.info("Skip dqc check, skynetId: {}, target type: {}", info.getSkynetId(), info.getDataSourceType());
            return true;
        }

        TriggerJson triggerJson = genTriggerJson(info);
        DqcSdk dqc = new DqcSdk();
        DqcSdk.hostname = dqcUrl;

        String res = dqc.runDqcTask(triggerJson, maxRetry);

        return parseDQCResult(res);
    }

    private boolean parseDQCResult(String dqcResult) {
        if (null == dqcResult) {
            return true;
        }
        Map map = JSON.parseObject(dqcResult, Map.class);

        Object retValue = map.get("returnCode");
        Object msg = map.get("message");

        LOG.info("DQC检查结束: returnCode: {}, message: {}", retValue, msg);

        if (null != retValue && retValue.toString().equals("0")) {
            return true;
        }
        return false;
    }

    private TriggerJson genTriggerJson(final DQCCheckInfo info) {
        Map<String, Map> param = new HashMap<String, Map>(1) {
            {
                this.put("dtsummary", new HashMap<String, String>() {
                    {
                        this.put("INPUT", "");
                        this.put("OUTPUT", getDqcOutput(info));
                        this.put("TOTAL_RECORDS", info.getTotalReadRecord().toString());
                        this.put("GOOD_RECORDS", info.getTotalSuccessRecord().toString());
                        this.put("BAD_RECORDS", info.getTotalFailedRecord().toString());
                        this.put("TOTAL_BYTES", "0");
                        this.put("GOOD_BYTES", "0");
                        this.put("BAD_BYTES", "0");
                        this.put("TOTAL_SLICES", "0");
                        this.put("BadType", "0");
                        this.put("GOOD_SLICES", "0");
                        this.put("BAD_SLICES", "0");
                        this.put("MoreField", "0");
                        this.put("LessField", "0");
                        this.put("OverLength", "0");
                    }
                });
            }
        };

        JSONObject hookJson = JSONObject.fromObject(param);
        if (IS_DEBUG) {
            LOG.debug("dqcParameterStr=[{}].", hookJson.toString(2));
        }

        TriggerType type = new TriggerType();
        type.setAccessSys(info.getSkynetSysEnv() != null ? AccessSystemType.CWF2 : AccessSystemType.CWF1);
        type.setTaskType(TaskType.DataTrasmission);
        type.setDataSource(info.getDataSourceType());

        return new TriggerJson(
                hookJson,
                info.getSkynetId().toString(),
                info.getSkynetBizDate(),
                info.getSkynetOnDuty(),
                true,
                DateType.YMD,
                type);

    }

    private String getDqcOutput(DQCCheckInfo info) {
        // 一定是odps，因为有needDQC()方法检验
        if (DataSourceType.Odps == info.getDataSourceType()) {
            return String.format("odps://%s/%s/%s",
                    info.getProject(), info.getTable(), info.getPartition());
        } else {
            throw DataXException.asDataXException(CommonErrorCode.HOOK_INTERNAL_ERROR,
                    "非法逻辑，只支持ODPS DQC检查，但是类型是: " + info.getDataSourceType());
        }
    }

    private boolean needDQC(DQCCheckInfo info) {
        // 只检查天网的odps任务
        return info.getSkynetId() != null && DataSourceType.Odps == info.getDataSourceType();
    }
}
