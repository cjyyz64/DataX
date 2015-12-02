package com.alibaba.datax.plugin.reader.otsstreamreader.internal.config;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.OTSStreamReaderException;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.ParamChecker;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.TimeUtils;
import com.aliyun.openservices.ots.internal.OTS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;

public class OTSStreamReaderConfig {

    private static final Logger LOG = LoggerFactory.getLogger(OTSStreamReaderConfig.class);

    private static final String KEY_OTS_ENDPOINT = "endpoint";
    private static final String KEY_OTS_ACCESSID = "accessId";
    private static final String KEY_OTS_ACCESSKEY = "accessKey";
    private static final String KEY_OTS_INSTANCE_NAME = "instanceName";
    private static final String KEY_DATA_TABLE_NAME = "dataTable";
    private static final String KEY_STATUS_TABLE_NAME = "statusTable";
    private static final String KEY_START_TIMESTAMP_MILLIS = "startTimestampMillis";
    private static final String KEY_END_TIMESTAMP_MILLIS = "endTimestampMillis";
    private static final String KEY_IS_EXPORT_SEQUENCE_INFO = "isExportSequenceInfo";
    private static final String KEY_DATE = "date";
    private static final String KEY_MAX_RETRIES = "maxRetries";
    private static final int DEFAULT_MAX_RETRIES = 30;

    private String endpoint;
    private String accessId;
    private String accessKey;
    private String instanceName;
    private String dataTable;
    private String statusTable;
    private long startTimestampMillis;
    private long endTimestampMillis;
    private boolean isExportSequenceInfo;
    private int maxRetries = DEFAULT_MAX_RETRIES;

    private transient OTS otsForTest;

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getAccessId() {
        return accessId;
    }

    public void setAccessId(String accessId) {
        this.accessId = accessId;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public String getDataTable() {
        return dataTable;
    }

    public void setDataTable(String dataTable) {
        this.dataTable = dataTable;
    }

    public String getStatusTable() {
        return statusTable;
    }

    public void setStatusTable(String statusTable) {
        this.statusTable = statusTable;
    }

    public long getStartTimestampMillis() {
        return startTimestampMillis;
    }

    public void setStartTimestampMillis(long startTimestampMillis) {
        this.startTimestampMillis = startTimestampMillis;
    }

    public long getEndTimestampMillis() {
        return endTimestampMillis;
    }

    public void setEndTimestampMillis(long endTimestampMillis) {
        this.endTimestampMillis = endTimestampMillis;
    }

    public boolean isExportSequenceInfo() {
        return isExportSequenceInfo;
    }

    public void setIsExportSequenceInfo(boolean isExportSequenceInfo) {
        this.isExportSequenceInfo = isExportSequenceInfo;
    }

    public static OTSStreamReaderConfig load(Configuration param) {
        OTSStreamReaderConfig config = new OTSStreamReaderConfig();

        config.setEndpoint(ParamChecker.checkStringAndGet(param, KEY_OTS_ENDPOINT, true));
        config.setAccessId(ParamChecker.checkStringAndGet(param, KEY_OTS_ACCESSID, true));
        config.setAccessKey(ParamChecker.checkStringAndGet(param, KEY_OTS_ACCESSKEY, true));
        config.setInstanceName(ParamChecker.checkStringAndGet(param, KEY_OTS_INSTANCE_NAME, true));
        config.setDataTable(ParamChecker.checkStringAndGet(param, KEY_DATA_TABLE_NAME, true));
        config.setStatusTable(ParamChecker.checkStringAndGet(param, KEY_STATUS_TABLE_NAME, true));
        config.setIsExportSequenceInfo(param.getBool(KEY_IS_EXPORT_SEQUENCE_INFO, false));

        if (param.getString(KEY_DATE) == null &&
                (param.getLong(KEY_START_TIMESTAMP_MILLIS) == null || param.getLong(KEY_END_TIMESTAMP_MILLIS) == null)) {
            throw new OTSStreamReaderException("Must set date or time range.");
        }
        if (param.get(KEY_DATE) != null &&
                (param.getLong(KEY_START_TIMESTAMP_MILLIS) != null || param.getLong(KEY_END_TIMESTAMP_MILLIS) != null)) {
            throw new OTSStreamReaderException("Can't set date and time range both.");
        }

        if (param.getString(KEY_DATE) == null) {
            config.setStartTimestampMillis(param.getLong(KEY_START_TIMESTAMP_MILLIS));
            config.setEndTimestampMillis(param.getLong(KEY_END_TIMESTAMP_MILLIS));
        } else {
            String date = ParamChecker.checkStringAndGet(param, KEY_DATE, true);
            try {
                long startTimestampMillis = TimeUtils.parseDateToTimestampMillis(date);
                config.setStartTimestampMillis(startTimestampMillis);
                config.setEndTimestampMillis(startTimestampMillis + TimeUtils.DAY_IN_MILLIS);
            } catch (ParseException ex) {
                throw new OTSStreamReaderException("Can't parse date: " + date);
            }
        }

        if (config.getStartTimestampMillis() >= config.getEndTimestampMillis()) {
            throw new OTSStreamReaderException("EndTimestamp must be larger than startTimestamp.");
        }

        config.setMaxRetries(param.getInt(KEY_MAX_RETRIES, DEFAULT_MAX_RETRIES));

        LOG.info("endpoint: {}, accessId: {}, accessKey: {}, instanceName: {}, dataTableName: {}, statusTableName: {}," +
                " isExportSequenceInfo: {}, startTimestampMillis: {}, endTimestampMillis:{}, maxRetries:{}.", config.getEndpoint(),
                config.getAccessId(), config.getAccessKey(), config.getInstanceName(), config.getDataTable(),
                config.getStatusTable(), config.isExportSequenceInfo(), config.getStartTimestampMillis(),
                config.getEndTimestampMillis(), config.getMaxRetries());

        return config;
    }

    /**
     * test use
     * @return
     */
    public OTS getOtsForTest() {
        return otsForTest;
    }

    /**
     * test use
     * @param otsForTest
     */
    public void setOtsForTest(OTS otsForTest) {
        this.otsForTest = otsForTest;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }
}
