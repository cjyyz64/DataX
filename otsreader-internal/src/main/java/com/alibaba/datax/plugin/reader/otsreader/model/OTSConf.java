package com.alibaba.datax.plugin.reader.otsreader.model;

import java.util.List;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.Constant;
import com.alibaba.datax.plugin.reader.otsreader.Key;
import com.alibaba.datax.plugin.reader.otsreader.utils.ParamChecker;

public class OTSConf {
    private String endpoint = null;
    private String accessId = null;
    private String accessKey = null;
    private String instanceName = null;
    private String tableName = null;
    private OTSRange range = null;
    private List<OTSColumn> column = null;
    private OTSMode mode = null;

    private OTSMultiVersionConf multi = null;
    
    private int retry = Constant.VALUE.RETRY;
    private int sleepInMilliSecond = Constant.VALUE.SLEEP_IN_MILLISECOND;
    private int ioThreadCount = Constant.VALUE.IO_THREAD_COUNT;
    private int maxConnectCount = Constant.VALUE.MAX_CONNECT_COUNT;
    private int socketTimeoutInMilliSecond = Constant.VALUE.SOCKET_TIMEOUTIN_MILLISECOND;
    private int connectTimeoutInMilliSecond = Constant.VALUE.CONNECT_TIMEOUT_IN_MILLISECOND;

    public int getIoThreadCount() {
        return ioThreadCount;
    }

    public void setIoThreadCount(int ioThreadCount) {
        this.ioThreadCount = ioThreadCount;
    }

    public int getMaxConnectCount() {
        return maxConnectCount;
    }

    public void setMaxConnectCount(int maxConnectCount) {
        this.maxConnectCount = maxConnectCount;
    }

    public int getSocketTimeoutInMilliSecond() {
        return socketTimeoutInMilliSecond;
    }

    public void setSocketTimeoutInMilliSecond(int socketTimeoutInMilliSecond) {
        this.socketTimeoutInMilliSecond = socketTimeoutInMilliSecond;
    }

    public int getConnectTimeoutInMilliSecond() {
        return connectTimeoutInMilliSecond;
    }

    public void setConnectTimeoutInMilliSecond(int connectTimeoutInMilliSecond) {
        this.connectTimeoutInMilliSecond = connectTimeoutInMilliSecond;
    }

    public int getRetry() {
        return retry;
    }

    public void setRetry(int retry) {
        this.retry = retry;
    }

    public int getSleepInMilliSecond() {
        return sleepInMilliSecond;
    }

    public void setSleepInMilliSecond(int sleepInMilliSecond) {
        this.sleepInMilliSecond = sleepInMilliSecond;
    }

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

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public OTSRange getRange() {
        return range;
    }

    public void setRange(OTSRange range) {
        this.range = range;
    }

    public OTSMode getMode() {
        return mode;
    }

    public void setMode(OTSMode mode) {
        this.mode = mode;
    }

    public OTSMultiVersionConf getMulti() {
        return multi;
    }

    public void setMulti(OTSMultiVersionConf multi) {
        this.multi = multi;
    }

    public List<OTSColumn> getColumn() {
        return column;
    }

    public void setColumn(List<OTSColumn> column) {
        this.column = column;
    }

    public static OTSConf load(Configuration param) throws OTSCriticalException {
        OTSConf c = new OTSConf();
        
        // account
        c.setEndpoint(ParamChecker.checkStringAndGet(param, Key.OTS_ENDPOINT, true));
        c.setAccessId(ParamChecker.checkStringAndGet(param, Key.OTS_ACCESSID, true));
        c.setAccessKey(ParamChecker.checkStringAndGet(param, Key.OTS_ACCESSKEY, true));
        c.setInstanceName(ParamChecker.checkStringAndGet(param, Key.OTS_INSTANCE_NAME, true));
        c.setTableName(ParamChecker.checkStringAndGet(param, Key.TABLE_NAME, true));
        
        c.setRetry(param.getInt(Constant.KEY.RETRY, Constant.VALUE.RETRY));
        c.setSleepInMilliSecond(param.getInt(Constant.KEY.SLEEP_IN_MILLISECOND, Constant.VALUE.SLEEP_IN_MILLISECOND));
        c.setIoThreadCount(param.getInt(Constant.KEY.IO_THREAD_COUNT, Constant.VALUE.IO_THREAD_COUNT));
        c.setMaxConnectCount(param.getInt(Constant.KEY.MAX_CONNECT_COUNT, Constant.VALUE.MAX_CONNECT_COUNT));
        c.setSocketTimeoutInMilliSecond(param.getInt(Constant.KEY.SOCKET_TIMEOUTIN_MILLISECOND, Constant.VALUE.SOCKET_TIMEOUTIN_MILLISECOND));
        c.setConnectTimeoutInMilliSecond(param.getInt(Constant.KEY.CONNECT_TIMEOUT_IN_MILLISECOND, Constant.VALUE.CONNECT_TIMEOUT_IN_MILLISECOND));

        // range
        c.setRange(ParamChecker.checkRangeAndGet(param));
        
        // mode
        c.setMode(ParamChecker.checkModeAndGet(param));
        
        // column
        c.setColumn(ParamChecker.checkOTSColumnAndGet(param, c.getMode()));
        
        if (c.getMode() == OTSMode.MULTI_VERSION) {
            c.setMulti(OTSMultiVersionConf.load(param));
        } 
        return c;
    }
}
