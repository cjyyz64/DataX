package com.alibaba.datax.plugin.writer.adswriter;

import com.alibaba.datax.common.util.Configuration;

/**
 * Created by xiafei.qiuxf on 15/4/13.
 */
public class TransferProjectConf {

    public final static String KEY_ACCESS_ID = "odpsAccount.accessId";
    public final static String KEY_ACCESS_KEY = "odpsAccount.accessKey";
    public final static String KEY_ACCOUNT = "odpsAccount.account";
    public final static String KEY_ODPS_SERVER = "odpsAccount.odpsServer";
    public final static String KEY_ODPS_TUNNEL = "odpsAccount.odpsTunnel";
    public final static String KEY_ACCOUNT_TYPE = "odpsAccount.accountType";
    public final static String KEY_PROJECT = "odpsAccount.project";

    private String accessId;
    private String accessKey;
    private String account;
    private String odpsServer;
    private String odpsTunnel;
    private String accountType;
    private String project;

    public static  TransferProjectConf create(Configuration adsWriterConf) {
        TransferProjectConf res = new TransferProjectConf();
        res.accessId = adsWriterConf.getNecessaryValue(KEY_ACCESS_ID, AdsWriterErrorCode.REQUIRED_VALUE);
        res.accessKey = adsWriterConf.getNecessaryValue(KEY_ACCESS_KEY, AdsWriterErrorCode.REQUIRED_VALUE);
        res.account = adsWriterConf.getNecessaryValue(KEY_ACCOUNT, AdsWriterErrorCode.REQUIRED_VALUE);
        res.odpsServer = adsWriterConf.getNecessaryValue(KEY_ODPS_SERVER, AdsWriterErrorCode.REQUIRED_VALUE);
        res.odpsTunnel = adsWriterConf.getString(KEY_ODPS_TUNNEL);
        res.accountType = adsWriterConf.getString(KEY_ACCOUNT_TYPE, "aliyun");
        res.project = adsWriterConf.getNecessaryValue(KEY_PROJECT, AdsWriterErrorCode.REQUIRED_VALUE);
        return res;
    }

    public String getAccessId() {
        return accessId;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getAccount() {
        return account;
    }

    public String getOdpsServer() {
        return odpsServer;
    }

    public String getOdpsTunnel() {
        return odpsTunnel;
    }

    public String getAccountType() {
        return accountType;
    }

    public String getProject() {
        return project;
    }
}
