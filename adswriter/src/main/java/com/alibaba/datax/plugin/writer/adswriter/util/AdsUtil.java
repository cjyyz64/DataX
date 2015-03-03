package com.alibaba.datax.plugin.writer.adswriter.util;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.adswriter.AdsHelper;
import com.alibaba.datax.plugin.writer.adswriter.AdsWriterErrorCode;
import com.alibaba.datax.plugin.writer.adswriter.odps.FieldSchema;
import com.alibaba.datax.plugin.writer.adswriter.odps.TableMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by judy.lt on 2015/1/30.
 */
public class AdsUtil {
    private static final Logger LOG = LoggerFactory.getLogger(AdsUtil.class);

    /*检查配置文件中必填的配置项是否都已填
    * */
    public static void checkNecessaryConfig(Configuration originalConfig) {
        //检查ADS必要参数
        originalConfig.getNecessaryValue(Key.ADS_URL,
                AdsWriterErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(Key.USERNAME,
                AdsWriterErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(Key.PASSWORD,
                AdsWriterErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(Key.SCHEMA,
                AdsWriterErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(Key.Life_CYCLE,
                AdsWriterErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(Key.ADS_TABLE,
                AdsWriterErrorCode.REQUIRED_VALUE);
    }

    /*生成AdsHelp实例
    * */
    public static AdsHelper createAdsHelp(Configuration originalConfig){
        //Get adsUrl,userName,password,schema等参数,创建AdsHelp实例
        String adsUrl = originalConfig.getString(Key.ADS_URL);
        String userName = originalConfig.getString(Key.USERNAME);
        String password = originalConfig.getString(Key.PASSWORD);
        String schema = originalConfig.getString(Key.SCHEMA);
        return new AdsHelper(adsUrl,userName,password,schema);
    }

    /*生成ODPSWriter Plugin所需要的配置文件
    * */
    public static Configuration generateConf(Configuration originalConfig, String odpsTableName, TableMeta tableMeta){
        Configuration newConfig = originalConfig.clone();
        String endPoint = PropertyLoader.getString(Key.CONFIG_ODPS_SERVER);
        String tunnel = PropertyLoader.getString(Key.CONFIG_TUNNEL);
        String accessId = PropertyLoader.getString(Key.CONFIG_ACCESS_ID);
        String accessKey = PropertyLoader.getString(Key.CONFIG_ACCESS_KEY);
        String project = PropertyLoader.getString(Key.CONFIG_PROJECT);
        boolean truncate = PropertyLoader.getBoolean(Key.CONFIG_TRUNCATE);
        newConfig.set(Key.ODPSTABLENAME, odpsTableName);
        newConfig.set(Key.ODPS_SERVER,endPoint);
        newConfig.set(Key.TUNNEL_SERVER,tunnel);
        newConfig.set(Key.ACCESS_ID,accessId);
        newConfig.set(Key.ACCESS_KEY,accessKey);
        newConfig.set(Key.PROJECT,project);
        newConfig.set(Key.TRUNCATE,truncate);
        newConfig.set(Key.PARTITION,null);
//        newConfig.remove(Key.PARTITION);
        List<FieldSchema> cols = tableMeta.getCols();
        List<String> allColumns = new ArrayList();
        if(cols != null && !cols.isEmpty()){
            for(FieldSchema col:cols){
                allColumns.add(col.getName());
            }
        }
        newConfig.set(Key.COLUMN,allColumns);
        return newConfig;
    }

    /*生成ADS数据倒入时的source_path
    * */
    public static String generateSourcePath(String project, String tmpOdpsTableName){
        StringBuilder builder = new StringBuilder();
        builder.append("odps://").append(project).append("/").append(tmpOdpsTableName);
        return builder.toString();
    }

}
