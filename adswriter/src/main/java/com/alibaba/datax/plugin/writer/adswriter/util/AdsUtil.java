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

        //检查ODPS必要参数
        originalConfig.getNecessaryValue(Key.ODPS_SERVER,
                AdsWriterErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(Key.ACCESS_ID,
                AdsWriterErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(Key.ACCESS_KEY,
                AdsWriterErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(Key.PROJECT,
                AdsWriterErrorCode.REQUIRED_VALUE);


//        if (null == originalConfig.getList(Key.COLUMN) ||
//                originalConfig.getList(com.alibaba.datax.plugin.writer.odpswriter.Key.COLUMN, String.class).isEmpty()) {
//            throw DataXException.asDataXException(OdpsWriterErrorCode.REQUIRED_VALUE, "您未配置写入 ODPS 目的表的列信息. " +
//                    "正确的配置方式是给 column 配置上您需要读取的列名称,用英文逗号分隔.");
//        }
//
//        // getBool 内部要求，值只能为 true,false 的字符串（大小写不敏感），其他一律报错，不再有默认配置
//        Boolean truncate = originalConfig.getBool(Key.TRUNCATE);
//        if (null == truncate) {
//            throw DataXException.asDataXException(OdpsWriterErrorCode.REQUIRED_VALUE, "您未配置写入 ODPS 目的表前是否清空表/分区. " +
//                    "正确的配置方式是给 truncate 配置上true 或者 false.");
//        }
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
        /*TODO 需要的参数还却column list*/
        Configuration newConfig = originalConfig;
        newConfig.set(Key.TABLE, odpsTableName);
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
