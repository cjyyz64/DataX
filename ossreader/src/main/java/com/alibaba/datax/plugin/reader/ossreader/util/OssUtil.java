package com.alibaba.datax.plugin.reader.ossreader.util;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.ossreader.Key;
import com.aliyun.oss.OSSClient;

/**
 * Created by mengxin.liumx on 2014/12/8.
 */
public class OssUtil {
    public static OSSClient initOssClient(Configuration conf){
        // TODO 增加 OSS 网络参数设置
        String endpoint = conf.getString(Key.ENDPOINT);
        String accessId = conf.getString(Key.ACCESSID);
        String accessKey = conf.getString(Key.ACCESSKEY);
        return new OSSClient(endpoint, accessId, accessKey);
    }
}
