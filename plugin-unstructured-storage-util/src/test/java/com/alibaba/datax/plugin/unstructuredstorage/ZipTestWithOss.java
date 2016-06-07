package com.alibaba.datax.plugin.unstructuredstorage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.unstructuredstorage.reader.ZipCycleInputStream;
import com.aliyun.oss.ClientConfiguration;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.OSSObject;

public class ZipTestWithOss {
    Map<String, Object> ossSetting = new HashMap<String, Object>() {
        private static final long serialVersionUID = 1L;
        {
            put("endpoint", "http://oss-cn-hangzhou.aliyuncs.com");
            put("accessId", "0Jr3aep1X11RykR8");
            put("accessKey", "1dMT1K4PdcxIYjfPUPHgRTOf6c9hnV");
            put("bucket", "i-like-yixiao");
            put("object", "yixiao.zip");
        }
    };

    public static OSSClient initOssClient(Configuration conf) {
        String endpoint = conf.getString("endpoint");
        String accessId = conf.getString("accessId");
        String accessKey = conf.getString("accessKey");
        ClientConfiguration ossConf = new ClientConfiguration();
        ossConf.setSocketTimeout(50000);
        // .aliyun.com, if you are .aliyun.ga you need config this
        String cname = conf.getString("cname");
        if (StringUtils.isNotBlank(cname)) {
            List<String> cnameExcludeList = new ArrayList<String>();
            cnameExcludeList.add(cname);
            ossConf.setCnameExcludeList(cnameExcludeList);
        }
        OSSClient client = new OSSClient(endpoint, accessId, accessKey, ossConf);
        return client;
    }

    @Test
    public void testOriginal() throws IOException {
        StringBuilder sb = new StringBuilder();
        Configuration conf = Configuration.from(ossSetting);
        OSSClient client = ZipTestWithOss.initOssClient(conf);
        OSSObject ossObject = client.getObject(conf.getString("bucket"),
                conf.getString("object"));
        InputStream objectStream = ossObject.getObjectContent();
        ZipInputStream zipInputStream = new ZipInputStream(objectStream);

        ZipEntry zipEntry = null;
        while ((zipEntry = zipInputStream.getNextEntry()) != null) {
            if (zipEntry.isDirectory()) {
                System.out.println("meet a directory, ignore...");
            }

            System.out.println(zipEntry.getName());
            sb.append(zipEntry.getName() + ":");
            // System.out.println(zipEntry.getSize());
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    zipInputStream, "utf-8"));
            while (true) {
                String line = reader.readLine();
                if (null != line) {
                    System.out.println(line);
                    sb.append(line + "|");
                } else {
                    break;
                }
            }
            System.out.println();
            System.out.println(sb.toString());
        }
        Assert.assertTrue(sb
                .toString()
                .equals("1.csv:1,1,1,1|2,2,2,2|3,3,3,3|4,4,4,4|5,5,5,5|2.csv:a,a,a,a|b,b,b,b|c,c,c,c|d,d,d,d|"));
    }

    @Test
    public void testZipCycleInputStream() throws IOException {
        StringBuilder sb = new StringBuilder();
        Configuration conf = Configuration.from(ossSetting);
        OSSClient client = ZipTestWithOss.initOssClient(conf);
        OSSObject ossObject = client.getObject(conf.getString("bucket"),
                conf.getString("object"));
        InputStream objectStream = ossObject.getObjectContent();
        ZipCycleInputStream zipCycleInputStream = new ZipCycleInputStream(
                objectStream);
        BufferedReader reader = new BufferedReader(new InputStreamReader(
                zipCycleInputStream, "utf-8"));
        String line = null;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
            sb.append(line + "|");
        }
        System.out.println();
        System.out.println(sb.toString());
        Assert.assertTrue("1,1,1,1|2,2,2,2|3,3,3,3|4,4,4,4|5,5,5,5|a,a,a,a|b,b,b,b|c,c,c,c|d,d,d,d|"
                .equals(sb.toString()));

        System.out.println("test dir...");
        sb.setLength(0);
        // with dir
        ossObject = client
                .getObject(conf.getString("bucket"), "yixiao_dir.zip");
        objectStream = ossObject.getObjectContent();
        zipCycleInputStream = new ZipCycleInputStream(objectStream);
        reader = new BufferedReader(new InputStreamReader(zipCycleInputStream,
                "utf-8"));
        line = null;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
            sb.append(line + "|");
        }
        System.out.println();
        System.out.println(sb.toString());
        Assert.assertTrue("1,1,1,1|2,2,2,2|3,3,3,3|4,4,4,4|5,5,5,5|a,a,a,a|b,b,b,b|c,c,c,c|d,d,d,d|6,6,6,6|7,7,7,7|8,8,8,8|9,9,9,9|10,10,10,10|"
                .equals(sb.toString()));

    }
}
