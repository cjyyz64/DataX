package com.alibaba.datax.plugin.reader.ossreader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.junit.Test;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.ossreader.util.OssUtil;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.OSSObject;

public class OssReaderTest {

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

    @Test
    public void test() throws IOException {
        Configuration conf = Configuration.from(ossSetting);
        OSSClient client = OssUtil.initOssClient(conf);
        OSSObject ossObject = client.getObject(conf.getString(Key.BUCKET),
                conf.getString(Key.OBJECT));
        InputStream objectStream = ossObject.getObjectContent();
        ZipInputStream zipInputStream = new ZipInputStream(objectStream);

        ZipEntry zipEntry = null;
        while ((zipEntry = zipInputStream.getNextEntry()) != null) {
            if (zipEntry.isDirectory()) {
                System.out.println("meet a directory, ignore...");
            }

            System.out.println(zipEntry.getName());
            System.out.println(zipEntry.getSize());
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    zipInputStream, "utf-8"));
            while (true) {
                String line = reader.readLine();
                if (null != line) {
                    System.out.println(line);
                } else {
                    break;
                }
            }

        }
    }
}
