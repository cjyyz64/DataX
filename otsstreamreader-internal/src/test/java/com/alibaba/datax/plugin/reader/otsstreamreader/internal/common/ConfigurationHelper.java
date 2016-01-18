package com.alibaba.datax.plugin.reader.otsstreamreader.internal.common;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConfig;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.OTSHelper;
import com.aliyun.openservices.ots.internal.OTS;
import com.aliyun.openservices.ots.internal.OTSAsync;
import com.aliyun.openservices.ots.internal.OTSClientAsync;

public class ConfigurationHelper {

    public static Configuration loadConf() {
        String path = "src/test/resources/conf.json";
        InputStream f;
        try {
            f = new FileInputStream(path);
            Configuration p = Configuration.from(f);
            return p;
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static Configuration loadConf(String dataTable, String statusTable,
                                         long startTimestampMillis, long endTimestampMillis) {
        Configuration configuration = loadConf();
        configuration.set("dataTable", dataTable);
        configuration.set("statusTable", statusTable);
        configuration.set("startTimestampMillis", startTimestampMillis);
        configuration.set("endTimestampMillis", endTimestampMillis);
        return configuration;
    }

    public static OTSStreamReaderConfig loadReaderConfig(String dataTable, String statusTable,
                                                         long startTimestampMillis, long endTimestampMillis) {
        Configuration configuration = loadConf(dataTable, statusTable, startTimestampMillis, endTimestampMillis);
        OTSStreamReaderConfig config = OTSStreamReaderConfig.load(configuration);
        return config;
    }

    public static OTS getOTSFromConfig() {
        OTSStreamReaderConfig config = loadReaderConfig("a", "b", 1, 2);
        return OTSHelper.getOTSInstance(config);
    }

    public static OTSAsync getOTSAsyncFromConfig() {
        OTSStreamReaderConfig config = loadReaderConfig("a", "b", 1, 2);
        OTSAsync otsAsync = new OTSClientAsync(config.getEndpoint(), config.getAccessId(), config.getAccessKey(), config.getInstanceName());
        return otsAsync;
    }
}

