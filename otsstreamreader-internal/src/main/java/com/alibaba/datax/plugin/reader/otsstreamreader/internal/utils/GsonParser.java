package com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils;

import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConfig;
import com.google.gson.GsonBuilder;

public class GsonParser {

    public static String configToJson(OTSStreamReaderConfig config) {
        return new GsonBuilder().create().toJson(config);
    }

    public static OTSStreamReaderConfig jsonToConfig(String jsonStr) {
        return new GsonBuilder().create().fromJson(jsonStr, OTSStreamReaderConfig.class);
    }
}
