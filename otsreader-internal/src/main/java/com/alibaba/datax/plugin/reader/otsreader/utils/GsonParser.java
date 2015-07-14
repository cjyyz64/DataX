package com.alibaba.datax.plugin.reader.otsreader.utils;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.plugin.reader.otsreader.adaptor.ColumnAdaptor;
import com.alibaba.datax.plugin.reader.otsreader.adaptor.PrimaryKeyValueAdaptor;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSRange;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyValue;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class GsonParser {
    
    private static Gson gsonBuilder() {
        return new GsonBuilder()
        .registerTypeAdapter(PrimaryKeyValue.class, new PrimaryKeyValueAdaptor())
        .registerTypeAdapter(Column.class, new ColumnAdaptor())
        .create();
    }

    public static String rangeToJson (OTSRange range) {
        Gson g = gsonBuilder();
        return g.toJson(range);
    }

    public static OTSRange jsonToRange (String jsonStr) {
        Gson g = gsonBuilder();
        return g.fromJson(jsonStr, OTSRange.class);
    }

    public static String confToJson (OTSConf conf) {
        Gson g = gsonBuilder();
        return g.toJson(conf);
    }

    public static OTSConf jsonToConf (String jsonStr) {
        Gson g = gsonBuilder();
        return g.fromJson(jsonStr, OTSConf.class);
    }
}
