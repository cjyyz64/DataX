package com.alibaba.datax.plugin.writer.otswriter.common;

import com.alibaba.datax.plugin.writer.otswriter.adaptor.OTSAttrColumnAdaptor;
import com.alibaba.datax.plugin.writer.otswriter.adaptor.OTSConfAdaptor;
import com.alibaba.datax.plugin.writer.otswriter.adaptor.OTSPKColumnAdaptor;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSAttrColumn;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSPKColumn;
import com.aliyun.openservices.ots.model.Direction;
import com.aliyun.openservices.ots.model.RowPrimaryKey;
import com.aliyun.openservices.ots.model.TableMeta;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class TestGsonParser {
    
    private static Gson gsonBuilder() {
        return new GsonBuilder()
        .registerTypeAdapter(OTSConf.class, new OTSConfAdaptor())
        .registerTypeAdapter(OTSPKColumn.class, new OTSPKColumnAdaptor())
        .registerTypeAdapter(OTSAttrColumn.class, new OTSAttrColumnAdaptor())
        .create();
    }

    public static String confToJson (OTSConf conf) {
        Gson g = gsonBuilder();
        return g.toJson(conf);
    }

    public static OTSConf jsonToConf (String jsonStr) {
        Gson g = gsonBuilder();
        return g.fromJson(jsonStr, OTSConf.class);
    }

    public static String directionToJson (Direction direction) {
        Gson g = gsonBuilder();
        return g.toJson(direction);
    }

    public static Direction jsonToDirection (String jsonStr) {
        Gson g = gsonBuilder();
        return g.fromJson(jsonStr, Direction.class);
    }
    
    public static String metaToJson (TableMeta meta) {
        Gson g = gsonBuilder();
        return g.toJson(meta);
    }
    
    public static String rowPrimaryKeyToJson (RowPrimaryKey row) {
        Gson g = gsonBuilder();
        return g.toJson(row);
    }
}
