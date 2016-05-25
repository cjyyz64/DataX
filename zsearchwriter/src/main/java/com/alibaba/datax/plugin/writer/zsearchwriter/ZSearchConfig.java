package com.alibaba.datax.plugin.writer.zsearchwriter;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * 配置定义
 * Created by discolt on 16/4/26.
 */
public class ZSearchConfig {

    // ----------------------------------------
    //   类型定义 主键字段定义
    // ----------------------------------------

    public static final String TYPE_STRING             = "string";
    public static final String TYPE_TEXT               = "text";
    public static final String TYPE_LONG               = "long";
    public static final String TYPE_DOUBLE             = "double";
    public static final String PRIMARY_KEY_COLUMN_NAME = "pk";

    // ----------------------------------------
    //   ZSearch配置对象
    // ----------------------------------------

    // zsearch 服务器路径
    public final String                                server;
    // 表名
    public final String                                tableName;
    // 表token
    public final String                                tableToken;
    // 失效时间，不是必须，默认一年
    public final int                                   ttl;
    // 批次大小
    public final int                                   batchSize;
    // Http Pool 大小
    public final int                                   httpPoolSize;
    // 是否清除表数据 ， 不是必须，默认不清表
    public final boolean                               cleanup;
    // 是否开启gzip， 默认不开启
    public final boolean                               gzip;
    // 列
    public final List                                  column;
    // 列 json转换成pair
    public final List<Triple<String, String, Boolean>> columnMeta;

    private ZSearchConfig(String server, String tableName, String tableToken, List column, int ttl, int batchSize, int httpPoolSize, boolean cleanup, boolean gzip) {
        this.server = server;
        this.tableName = tableName;
        this.tableToken = tableToken;
        this.ttl = ttl;
        this.batchSize = batchSize;
        this.httpPoolSize = httpPoolSize;
        this.cleanup = cleanup;
        this.column = column;
        this.gzip = gzip;
        this.columnMeta = new ArrayList<Triple<String, String, Boolean>>(column.size());
        for (Object col : column) {
            JSONObject jo = JSONObject.parseObject(col.toString());
            Triple triple = Triple
                    .of(jo.getString("name"), jo.getString("type"), jo.getBoolean("sort"));
            columnMeta.add(triple);
        }
    }

    /**
     * Factory
     *
     * @param conf
     * @return
     */
    public static ZSearchConfig of(Configuration conf) {
        return new ZSearchConfig(conf.getString("server"), conf.getString("tableName"), conf
                .getString("tableToken"), conf.getList("column"), conf
                .getInt("ttl", 60 * 60 * 24 * 360), conf.getInt("batchSize", 100), conf
                .getInt("httpPoolSize", 50), conf.getBool("cleanup", false), conf
                .getBool("gzip", false));
    }

    public Triple<String,String,Boolean> getColumnMeta(int index) {
        return columnMeta.get(index);
    }
}
