package com.alibaba.datax.plugin.reader.oceanbasereader.utils;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.oceanbasereader.Key;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

public class TaskSplitter {

    public static List<Configuration> split(Configuration configuration){
        List<Configuration> slices = Lists.newArrayList();
        List<JSONObject> connections = configuration.getList(Key.CONNECTION,JSONObject.class);
        for (JSONObject connection : connections){
            slices.addAll(slice(connection, configuration));
            Collections.shuffle(slices);
        }
        return slices;
    }

    private static List<Configuration> slice(JSONObject json,Configuration orgin){
        List<Configuration> slices = Lists.newArrayList();
        Configuration configuration = Configuration.from(json);
        List<String> sqls = configuration.getList(Key.SQL, Collections.<String>emptyList(),String.class);
        for (String sql : sqls){
            Configuration slice = orgin.clone();
            slice.remove(Key.CONNECTION);
            slice.set(Key.SQL,sql);
            slice.set(Key.CONFIG_URL,configuration.get(Key.CONFIG_URL));
            slices.add(slice);
        }
        if(!slices.isEmpty()) return slices;
        List<String> tables = configuration.getList(Key.TABLE, Collections.<String>emptyList(),String.class);
        for (String table : tables){
            Configuration slice = orgin.clone();
            slice.remove(Key.CONNECTION);
            slice.set(Key.TABLE,table);
            slice.set(Key.CONFIG_URL,configuration.get(Key.CONFIG_URL));
            slices.add(slice);
        }
        return slices;
    }

}
