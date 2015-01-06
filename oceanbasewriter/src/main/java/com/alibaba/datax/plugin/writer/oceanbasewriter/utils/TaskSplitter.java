package com.alibaba.datax.plugin.writer.oceanbasewriter.utils;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.writer.oceanbasewriter.Key;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

public class TaskSplitter {

    public static List<Configuration> split(Configuration configuration,int mandatoryNumber){
        List<Configuration> tasks = TaskSplitter.split(configuration);
        int tableNumber = tasks.size();
        if (tableNumber != mandatoryNumber && tableNumber != 1) {
            throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
                    String.format("您要写入的目的端的表个数是:%s , 但是根据系统建议需要切分的份数是：%s .",
                            tableNumber, mandatoryNumber));
        }
        return tasks;
    }
    public static List<Configuration> split(Configuration configuration){
        List<Configuration> slices = Lists.newArrayList();
        List<JSONObject> connections = configuration.getList(Key.CONNECTION,JSONObject.class);
        for (JSONObject connection : connections){
            slices.addAll(slice(connection, configuration));
        }
        return slices;
    }

    private static List<Configuration> slice(JSONObject json,Configuration orgin){
        List<Configuration> slices = Lists.newArrayList();
        Configuration configuration = Configuration.from(json);
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
