package com.alibaba.datax.plugin.reader.oceanbasereader.utils;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.oceanbasereader.Key;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.List;

public class ConfigurationChecker {
	
	public static void check(Configuration configuration){
        Checker.checkConnection(configuration);
        Checker.checkConfigURL(configuration);
        Checker.checkColumn(configuration);
	}

    private static class Checker {
        private static void checkConnection(Configuration configuration){
            List<JSONObject> connections = configuration.getList(Key.CONNECTION, JSONObject.class);
            Preconditions.checkArgument(connections != null && !connections.isEmpty(), "%s not provide", Key.CONNECTION);
        }
        private static void checkConfigURL(Configuration configuration){
            List<JSONObject> connections = configuration.getList(Key.CONNECTION, JSONObject.class);
            for (JSONObject connection : connections){
                Configuration cfg = Configuration.from(connection);
                String url = cfg.getString(Key.CONFIG_URL,"");
                Preconditions.checkArgument(!Strings.isNullOrEmpty(url), "%s not provide,yours %s", Key.CONFIG_URL, connection);
            }
        }

        private static void checkColumn(Configuration configuration){
            List<String> columns = configuration.getList(Key.COLUMN,String.class);
            Preconditions.checkArgument(columns != null && !columns.isEmpty(), "%s not provide",Key.COLUMN);
        }
    }
	
}