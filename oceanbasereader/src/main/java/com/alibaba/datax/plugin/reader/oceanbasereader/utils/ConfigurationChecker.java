package com.alibaba.datax.plugin.reader.oceanbasereader.utils;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.oceanbasereader.Key;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.List;

public class ConfigurationChecker {
	
	public static void check(Configuration configuration){
        Checker.checkConnection(configuration);
        Checker.checkConfigURL(configuration);
	}

    private static class Checker {
        private static void checkConnection(Configuration configuration){
            List<String> connections = configuration.getList(Key.CONNECTION, String.class);
            Preconditions.checkArgument(connections != null && !connections.isEmpty(), "%s not provide", Key.CONNECTION);
        }
        private static void checkConfigURL(Configuration configuration){
            List<String> connections = configuration.getList(Key.CONNECTION, String.class);
            for (String connection : connections){
                Configuration cfg = Configuration.from(connection);
                String url = cfg.getString(Key.CONFIG_URL,"");
                Preconditions.checkArgument(!Strings.isNullOrEmpty(url), "%s not provide,yours []", Key.CONFIG_URL, connection);
            }
        }
    }
	
}