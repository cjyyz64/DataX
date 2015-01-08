package com.alibaba.datax.plugin.writer.oceanbasewriter.utils;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.oceanbasewriter.Key;
import com.alibaba.datax.plugin.writer.oceanbasewriter.strategy.Context;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.List;
import java.util.Map;

public class ConfigurationChecker {
	
	public static void check(Configuration configuration){
		checkMemPercent(configuration);
        checkWriteMode(configuration);
        checkConnection(configuration);
        checkTable(configuration);
        checkColumnOrDsl(configuration);
        checkConfigURL(configuration);
	}
	
	private static void checkMemPercent(Configuration configuration){
        int percent = configuration.getInt(Key.ACTIVE_MEM_PERCENT, 60);
        Preconditions.checkArgument(percent >= 0 && percent <= 100,"activeMemPercent must between 0 ~ 100");
    }

    private static void checkWriteMode(Configuration configuration){
        String mode = configuration.getString(Key.WRITE_MODE,"replace");
        Preconditions.checkArgument(mode.equalsIgnoreCase("replace") || mode.equalsIgnoreCase("insert"),"writeMode must be replace or insert");
    }

    private static void checkConnection(Configuration configuration){
        List<JSONObject> connections = configuration.getList(Key.CONNECTION, JSONObject.class);
        Preconditions.checkArgument(connections != null && !connections.isEmpty(), "%s not provide", Key.CONNECTION);
    }

    private static void checkTable(Configuration configuration){
        List<JSONObject> connections = configuration.getList(Key.CONNECTION, JSONObject.class);
        for (JSONObject connection : connections){
            Configuration cfg = Configuration.from(connection);
            String url = cfg.getString(Key.TABLE,"");
            Preconditions.checkArgument(!Strings.isNullOrEmpty(url), "%s not provide,yours %s", Key.TABLE, connection);
        }
    }

    private static void checkConfigURL(Configuration configuration){
        List<JSONObject> connections = configuration.getList(Key.CONNECTION, JSONObject.class);
        for (JSONObject connection : connections){
            Configuration cfg = Configuration.from(connection);
            String url = cfg.getString(Key.CONFIG_URL,"");
            Preconditions.checkArgument(!Strings.isNullOrEmpty(url), "%s not provide,yours %s", Key.CONFIG_URL, connection);
        }
    }

    private static void checkColumnOrDsl(Configuration configuration){
        List<String> columns = configuration.getList(Key.COLUMNS,String.class);
        List<String> dsl = configuration.getList(Key.ADVANCE,String.class);
        boolean expect = (columns != null && !columns.isEmpty()) || (dsl != null && !dsl.isEmpty());
        Preconditions.checkArgument(expect, "%s or %s not provide",Key.COLUMNS,Key.ADVANCE);
    }

	public static void checkNormalConfig(Context context){
		List<String> columns = context.normal();
        Preconditions.checkArgument(columns != null && !columns.isEmpty(),"column config miss");
		Map<String, String> columnType = context.columnType();
		String tip = "yours column [%s] not exist in table[%s], should conform to one of %s";
		for(String column : columns){
			Preconditions.checkArgument(!"".equals(column.trim()), String.format("a empty column name : [%s]", columns));
			Preconditions.checkArgument(columnType.containsKey(column.trim().toLowerCase()),String.format(tip, column, context.table(), columnType));
		}
	}

}