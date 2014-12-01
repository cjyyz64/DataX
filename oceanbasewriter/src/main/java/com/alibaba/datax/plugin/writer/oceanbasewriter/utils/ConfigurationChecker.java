package com.alibaba.datax.plugin.writer.oceanbasewriter.utils;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.oceanbasewriter.Key;
import com.alibaba.datax.plugin.writer.oceanbasewriter.strategy.Context;
import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Map;

public class ConfigurationChecker {
	
	public static void check(Configuration configuration){
		checkRuleA(configuration);
	}
	
	private static void checkRuleA(Configuration configuration){
		int percent = configuration.getInt(Key.ACTIVE_MEM_PERCENT, 60);
		Preconditions.checkArgument(percent >= 0 && percent <= 100,"config activeMemPercent must between 0 ~ 100");
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