package com.alibaba.datax.plugin.reader.sqlserverreader.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class TableExpandUtil {

	private static Pattern pattern = Pattern
			.compile("(\\w+)\\[(\\d+)-(\\d+)\\](.*)");

	private TableExpandUtil() {
	}

	public static String quoteTableOrColumnName(String name) {
		// return "`" + name.replace("`", "``") + "`";
		return name;
	}

	/**
	 * Split the table string(Usually contains names of some tables) to a List
	 * that is formated. example: table[0-32] will be splitted into `table0`,
	 * `table1`, `table2`, ... ,`table32` in {@link List}
	 * 
	 * @param tables
	 *            a string contains table name(one or many).
	 * 
	 * @return a split result of table name.
	 * 
	 */
	public static List<String> splitTables(String tables) {
		List<String> splittedTables = new ArrayList<String>();

		String[] tableArrays = tables.split(",");

		String tableName = null;
		for (String tableArray : tableArrays) {
			Matcher matcher = pattern.matcher(tableArray.trim());
			if (!matcher.matches()) {
				tableName = tableArray.trim();
				splittedTables.add(quoteTableOrColumnName(tableName));
			} else {
				String start = matcher.group(2).trim();
				String end = matcher.group(3).trim();
				String tmp = "";
				if (Integer.valueOf(start) > Integer.valueOf(end)) {
					tmp = start;
					start = end;
					end = tmp;
				}
				int len = start.length();
				for (int k = Integer.valueOf(start); k <= Integer.valueOf(end); k++) {
					if (start.startsWith("0")) {
						tableName = matcher.group(1).trim()
								+ String.format("%0" + len + "d", k)
								+ matcher.group(4).trim();
						splittedTables.add(quoteTableOrColumnName(tableName));
					} else {
						tableName = matcher.group(1).trim()
								+ String.format("%d", k)
								+ matcher.group(4).trim();
						splittedTables.add(quoteTableOrColumnName(tableName));
					}
				}
			}
		}
		return splittedTables;
	}

	/**
	 * Split the table string(Usually contains names of some tables) to a List
	 * that is formated. example: table[0-32] will be splitted into `table0`,
	 * `table1`, `table2`, ... ,`table32` in {@link List}
	 * 
	 * @param tables
	 *            a string contains table name(one or many).
	 * 
	 * @return a split result of table name.
	 * 
	 */
	public static List<String> splitTablesForSqlServer(String tables) {
		List<String> splittedTables = new ArrayList<String>();

		String[] tableArrays = tables.split(",");

		String tableName = null;
		for (String tableArray : tableArrays) {
			Matcher matcher = pattern.matcher(tableArray.trim());
			if (!matcher.matches()) {
				tableName = tableArray.trim();
				splittedTables.add(tableName);
			} else {
				String start = matcher.group(2).trim();
				String end = matcher.group(3).trim();
				String tmp = "";
				if (Integer.valueOf(start) > Integer.valueOf(end)) {
					tmp = start;
					start = end;
					end = tmp;
				}
				int len = start.length();
				for (int k = Integer.valueOf(start); k <= Integer.valueOf(end); k++) {
					if (start.startsWith("0")) {
						tableName = matcher.group(1).trim()
								+ String.format("%0" + len + "d", k)
								+ matcher.group(4).trim();
						splittedTables.add(tableName);
					} else {
						tableName = matcher.group(1).trim()
								+ String.format("%d", k)
								+ matcher.group(4).trim();
						splittedTables.add(tableName);
					}
				}
			}
		}
		return splittedTables;
	}

	public static List<String> expandTableConf(List<String> tables) {
		List<String> parsedTables = new ArrayList<String>();
		for (String table : tables) {
			List<String> splittedTables = splitTables(table);
			parsedTables.addAll(splittedTables);
		}

		return parsedTables;
	}

}
