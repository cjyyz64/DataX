package com.alibaba.datax.plugin.writer.oceanbasewriter.strategy;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.plugin.writer.oceanbasewriter.column.ColumnMeta;
import com.alibaba.datax.plugin.writer.oceanbasewriter.utils.ConnectionHandler;
import com.alibaba.datax.plugin.writer.oceanbasewriter.utils.OBDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PrepareStrategy extends Strategy {

	private final ConcurrentMap<Integer,String> SQL = new ConcurrentHashMap<Integer, String>(2);
	private final Handler handler = new Handler();
	
	public PrepareStrategy(Context context, List<ColumnMeta> columns) {
		super(context, columns);
	}

	@Override
	protected void write(List<Record> lines) throws Exception {
		handler.lines(lines);
        String url = context.url();
		OBDataSource.execute(url, handler);
	}

	private String sql(int rownum) {
        if(SQL.containsKey(rownum)) return SQL.get(rownum);
		class Helper {
			private String name() {
				StringBuilder builder = new StringBuilder();
				for (ColumnMeta column : columns) {
					builder.append(column.name()).append(",");
				}
				return builder.substring(0, builder.length() - 1);
			}

			private String placeholder() {
				StringBuilder builder = new StringBuilder();
				for (@SuppressWarnings("unused")
				ColumnMeta column : columns) {
					builder.append("?,");
				}
				return builder.substring(0, builder.length() - 1);
			}
		}
		String table = context.table();
        String writeMode = context.writeMode();
		Helper helper = new Helper();
        StringBuilder builder = new StringBuilder(String.format("%s into %s (%s) values ", writeMode, table, helper.name()));
        for(int index = 0; index < rownum; index ++){
             builder.append(String.format("(%s)",helper.placeholder())).append(",");
        }
        SQL.putIfAbsent(rownum,builder.substring(0, builder.length() - 1));
        return SQL.get(rownum);
	}

	private class Handler implements ConnectionHandler {

		private List<Record> records;

		public void lines(List<Record> records) {
			this.records = records;
		}

		@Override
		public Statement callback(Connection connection) throws Exception {
			PreparedStatement statement = connection.prepareStatement(sql(records.size()));
            int count = 1;
			for (Record record : records) {
				for (int index = 0, size = columns.size(); index < size; index++) {
                    Column column = record.getColumn(index);
                    ColumnMeta meta = columns.get(index);
					statement.setObject(count ++ , meta.value(column,record));
				}
			}
			statement.execute();
			return statement;
		}
	}
}