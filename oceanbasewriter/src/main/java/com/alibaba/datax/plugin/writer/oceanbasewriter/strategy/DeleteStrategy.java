package com.alibaba.datax.plugin.writer.oceanbasewriter.strategy;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.plugin.writer.oceanbasewriter.Rowkey;
import com.alibaba.datax.plugin.writer.oceanbasewriter.column.ColumnMeta;
import com.alibaba.datax.plugin.writer.oceanbasewriter.utils.ConnectionHandler;
import com.alibaba.datax.plugin.writer.oceanbasewriter.utils.OBDataSource;
import com.google.common.base.Preconditions;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;

public class DeleteStrategy extends Strategy {

    private final Rowkey rowkey;
    private final String delete;
	private final Handler handler = new Handler();

	public DeleteStrategy(Context context, List<ColumnMeta> columns) throws Exception{
		super(context, columns);
        this.rowkey = context.rowkey();
        this.delete = sql();
	}

	@Override
	protected void write(List<Record> lines) throws Exception {
		handler.lines(lines);
        String url = context.url();
		OBDataSource.execute(url, handler);
	}

	private String sql() throws Exception{
		class Helper {
			private String name() {
				StringBuilder builder = new StringBuilder();
				for (Rowkey.Entry entry : rowkey) {
					builder.append(entry.name).append(",");
				}
				return builder.substring(0, builder.length() - 1);
			}

			private String placeholder() {
				StringBuilder builder = new StringBuilder();
				for (@SuppressWarnings("unused") Rowkey.Entry entry : rowkey) {
					builder.append("?,");
				}
				return builder.substring(0, builder.length() - 1);
			}
		}
		String table = context.table();
		Helper helper = new Helper();
        return String.format("delete from %s where (%s) = (%s)", table, helper.name(), helper.placeholder());
	}

	private class Handler implements ConnectionHandler {

		private List<Record> records;

		public void lines(List<Record> records) {
			this.records = records;
		}

		@Override
		public Statement callback(Connection connection) throws Exception {
			PreparedStatement statement = connection.prepareStatement(delete);
            for (Record record : records) {
                int count = 1;
                for (Rowkey.Entry entry : rowkey){
                    Column column = null;
                    ColumnMeta meta = null;
                    for (int index = 0, size = columns.size(); index < size; index++) {
                        column = record.getColumn(index);
                        meta = columns.get(index);
                        if (entry.name.equalsIgnoreCase(meta.name())) break;
                    }
                    Preconditions.checkNotNull(meta,String.format("primary key %s not exist",entry.name));
                    statement.setObject(count ++ , meta.value(column,record));
                }
			    statement.execute();
            }
			return statement;
		}
	}

}
