package com.alibaba.datax.plugin.reader.oceanbasereader.command;

import com.alibaba.datax.plugin.reader.oceanbasereader.Index;
import com.alibaba.datax.plugin.reader.oceanbasereader.ast.Expression;
import com.alibaba.datax.plugin.reader.oceanbasereader.ast.SelectExpression;
import com.alibaba.datax.plugin.reader.oceanbasereader.utils.OBDataSource;
import com.alibaba.datax.plugin.reader.oceanbasereader.utils.ResultSetHandler;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.util.List;
import java.util.Set;

public class WholeTableScanCommand implements Command {

	private Logger log = LoggerFactory.getLogger(WholeTableScanCommand.class);
	
	@Override
	public void execute(Context context) throws Exception {
		log.info("Case[whole table scan] start");
		SelectExpression select = context.orginalAst();
		meetRowkeyExist(context.rowkey(),select.columns);
		String sql = String.format("%s limit %s", select, context.limit());
		ResultSetHandler<String> handler = new SendToWriterHandler(context);
		String condition = OBDataSource.execute(context.url(), sql, handler);
		while (!"".equals(condition)) {
            if(select.where == null){
                sql = String.format("%s where %s limit %s", select, condition, context.limit());
            }else {
                sql = String.format("%s and %s limit %s", select, condition, context.limit());
            }
			condition = OBDataSource.execute(context.url(), sql, handler);
		}
		log.info("Case[whole table scan] end");
	}

	private void meetRowkeyExist(final Index rowkey, final List<Expression> columns) {
		Set<String> miss = Sets.newHashSet();
		out:
		for(Index.Entry field : rowkey){
			for(Expression column : columns){
				if(field.name.equalsIgnoreCase(column.toString())){
					continue out;
				}
			}
			miss.add(field.name);
		}
		Preconditions.checkArgument(miss.isEmpty(),String.format("select clause must contain primary key, you miss %s", miss));
	}

	private class SendToWriterHandler implements ResultSetHandler<String> {

		private Index rowkey;
		private Context context;

		private SendToWriterHandler(Context context) throws Exception {
			this.context = context;
			this.rowkey = context.rowkey();
		}

		@Override
		public String callback(ResultSet result) throws Exception {
			this.context.sendToWriter(result);
			return this.getLastRowkey(result);
		}

		private String getLastRowkey(ResultSet result) throws Exception {
			if (!result.isAfterLast())
				return "";
			result.last();
			String left = "(";
			for (Index.Entry entry : rowkey) {
				if (entry.position == 1) {
					left += entry.name;
				} else {
					left += ("," + entry.name);
				}
			}
			left += ")";
			String right = "(";
			for (Index.Entry entry : rowkey) {
				if (entry.position == 1) {
					right += entry.type.convert(result, entry.name);
				} else {
					right += ("," + entry.type.convert(result, entry.name));
				}
			}
			right += ")";
			return left + " > " + right;
		}
	}

}