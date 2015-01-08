package com.alibaba.datax.plugin.reader.oceanbasereader.command;

import com.alibaba.datax.plugin.reader.oceanbasereader.Index;
import com.alibaba.datax.plugin.reader.oceanbasereader.ast.Expression;
import com.alibaba.datax.plugin.reader.oceanbasereader.ast.SelectExpression;
import com.alibaba.datax.plugin.reader.oceanbasereader.utils.OBDataSource;
import com.alibaba.datax.plugin.reader.oceanbasereader.utils.ResultSetHandler;
import com.alibaba.datax.plugin.reader.oceanbasereader.utils.Tablet;
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
        Tablet tablet = context.tablet();
		String sql = tablet.sql(select, null, context.limit());
		ResultSetHandler<String> handler = new SendToWriterHandler(context);
		String boundRowkey = OBDataSource.execute(context.url(), sql, handler);
		while (!"".equals(boundRowkey) && !tablet.endkey.equals(boundRowkey)) {
            sql = tablet.sql(select, boundRowkey, context.limit());
			boundRowkey = OBDataSource.execute(context.url(), sql, handler);
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
			String rowkey = "(";
			for (Index.Entry entry : this.rowkey) {
				if (entry.position == 1) {
					rowkey += entry.type.convert(result, entry.name);
				} else {
					rowkey += ("," + entry.type.convert(result, entry.name));
				}
			}
			rowkey += ")";
			return rowkey;
		}
	}

}