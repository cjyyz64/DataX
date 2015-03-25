package com.alibaba.datax.plugin.reader.oceanbasereader.command;

import com.alibaba.datax.plugin.reader.oceanbasereader.Index;
import com.alibaba.datax.plugin.reader.oceanbasereader.ast.Expression;
import com.alibaba.datax.plugin.reader.oceanbasereader.ast.SelectExpression;
import com.alibaba.datax.plugin.reader.oceanbasereader.utils.OBDataSource;
import com.alibaba.datax.plugin.reader.oceanbasereader.utils.PreparedStatementHandler;
import com.alibaba.datax.plugin.reader.oceanbasereader.utils.ResultSetHandler;
import com.alibaba.datax.plugin.reader.oceanbasereader.utils.Tablet;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.util.Collections;
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
		ResultSetHandler<List<?>> handler = new SendToWriterHandler(context);
		List<?> boundRowkey = OBDataSource.executePrepare(context.url(), new Handler(tablet,Collections.emptyList(),select,context.limit()), handler);
		while (!boundRowkey.isEmpty() && !tablet.endkey.equals(boundRowkey)) {
			boundRowkey = OBDataSource.executePrepare(context.url(), new Handler(tablet, boundRowkey,select,context.limit()), handler);
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

	private class SendToWriterHandler implements ResultSetHandler<List<?>> {

		private Index rowkey;
		private Context context;

		private SendToWriterHandler(Context context) throws Exception {
			this.context = context;
			this.rowkey = context.rowkey();
		}

		@Override
		public List<?> callback(ResultSet result) throws Exception {
			this.context.sendToWriter(result);
			return this.getLastRowkey(result);
		}

		private List<?> getLastRowkey(ResultSet result) throws Exception {
			if (!result.isAfterLast())
				return Collections.emptyList();
			result.last();
            List<Object> values = Lists.newArrayList();
            for (Index.Entry entry : this.rowkey) {
                values.add(entry.type.convert(result,entry.name));
			}
			return values;
		}
	}

    private class Handler implements PreparedStatementHandler {

        private final Tablet tablet;
        private final List<?> boundRowkey;
        private final SelectExpression expression;
        private final int limit;

        private Handler(Tablet tablet,List<?> boundRowkey, SelectExpression expression,int limit){
            this.tablet = tablet;
            this.boundRowkey = boundRowkey;
            this.expression = expression;
            this.limit = limit;
        }

        @Override
        public String sql() {
            return tablet.sql(expression,boundRowkey,limit);
        }

        @Override
        public List<?> parameters() {
            return tablet.parameters(boundRowkey);
        }

    }
}