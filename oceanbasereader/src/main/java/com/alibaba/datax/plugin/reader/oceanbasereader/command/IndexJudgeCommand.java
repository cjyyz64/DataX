package com.alibaba.datax.plugin.reader.oceanbasereader.command;

import com.alibaba.datax.plugin.reader.oceanbasereader.Index;
import com.alibaba.datax.plugin.reader.oceanbasereader.ast.Expression;
import com.alibaba.datax.plugin.reader.oceanbasereader.ast.SelectExpression;
import com.alibaba.datax.plugin.reader.oceanbasereader.utils.OBDataSource;
import com.alibaba.datax.plugin.reader.oceanbasereader.utils.ResultSetHandler;
import com.alibaba.datax.plugin.reader.oceanbasereader.visitor.IndexScanVisitor;
import com.alibaba.datax.plugin.reader.oceanbasereader.visitor.ModifyConditionVisitor;
import com.alibaba.datax.plugin.reader.oceanbasereader.visitor.PrefixScanVisitor;
import com.alibaba.datax.plugin.reader.oceanbasereader.visitor.Visitor;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.util.List;
import java.util.Set;

public enum IndexJudgeCommand implements Command{

	PrefixScanCommand {
		@Override
		public void execute(final Context context) throws Exception {
			log.info("Case[prefix scan]");
			String sqlWithWeakConsistencyHint = context.orginalAst().toString();
			OBDataSource.execute(context.url(), sqlWithWeakConsistencyHint, new ResultSetHandler<Void>() {
                @Override
                public Void callback(ResultSet result) throws Exception {
                    context.sendToWriter(result);
                    return null;
                }
            });
		}

		@Override
		protected boolean meet(Expression expr, Set<Index> indexes) {
			Visitor<Boolean> visitor =  new PrefixScanVisitor(indexes);
			expr.accept(visitor);
			return visitor.visitResult();
		}
	},IndexScanCommand {
		
		private Index.Entry entry;
		
		@Override
		public void execute(Context context) throws Exception {
			log.info("Case[index range scan]");
			Preconditions.checkNotNull(entry,"not meet index scan case");
			SelectExpression select = context.orginalAst();
			this.checkSelectMustContainEntry(entry.name, select.columns);
			String sql = String.format("%s limit 1000", select);
			ResultSetHandler<String> handler = new SendToWriterHandler(context,entry);
			String condition = OBDataSource.execute(context.url(), sql, handler);
			while(!"".equals(condition)){
				select.where.accept(new ModifyConditionVisitor(entry.name, condition));
				sql = String.format("%s limit 1000", select);
				condition = OBDataSource.execute(context.url(), sql, handler);
			}
		}

		@Override
		protected boolean meet(Expression expr, Set<Index> indexes) {
			Visitor<Index.Entry> visitor = new IndexScanVisitor(indexes);
			expr.accept(visitor);
			entry = visitor.visitResult();
			return entry != null;
		}
		
		private boolean meetWildCard(List<Expression> columns){
			return columns.size() == 1 && "*".equals(columns.get(0).toString());
		}
		
		private void checkSelectMustContainEntry(String name, List<Expression> columns){
			if(this.meetWildCard(columns)) return;
			boolean exist = false;
			for(Expression column : columns){
				if(name.equalsIgnoreCase(column.toString())){
					exist = true;
					break;
				}
			}
			Preconditions.checkArgument(exist,String.format("select must contain %s column", name));
		}
		
		final class SendToWriterHandler implements ResultSetHandler<String> {

			private Index.Entry entry;
			private Context context;

			private SendToWriterHandler(Context context,Index.Entry entry) {
				this.context = context;
				this.entry = entry;
			}

			@Override
			public String callback(ResultSet result) throws Exception {
				this.context.sendToWriter(result);
				return this.getLastCondition(result);
			}

			private String getLastCondition(ResultSet result) throws Exception {
				if (!result.isAfterLast()) return "";//根据jdbc规范判断空结果集
				result.last();
				return entry.type.convert(result, entry.name);
			}
		}
	};

	protected Logger log = LoggerFactory.getLogger(this.getClass());
	protected abstract boolean meet(Expression expr, Set<Index> indexes);

	public static Command getCommand(Expression expr, Set<Index> indexes){
        if(expr == null) return new WholeTableScanCommand();
		IndexJudgeCommand[] commands = new IndexJudgeCommand[]{IndexScanCommand,PrefixScanCommand};//���ȼ�
		for(IndexJudgeCommand command : commands){
			if(command.meet(expr, indexes)) return command;
		}
		return new WholeTableScanCommand();
	}
}