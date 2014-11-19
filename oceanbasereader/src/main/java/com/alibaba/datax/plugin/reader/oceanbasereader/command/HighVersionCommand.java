package com.alibaba.datax.plugin.reader.oceanbasereader.command;

import com.alibaba.datax.plugin.reader.oceanbasereader.Index;
import com.alibaba.datax.plugin.reader.oceanbasereader.ast.SelectExpression;
import org.codehaus.jparsec.error.ParserException;

import java.util.Set;

public class HighVersionCommand implements Command {

	@Override
	public void execute(Context context) throws Exception {
		try {
			SelectExpression select = context.orginalAst();
			Set<Index> indexes = context.index();
			IndexJudgeCommand.getCommand(select.where, indexes).execute(context);
		} catch (ParserException e) {
			throw new UnsupportedOperationException(String.format("not support sql [%s]",context.originalSQL()),e);
		}
	}
}