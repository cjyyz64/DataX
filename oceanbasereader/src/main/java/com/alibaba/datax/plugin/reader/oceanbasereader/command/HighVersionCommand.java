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
			if (select.where == null) {
				new WholeTableScanCommand().execute(context);
			} else {
				Set<Index> indexes = context.index();
				WithWhereCommand.getCommand(select.where, indexes).execute(context);
			}
		} catch (ParserException e) {
			throw new UnsupportedOperationException("only support 1) whole table scan 2) primary or index = some value 3) primary or index closed-range scan",e);
		}
	}
}