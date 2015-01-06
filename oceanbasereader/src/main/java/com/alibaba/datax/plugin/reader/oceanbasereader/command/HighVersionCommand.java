package com.alibaba.datax.plugin.reader.oceanbasereader.command;

import org.codehaus.jparsec.error.ParserException;

public class HighVersionCommand implements Command {

	@Override
	public void execute(Context context) throws Exception {
		try {
			new WholeTableScanCommand().execute(context);
		} catch (ParserException e) {
			throw new UnsupportedOperationException(String.format("not support sql [%s]",context.originalSQL()),e);
		}
	}
}